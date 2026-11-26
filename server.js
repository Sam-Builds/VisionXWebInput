const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const app = express();
const server = http.createServer(app);

const PORT = parseInt(process.env.PORT || '8080', 10);

const DEFAULT_CONFIG = {
  targetInferenceFps: Math.max(1, parseFloat(process.env.WEBINPUT_TARGET_FPS || '10')),
  maxPendingFrames: Math.max(1, parseInt(process.env.WEBINPUT_MAX_PENDING_FRAMES || '1', 10)),
  samplingIntervalMs: Math.max(20, parseInt(process.env.WEBINPUT_SAMPLING_INTERVAL_MS || '100', 10)),
  inputWidth: Math.max(160, parseInt(process.env.WEBINPUT_INPUT_WIDTH || '640', 10)),
  inputHeight: Math.max(120, parseInt(process.env.WEBINPUT_INPUT_HEIGHT || '480', 10)),
  maxProcessingFps: Math.max(1, parseFloat(process.env.WEBINPUT_MAX_PROCESSING_FPS || '15')),
  staleStreamMs: Math.max(2000, parseInt(process.env.WEBINPUT_STALE_STREAM_MS || '6000', 10)),
};

const FRAME_MAGIC = 0x01; // Binary live-frame packet: [0x01][8-byte float64BE captureTs][JPEG bytes]
const MAX_WS_BUFFERED_BYTES = 256 * 1024;

// Trust reverse proxy headers (Cloudflare Tunnel / Nginx)
app.set('trust proxy', true);

// Optional subpath support (e.g. /webinput)
app.use((req, res, next) => {
  const prefixes = ['/webinput', '/cam'];
  for (const prefix of prefixes) {
    if (req.url === prefix || req.url.startsWith(`${prefix}/`)) {
      req.basePrefix = prefix;
      req.url = req.url.slice(prefix.length) || '/';
      break;
    }
  }
  next();
});

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

/**
 * Active live streams:
 * streamId -> {
 *   id, title, mimeType, config,
 *   initSegment, pendingFrames, latestDeliveredFrame,
 *   clients (HTTP /stream/:id), watchClients (WS /ws/watch/:id),
 *   broadcasterWs, metrics, dispatchScheduled, ...
 * }
 */
const streams = new Map();

function generateStreamId() {
  return `cam-${crypto.randomBytes(3).toString('hex')}`;
}

function getBaseUrl(req) {
  const proto = req.headers['x-forwarded-proto'] || req.protocol || 'http';
  const host = req.headers['x-forwarded-host'] || req.get('host') || `localhost:${PORT}`;
  const prefix = req.basePrefix || '';
  return `${proto}://${host}${prefix}`;
}

function computeFps(timestamps, now) {
  const cutoff = now - 5000;
  while (timestamps.length > 0 && timestamps[0] < cutoff) {
    timestamps.shift();
  }
  if (timestamps.length < 2) return timestamps.length;
  const spanSec = Math.max(0.25, (timestamps[timestamps.length - 1] - timestamps[0]) / 1000);
  return Number(((timestamps.length - 1) / spanSec).toFixed(2));
}

function buildMetricsSnapshot(s) {
  const now = Date.now();
  const m = s.metrics;
  const isStale = m.lastFrameReceivedAt > 0 && (now - m.lastFrameReceivedAt) > s.config.staleStreamMs;
  const status = !s.broadcasterWs || s.broadcasterWs.readyState !== WebSocket.OPEN
    ? 'Disconnected'
    : (m.lastFrameReceivedAt === 0 ? 'Connecting' : (isStale ? 'Reconnecting' : 'Live'));

  return {
    streamId: s.id,
    status,
    config: s.config,
    captureTimestamp: m.lastCaptureTimestamp || null,
    processingStartTimestamp: m.lastProcessingStartTimestamp || null,
    processingCompletionTimestamp: m.lastProcessingCompletionTimestamp || null,
    frameAgeMs: m.lastFrameAgeMs,
    captureFps: computeFps(m.captureTimes, now),
    inferenceFps: computeFps(m.processedTimes, now),
    skippedFrames: m.skippedFrames,
    processedFrames: m.processedFrames,
    httpDroppedChunks: m.httpDroppedChunks,
    bufferSize: s.pendingFrames.length,
    estimatedLatencyMs: m.estimatedLatencyMs,
    viewers: s.clients.size + s.watchClients.size,
    bytesReceived: s.bytesReceived,
    updatedAt: new Date(now).toISOString(),
  };
}

function enqueueLatestFrame(streamState, frame) {
  const maxPending = streamState.config.maxPendingFrames;
  const now = Date.now();

  streamState.metrics.captureTimes.push(now);
  streamState.metrics.lastCaptureTimestamp = frame.captureTs;
  streamState.metrics.lastFrameReceivedAt = now;

  // Bounded latest-frame-first buffer: drop oldest pending frames when full
  while (streamState.pendingFrames.length >= maxPending) {
    streamState.pendingFrames.shift();
    streamState.metrics.skippedFrames += 1;
  }

  streamState.pendingFrames.push(frame);
  scheduleDispatch(streamState);
}

function scheduleDispatch(streamState) {
  if (streamState.dispatchScheduled || streamState.isDispatching) return;
  streamState.dispatchScheduled = true;

  setImmediate(() => {
    streamState.dispatchScheduled = false;
    processNextLatestFrame(streamState);
  });
}

function processNextLatestFrame(streamState) {
  if (streamState.isDispatching || streamState.pendingFrames.length === 0) return;

  const minIntervalMs = 1000 / streamState.config.maxProcessingFps;
  const elapsed = Date.now() - streamState.lastDispatchAt;
  if (elapsed < minIntervalMs) {
    if (!streamState.throttleTimer) {
      streamState.throttleTimer = setTimeout(() => {
        streamState.throttleTimer = null;
        processNextLatestFrame(streamState);
      }, Math.max(1, minIntervalMs - elapsed));
    }
    return;
  }

  streamState.isDispatching = true;
  try {
    // Always pop the NEWEST frame and discard any older pending ones
    const frame = streamState.pendingFrames.pop();
    if (streamState.pendingFrames.length > 0) {
      streamState.metrics.skippedFrames += streamState.pendingFrames.length;
      streamState.pendingFrames.length = 0;
    }

    const procStart = Date.now();
    const frameAgeMs = Math.max(0, procStart - frame.captureTs);

    streamState.latestDeliveredFrame = frame;
    streamState.lastDispatchAt = procStart;

    // Build wire packet once (zero extra copies per viewer)
    const header = Buffer.allocUnsafe(9);
    header.writeUInt8(FRAME_MAGIC, 0);
    header.writeDoubleBE(frame.captureTs, 1);
    const packet = Buffer.concat([header, frame.jpegBuffer]);

    for (const viewerWs of streamState.watchClients) {
      if (viewerWs.readyState !== WebSocket.OPEN) {
        streamState.watchClients.delete(viewerWs);
        continue;
      }
      // Drop frame for slow viewer instead of accumulating a TCP/WS backlog
      if (viewerWs.bufferedAmount > MAX_WS_BUFFERED_BYTES) {
        streamState.metrics.skippedFrames += 1;
        continue;
      }
      viewerWs.send(packet, { binary: true });
    }

    const procEnd = Date.now();
    streamState.metrics.processedFrames += 1;
    streamState.metrics.processedTimes.push(procEnd);
    streamState.metrics.lastProcessingStartTimestamp = procStart;
    streamState.metrics.lastProcessingCompletionTimestamp = procEnd;
    streamState.metrics.lastFrameAgeMs = frameAgeMs;
    streamState.metrics.estimatedLatencyMs = Math.max(0, procEnd - frame.captureTs);
  } finally {
    streamState.isDispatching = false;
    if (streamState.pendingFrames.length > 0) {
      scheduleDispatch(streamState);
    }
  }
}

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    service: 'VisionX WebInput',
    activeStreams: streams.size,
    defaults: DEFAULT_CONFIG,
    timestamp: new Date().toISOString(),
  });
});

// List active streams with live metrics
app.get('/api/streams', (req, res) => {
  const baseUrl = getBaseUrl(req);
  const list = Array.from(streams.values()).map((s) => {
    const metrics = buildMetricsSnapshot(s);
    return {
      id: s.id,
      title: s.title,
      mimeType: s.mimeType,
      status: metrics.status,
      viewers: metrics.viewers,
      bytesReceived: s.bytesReceived,
      createdAt: s.createdAt,
      directVideoUrl: `${baseUrl}/stream/${s.id}`,
      watchUrl: `${baseUrl}/watch/${s.id}`,
      latestFrameUrl: `${baseUrl}/api/streams/${s.id}/frame.jpg`,
      metrics,
    };
  });
  res.json({ streams: list });
});

// Stream diagnostics endpoint
app.get('/api/streams/:id/metrics', (req, res) => {
  const stream = streams.get(req.params.id);
  if (!stream) {
    return res.status(404).json({ error: 'Stream not found', streamId: req.params.id });
  }
  res.json(buildMetricsSnapshot(stream));
});

// Newest single JPEG frame (zero backlog)
app.get('/api/streams/:id/frame.jpg', (req, res) => {
  const stream = streams.get(req.params.id);
  const frame = stream?.pendingFrames[stream.pendingFrames.length - 1] || stream?.latestDeliveredFrame;
  if (!stream || !frame) {
    return res.status(404).json({ error: 'No live frame available yet', streamId: req.params.id });
  }
  res.set({
    'Content-Type': 'image/jpeg',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'X-Capture-Timestamp': String(frame.captureTs),
    'X-Frame-Seq': String(frame.seq),
    'Access-Control-Allow-Origin': '*',
  });
  res.send(frame.jpegBuffer);
});

// Watch page for a stream
app.get('/watch/:id', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'watch.html'));
});

// Direct shareable live video URL (MediaRecorder container stream with backpressure dropping)
app.get('/stream/:id', (req, res) => {
  const stream = streams.get(req.params.id);
  if (!stream) {
    return res.status(404).json({
      error: 'Stream not found or broadcast has ended.',
      streamId: req.params.id,
    });
  }

  res.writeHead(200, {
    'Content-Type': stream.mimeType || 'video/webm',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    Pragma: 'no-cache',
    Expires: '0',
    Connection: 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'X-Content-Type-Options': 'nosniff',
  });

  // Send only container init header (never historical chunks) so late joiners start at live edge
  if (stream.initSegment) {
    res.write(stream.initSegment);
  }

  stream.clients.add(res);

  req.on('close', () => {
    stream.clients.delete(res);
  });
});

// WebSocket server for incoming camera broadcasts & low-latency live-edge viewers
const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = url.pathname.replace(/^\/(webinput|cam)/, '');

  // Viewer WebSocket: /ws/watch/:id
  if (pathname.startsWith('/ws/watch/')) {
    const targetId = pathname.slice('/ws/watch/'.length).replace(/[^a-zA-Z0-9_-]/g, '');
    const stream = streams.get(targetId);
    if (!stream) {
      ws.close(1008, 'Stream not found');
      return;
    }

    stream.watchClients.add(ws);
    ws.send(JSON.stringify({ type: 'status', metrics: buildMetricsSnapshot(stream) }));

    // Immediately push newest available frame (never replay historical frames)
    if (stream.latestDeliveredFrame && ws.readyState === WebSocket.OPEN) {
      const f = stream.latestDeliveredFrame;
      const header = Buffer.allocUnsafe(9);
      header.writeUInt8(FRAME_MAGIC, 0);
      header.writeDoubleBE(f.captureTs, 1);
      ws.send(Buffer.concat([header, f.jpegBuffer]), { binary: true });
    }

    const metricsTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'metrics', metrics: buildMetricsSnapshot(stream) }));
      }
    }, 1000);

    ws.on('close', () => {
      clearInterval(metricsTimer);
      stream.watchClients.delete(ws);
    });
    return;
  }

  if (!pathname.startsWith('/ws/broadcast')) {
    ws.close(1008, 'Invalid WebSocket endpoint');
    return;
  }

  const requestedId = (url.searchParams.get('id') || '').replace(/[^a-zA-Z0-9_-]/g, '');
  const streamId = requestedId && !streams.has(requestedId) ? requestedId : generateStreamId();
  const title = (url.searchParams.get('title') || 'Live Camera').slice(0, 80);
  const mimeType = url.searchParams.get('mimeType') || 'video/webm';

  const config = {
    targetInferenceFps: Math.max(1, Math.min(30, parseFloat(url.searchParams.get('fps') || DEFAULT_CONFIG.targetInferenceFps))),
    maxPendingFrames: Math.max(1, Math.min(5, parseInt(url.searchParams.get('maxPending') || DEFAULT_CONFIG.maxPendingFrames, 10))),
    samplingIntervalMs: Math.max(20, parseInt(url.searchParams.get('intervalMs') || DEFAULT_CONFIG.samplingIntervalMs, 10)),
    inputWidth: Math.max(160, parseInt(url.searchParams.get('width') || DEFAULT_CONFIG.inputWidth, 10)),
    inputHeight: Math.max(120, parseInt(url.searchParams.get('height') || DEFAULT_CONFIG.inputHeight, 10)),
    maxProcessingFps: Math.max(1, Math.min(30, parseFloat(url.searchParams.get('maxFps') || DEFAULT_CONFIG.maxProcessingFps))),
    staleStreamMs: DEFAULT_CONFIG.staleStreamMs,
  };

  const streamState = {
    id: streamId,
    title,
    mimeType,
    config,
    initSegment: null,
    pendingFrames: [],
    latestDeliveredFrame: null,
    clients: new Set(),
    watchClients: new Set(),
    broadcasterWs: ws,
    createdAt: new Date().toISOString(),
    bytesReceived: 0,
    seqCounter: 0,
    dispatchScheduled: false,
    isDispatching: false,
    throttleTimer: null,
    lastDispatchAt: 0,
    metrics: {
      captureTimes: [],
      processedTimes: [],
      lastCaptureTimestamp: 0,
      lastProcessingStartTimestamp: 0,
      lastProcessingCompletionTimestamp: 0,
      lastFrameAgeMs: 0,
      lastFrameReceivedAt: 0,
      skippedFrames: 0,
      processedFrames: 0,
      httpDroppedChunks: 0,
      estimatedLatencyMs: 0,
    },
  };

  streams.set(streamId, streamState);

  ws.send(
    JSON.stringify({
      type: 'ready',
      streamId,
      streamPath: `/stream/${streamId}`,
      watchPath: `/watch/${streamId}`,
      config,
    })
  );

  ws.on('message', (data, isBinary) => {
    if (!isBinary) {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'ping') {
          ws.send(JSON.stringify({
            type: 'pong',
            viewers: streamState.clients.size + streamState.watchClients.size,
            metrics: buildMetricsSnapshot(streamState),
          }));
        }
      } catch {
        // ignore invalid JSON
      }
      return;
    }

    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (chunk.length === 0) return;

    streamState.bytesReceived += chunk.length;

    // 1. Live-edge JPEG frame packet: [0x01][8-byte float64BE captureTs][JPEG bytes]
    if (chunk.length > 9 && chunk[0] === FRAME_MAGIC) {
      const captureTs = chunk.readDoubleBE(1);
      const jpegBuffer = chunk.subarray(9);
      streamState.seqCounter += 1;
      enqueueLatestFrame(streamState, {
        seq: streamState.seqCounter,
        captureTs: Number.isFinite(captureTs) ? captureTs : Date.now(),
        jpegBuffer,
      });
      return;
    }

    // 2. MediaRecorder chunk for /stream/:id
    if (!streamState.initSegment) {
      streamState.initSegment = chunk;
    }

    for (const clientRes of streamState.clients) {
      if (clientRes.writableEnded) {
        streamState.clients.delete(clientRes);
        continue;
      }
      // Drop intermediate chunks for slow HTTP viewers rather than building a multi-minute buffer
      if (clientRes.writableNeedDrain && chunk !== streamState.initSegment) {
        streamState.metrics.httpDroppedChunks += 1;
        continue;
      }
      clientRes.write(chunk);
    }
  });

  ws.on('close', () => {
    if (streamState.throttleTimer) {
      clearTimeout(streamState.throttleTimer);
      streamState.throttleTimer = null;
    }
    for (const clientRes of streamState.clients) {
      if (!clientRes.writableEnded) {
        clientRes.end();
      }
    }
    for (const viewerWs of streamState.watchClients) {
      if (viewerWs.readyState === WebSocket.OPEN) {
        viewerWs.close(1000, 'Broadcast ended');
      }
    }
    streamState.clients.clear();
    streamState.watchClients.clear();
    streams.delete(streamId);
  });
});

if (require.main === module) {
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`VisionX WebInput server listening on http://0.0.0.0:${PORT}`);
  });
}

module.exports = { app, server, streams, DEFAULT_CONFIG };
