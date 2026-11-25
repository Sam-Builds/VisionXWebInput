const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);

const PORT = parseInt(process.env.PORT || '8080', 10);

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
 *   id: string,
 *   title: string,
 *   mimeType: string,
 *   initSegment: Buffer | null,
 *   recentChunks: Buffer[],
 *   clients: Set<express.Response>,
 *   broadcasterWs: WebSocket | null,
 *   createdAt: string,
 *   bytesReceived: number
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

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    service: 'VisionX WebInput',
    activeStreams: streams.size,
    timestamp: new Date().toISOString(),
  });
});

// List active streams
app.get('/api/streams', (req, res) => {
  const baseUrl = getBaseUrl(req);
  const list = Array.from(streams.values()).map((s) => ({
    id: s.id,
    title: s.title,
    mimeType: s.mimeType,
    viewers: s.clients.size,
    bytesReceived: s.bytesReceived,
    createdAt: s.createdAt,
    directVideoUrl: `${baseUrl}/stream/${s.id}`,
    watchUrl: `${baseUrl}/watch/${s.id}`,
  }));
  res.json({ streams: list });
});

// Watch page for a stream
app.get('/watch/:id', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'watch.html'));
});

// Direct shareable live video URL
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

  // Send the EBML/MP4 initialization segment so late-joining players decode cleanly
  if (stream.initSegment) {
    res.write(stream.initSegment);
  }

  stream.clients.add(res);

  req.on('close', () => {
    stream.clients.delete(res);
  });
});

// WebSocket server for incoming camera broadcasts
const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  // Strip optional /webinput or /cam prefix
  let pathname = url.pathname.replace(/^\/(webinput|cam)/, '');

  if (!pathname.startsWith('/ws/broadcast')) {
    ws.close(1008, 'Invalid WebSocket endpoint');
    return;
  }

  const requestedId = (url.searchParams.get('id') || '').replace(/[^a-zA-Z0-9_-]/g, '');
  const streamId = requestedId && !streams.has(requestedId) ? requestedId : generateStreamId();
  const title = (url.searchParams.get('title') || 'Live Camera').slice(0, 80);
  const mimeType = url.searchParams.get('mimeType') || 'video/webm';

  const streamState = {
    id: streamId,
    title,
    mimeType,
    initSegment: null,
    clients: new Set(),
    broadcasterWs: ws,
    createdAt: new Date().toISOString(),
    bytesReceived: 0,
  };

  streams.set(streamId, streamState);

  // Notify broadcaster of assigned stream ID
  ws.send(
    JSON.stringify({
      type: 'ready',
      streamId,
      streamPath: `/stream/${streamId}`,
      watchPath: `/watch/${streamId}`,
    })
  );

  ws.on('message', (data, isBinary) => {
    if (!isBinary) {
      // Handle JSON control messages (e.g., viewer count ping)
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'ping') {
          ws.send(JSON.stringify({ type: 'pong', viewers: streamState.clients.size }));
        }
      } catch {
        // ignore invalid control JSON
      }
      return;
    }

    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (chunk.length === 0) return;

    streamState.bytesReceived += chunk.length;

    // The very first binary chunk from MediaRecorder contains the container header
    if (!streamState.initSegment) {
      streamState.initSegment = chunk;
    }

    // Fan out to all connected HTTP viewers
    for (const clientRes of streamState.clients) {
      if (!clientRes.writableEnded) {
        clientRes.write(chunk);
      } else {
        streamState.clients.delete(clientRes);
      }
    }
  });

  ws.on('close', () => {
    for (const clientRes of streamState.clients) {
      if (!clientRes.writableEnded) {
        clientRes.end();
      }
    }
    streamState.clients.clear();
    streams.delete(streamId);
  });
});

if (require.main === module) {
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`VisionX WebInput server listening on http://0.0.0.0:${PORT}`);
  });
}

module.exports = { app, server, streams };
