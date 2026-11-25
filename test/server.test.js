const assert = require('assert');
const http = require('http');
const { WebSocket } = require('ws');
const { server, streams } = require('../server');

async function runTests() {
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  const baseUrl = `http://127.0.0.1:${port}`;

  try {
    // 1. Health check
    const healthRes = await fetch(`${baseUrl}/health`);
    assert.strictEqual(healthRes.status, 200);
    const healthJson = await healthRes.json();
    assert.strictEqual(healthJson.status, 'healthy');

    // 2. Connect broadcaster WebSocket
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws/broadcast?id=testcam&title=UnitTestCam&mimeType=video/webm`);
    const readyMsg = await new Promise((resolve, reject) => {
      ws.on('error', reject);
      ws.on('message', (data) => resolve(JSON.parse(data.toString())));
    });
    assert.strictEqual(readyMsg.type, 'ready');
    assert.strictEqual(readyMsg.streamId, 'testcam');

    // 3. Send init segment + live video chunk
    const initBuf = Buffer.from('WEBM_INIT_HEADER');
    const liveBuf = Buffer.from('_LIVE_CHUNK_1');
    ws.send(initBuf);
    await new Promise((r) => setTimeout(r, 50));

    // 4. Verify /api/streams lists the active stream
    const listRes = await fetch(`${baseUrl}/api/streams`);
    assert.strictEqual(listRes.status, 200);
    const listJson = await listRes.json();
    assert.strictEqual(listJson.streams.length, 1);
    assert.strictEqual(listJson.streams[0].id, 'testcam');
    assert.ok(listJson.streams[0].directVideoUrl.endsWith('/stream/testcam'));

    // 5. Connect HTTP viewer to direct /stream/testcam URL and verify init + live chunk delivery
    const received = await new Promise((resolve, reject) => {
      const req = http.get(`${baseUrl}/stream/testcam`, (res) => {
        assert.strictEqual(res.statusCode, 200);
        assert.strictEqual(res.headers['content-type'], 'video/webm');
        const chunks = [];
        res.on('data', (c) => {
          chunks.push(c);
          const combined = Buffer.concat(chunks).toString();
          if (combined.includes('WEBM_INIT_HEADER_LIVE_CHUNK_1')) {
            req.destroy();
            resolve(combined);
          }
        });
        setTimeout(() => ws.send(liveBuf), 40);
      });
      req.on('error', reject);
    });
    assert.ok(received.includes('WEBM_INIT_HEADER_LIVE_CHUNK_1'));

    // 6. Close broadcaster and confirm cleanup
    ws.close();
    await new Promise((r) => setTimeout(r, 50));
    assert.strictEqual(streams.size, 0);

    const notFoundRes = await fetch(`${baseUrl}/stream/testcam`);
    assert.strictEqual(notFoundRes.status, 404);

    console.log('All WebInput tests passed!');
  } finally {
    server.close();
  }
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
