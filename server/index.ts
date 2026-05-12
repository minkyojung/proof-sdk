import express from 'express';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import path from 'path';
import { fileURLToPath } from 'url';
import { apiRoutes } from './routes.js';
import { agentRoutes } from './agent-routes.js';
import { setupWebSocket } from './ws.js';
import { createBridgeMountRouter } from './bridge.js';
import {
  flushAllDocumentsForShutdown,
  getCollabRuntime,
  startCollabRuntimeEmbedded,
  stopCollabRuntime,
} from './collab.js';
import { discoveryRoutes } from './discovery-routes.js';
import { shareWebRoutes } from './share-web-routes.js';
import { setShuttingDown } from './shutdown-state.js';
import {
  capabilitiesPayload,
  enforceApiClientCompatibility,
  enforceBridgeClientCompatibility,
} from './client-capabilities.js';
import { getBuildInfo } from './build-info.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = Number.parseInt(process.env.PORT || '4000', 10);
const DEFAULT_ALLOWED_CORS_ORIGINS = [
  'http://localhost:3000',
  'http://127.0.0.1:3000',
  'http://localhost:4000',
  'http://127.0.0.1:4000',
  'null',
];

function parseAllowedCorsOrigins(): Set<string> {
  const configured = (process.env.PROOF_CORS_ALLOW_ORIGINS || '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  return new Set(configured.length > 0 ? configured : DEFAULT_ALLOWED_CORS_ORIGINS);
}

async function main(): Promise<void> {
  const app = express();
  const server = createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });
  wss.on('error', (error) => {
    console.error('[server] WebSocketServer error (non-fatal):', error);
  });
  const allowedCorsOrigins = parseAllowedCorsOrigins();

  app.use(express.json({ limit: '10mb' }));
  app.use(express.static(path.join(__dirname, '..', 'public')));

  app.use((req, res, next) => {
    const originHeader = req.header('origin');
    if (originHeader && allowedCorsOrigins.has(originHeader)) {
      res.setHeader('Access-Control-Allow-Origin', originHeader);
      res.setHeader('Vary', 'Origin');
    }
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
    res.setHeader(
      'Access-Control-Allow-Headers',
      [
        'Content-Type',
        'Authorization',
        'X-Proof-Client-Version',
        'X-Proof-Client-Build',
        'X-Proof-Client-Protocol',
        'x-share-token',
        'x-bridge-token',
        'x-auth-poll-token',
        'X-Agent-Id',
        'X-Window-Id',
        'X-Document-Id',
        'Idempotency-Key',
        'X-Idempotency-Key',
      ].join(', '),
    );
    if (req.method === 'OPTIONS') {
      res.status(204).end();
      return;
    }
    next();
  });

  app.get('/', (_req, res) => {
    res.type('html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Proof SDK</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, sans-serif; margin: 0; padding: 48px 24px; color: #17261d; background: #f7faf5; }
      main { max-width: 760px; margin: 0 auto; }
      h1 { font-size: 2.5rem; margin: 0 0 0.5rem; }
      p { font-size: 1.05rem; line-height: 1.6; }
      code { background: #eaf2e6; padding: 0.2rem 0.35rem; border-radius: 4px; }
      a { color: #266854; }
    </style>
  </head>
  <body>
    <main>
      <h1>Proof SDK</h1>
      <p>Open-source collaborative markdown editing with provenance tracking and an agent HTTP bridge.</p>
      <p>Start with <code>POST /documents</code>, inspect <a href="/agent-docs">agent docs</a>, or read <a href="/.well-known/agent.json">discovery metadata</a>.</p>
    </main>
  </body>
</html>`);
  });

  app.get('/health', (_req, res) => {
    const buildInfo = getBuildInfo();
    res.json({
      ok: true,
      buildInfo,
      collab: getCollabRuntime(),
    });
  });

  app.get('/api/capabilities', (_req, res) => {
    res.json(capabilitiesPayload());
  });

  app.use(discoveryRoutes);
  app.use('/api', enforceApiClientCompatibility, apiRoutes);
  app.use('/api/agent', agentRoutes);
  app.use(apiRoutes);
  app.use('/d', createBridgeMountRouter(enforceBridgeClientCompatibility));
  app.use('/documents', createBridgeMountRouter(enforceBridgeClientCompatibility));
  app.use('/documents', agentRoutes);
  app.use(shareWebRoutes);

  setupWebSocket(wss);
  await startCollabRuntimeEmbedded(PORT);

  registerGracefulShutdown();

  server.listen(PORT, () => {
    console.log(`[proof-sdk] listening on http://127.0.0.1:${PORT}`);
  });
}

// Wire SIGINT (Ctrl+C) and SIGTERM (process supervisors / Tauri's
// kill on app quit) to proof-sdk's graceful-flush path. Without this
// the collab runtime's debounced persistDoc timers (250ms by default)
// drop their in-memory deltas the moment the process is killed —
// users lose the last keystrokes before shutdown, and on dev restarts
// that loss is much larger because so many edits land inside any
// given debounce window. flushAllDocumentsForShutdown + stopCollab
// Runtime are the existing helpers in collab.ts; this function is the
// missing wiring step that previously left them dormant. Idempotent:
// repeat signals during shutdown are absorbed instead of restarting
// the dance, and a hard timeout prevents a hung flush from leaving
// a zombie process the user has to SIGKILL.
function registerGracefulShutdown(): void {
  let inProgress = false;
  const HARD_TIMEOUT_MS = 5000;

  const shutdown = async (signal: NodeJS.Signals): Promise<void> => {
    if (inProgress) return;
    inProgress = true;
    console.log(`[proof-sdk] ${signal} received, flushing pending writes`);
    setShuttingDown();

    const hardExit = setTimeout(() => {
      console.error('[proof-sdk] graceful shutdown timed out, forcing exit');
      process.exit(1);
    }, HARD_TIMEOUT_MS);
    hardExit.unref();

    try {
      // flushAllDocumentsForShutdown internally waits for collab
      // WebSocket connections to drain (waitForCollabConnectionDrain)
      // before persisting. Calling server.close() here in parallel
      // double-waits on the same WS sockets — the first version of
      // this handler did and reliably blew past the 5s hard timeout
      // because both paths sat waiting for each other. Let the flush
      // helper own connection-drain, then stop the runtime. The OS
      // will close listening sockets when the process exits.
      await flushAllDocumentsForShutdown();
      await stopCollabRuntime({ skipDocFlush: true });
    } catch (error) {
      console.error('[proof-sdk] error during graceful shutdown', error);
      clearTimeout(hardExit);
      process.exit(1);
      return;
    }

    console.log('[proof-sdk] graceful shutdown complete');
    clearTimeout(hardExit);
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((error) => {
  console.error('[proof-sdk] failed to start server', error);
  process.exit(1);
});
