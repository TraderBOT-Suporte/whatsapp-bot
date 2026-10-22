// ===================== server.js (Painel de Sinais) — TURSO EDITION =====================
// v2.40 — RELAY-STRICT-FINAL: 4 correções aplicadas
//         · PATCH A — permitidoPush usa dados.risk_validation (não consolidated.risk_validation)
//         · PATCH B — TRAVA-DIRECIONAL respeita reversões autorizadas pelo motor
//         · PATCH C — Filtro TP/SL crítico aceita _tpslCriticoSizing
//         · PATCH D — PRONTIDAO mostra claramente quando motor bloqueou MATURE
// (ver histórico completo em versões anteriores — mantido igual)

import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '@libsql/client';
import pino from 'pino';
import crypto from 'crypto';
import cron from 'node-cron';
import webpush from 'web-push';

const logger = pino({ level: process.env.LOG_LEVEL || 'info' });

process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled Rejection:', {
    message: reason?.message || String(reason),
    name: reason?.name, code: reason?.code, stack: reason?.stack
  });
  console.error('RAW REJECTION:', reason);
});

process.on('uncaughtException', (err) => {
  logger.error('Uncaught Exception:', {
    message: err?.message || String(err),
    name: err?.name, code: err?.code, stack: err?.stack
  });
  console.error('RAW ERROR:', err);
  console.error('RAW STACK:', err?.stack);
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json({ limit: '5mb' }));

app.get('/service-worker.js', (req, res) => {
  res.set('Content-Type', 'application/javascript; charset=utf-8');
  res.set('Service-Worker-Allowed', '/');
  res.set('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.sendFile(path.join(__dirname, 'public', 'service-worker.js'));
});

app.get('/manifest.json', (req, res) => {
  res.set('Content-Type', 'application/manifest+json; charset=utf-8');
  res.set('Cache-Control', 'public, max-age=3600');
  res.sendFile(path.join(__dirname, 'public', 'manifest.json'));
});

app.get('/privacy', (req, res) => {
  res.set('Cache-Control', 'public, max-age=3600');
  res.sendFile(path.join(__dirname, 'public', 'privacy.html'));
});

app.get('/.well-known/assetlinks.json', (req, res) => {
  res.type('application/json');
  res.sendFile(path.join(__dirname, 'public', '.well-known', 'assetlinks.json'), (err) => {
    if (err) res.status(404).json({ error: 'assetlinks.json ainda não configurado' });
  });
});

app.use(express.static(path.join(__dirname, 'public')));

const PORT = process.env.PORT || 3000;

function normalizeApiUrl(rawUrl) {
  if (!rawUrl || typeof rawUrl !== 'string') return rawUrl;
  const trimmed = rawUrl.trim().replace(/\/+$/, '');
  if (/\/api$/i.test(trimmed)) return trimmed;
  return trimmed + '/api';
}

const ANALYSIS_SERVERS = (() => {
  const servers = {};
  servers['server1'] = {
    id: 'server1',
    name: process.env.ANALYSIS_SERVER_1_NAME || 'Servidor 1',
    url: normalizeApiUrl(
      process.env.ANALYSIS_API_URL_1
      || process.env.ANALYSIS_API_URL
      || 'http://localhost:3001'
    )
  };
  for (let i = 2; i <= 6; i++) {
    const url = process.env[`ANALYSIS_API_URL_${i}`];
    if (!url) continue;
    servers[`server${i}`] = {
      id: `server${i}`,
      name: process.env[`ANALYSIS_SERVER_${i}_NAME`] || `Servidor ${i}`,
      url: normalizeApiUrl(url)
    };
  }
  return servers;
})();

const DEFAULT_SERVER_ID = 'server1';
const VALID_SERVER_IDS = Object.keys(ANALYSIS_SERVERS);
function isValidServerId(id) { return typeof id === 'string' && VALID_SERVER_IDS.includes(id); }
function resolveServerId(id) { return isValidServerId(id) ? id : DEFAULT_SERVER_ID; }
function getServer(id) { return ANALYSIS_SERVERS[resolveServerId(id)]; }
function getAllServersPublic() {
  return VALID_SERVER_IDS.map(id => ({ id, name: ANALYSIS_SERVERS[id].name }));
}

function buildTradeKey(serverId, symbol, mode) {
  return `${resolveServerId(serverId)}_${symbol}_${mode}`;
}
function parseTradeKey(tradeKey) {
  if (!tradeKey || typeof tradeKey !== 'string') return null;
  const parts = tradeKey.split('_');
  if (parts.length < 3) {
    const idx = tradeKey.lastIndexOf('_');
    if (idx <= 0) return null;
    return { serverId: DEFAULT_SERVER_ID, symbol: tradeKey.slice(0, idx), mode: tradeKey.slice(idx + 1) };
  }
  const serverId = parts[0];
  const mode = parts[parts.length - 1];
  const symbol = parts.slice(1, -1).join('_');
  if (!isValidServerId(serverId)) {
    const idx = tradeKey.lastIndexOf('_');
    return { serverId: DEFAULT_SERVER_ID, symbol: tradeKey.slice(0, idx), mode: tradeKey.slice(idx + 1) };
  }
  return { serverId, symbol, mode };
}

// ========== TURSO (libSQL) ==========
let db = null;
let tursoInitialized = false;
let _watchlistsCache = null;
let _watchlistsCacheExpira = 0;
const WATCHLISTS_CACHE_TTL = 5 * 60 * 1000;

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS push_subscriptions (
  id TEXT PRIMARY KEY, subscription TEXT NOT NULL, token_hash TEXT NOT NULL, email TEXT, updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_push_sub_token ON push_subscriptions(token_hash);
CREATE TABLE IF NOT EXISTS user_watchlists (
  token_hash TEXT NOT NULL, server_id TEXT NOT NULL DEFAULT 'server1', email TEXT,
  engine_active INTEGER NOT NULL DEFAULT 0, sniper TEXT NOT NULL DEFAULT '[]',
  cacador TEXT NOT NULL DEFAULT '[]', pescador TEXT NOT NULL DEFAULT '[]',
  baleeiro TEXT NOT NULL DEFAULT '[]', atualizado_em INTEGER NOT NULL,
  PRIMARY KEY (token_hash, server_id)
);
CREATE TABLE IF NOT EXISTS user_preferences (
  token_hash TEXT PRIMARY KEY,
  sniper_early INTEGER NOT NULL DEFAULT 25, sniper_mature INTEGER NOT NULL DEFAULT 40,
  cacador_early INTEGER NOT NULL DEFAULT 28, cacador_mature INTEGER NOT NULL DEFAULT 42,
  pescador_early INTEGER NOT NULL DEFAULT 30, pescador_mature INTEGER NOT NULL DEFAULT 45,
  baleeiro_early INTEGER NOT NULL DEFAULT 35, baleeiro_mature INTEGER NOT NULL DEFAULT 48,
  atualizado_em INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS open_trades (trade_key TEXT PRIMARY KEY, data TEXT NOT NULL, atualizado_em INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS cooldowns (trade_key TEXT PRIMARY KEY, expires_at INTEGER NOT NULL, atualizado_em INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS prontidao_state (
  trade_key TEXT PRIMARY KEY, historico TEXT NOT NULL DEFAULT '[]',
  ativa INTEGER NOT NULL DEFAULT 0, ultimo_score REAL, atualizado_em INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, mode TEXT NOT NULL,
  tipo TEXT NOT NULL, titulo TEXT NOT NULL, corpo TEXT NOT NULL, detalhes TEXT,
  watchers TEXT NOT NULL DEFAULT '[]', score REAL, confidence REAL, zona TEXT,
  entry REAL, take_profit REAL, stop_loss REAL, nivel_prontidao TEXT,
  server_id TEXT DEFAULT 'server1', origem TEXT DEFAULT 'motor', criado_em INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_signals_mode ON signals(mode);
CREATE INDEX IF NOT EXISTS idx_signals_tipo ON signals(tipo);
CREATE INDEX IF NOT EXISTS idx_signals_criado ON signals(criado_em);
`;

async function initializeTurso() {
  const url = process.env.TURSO_DATABASE_URL;
  const authToken = process.env.TURSO_AUTH_TOKEN;
  if (!url || !authToken) { logger.warn('Turso não configurado.'); return false; }
  try {
    db = createClient({ url, authToken });
    const statements = SCHEMA_SQL.split(';').map(s => s.trim()).filter(s => s.length > 0);
    for (const stmt of statements) await db.execute(stmt);

    try { await db.execute('ALTER TABLE prontidao_state ADD COLUMN ultimo_score REAL'); }
    catch (err) { if (!/duplicate column/i.test(err.message || '')) logger.warn('⚠️ Migração ultimo_score:', err.message); }

    try {
      const info = await db.execute("PRAGMA table_info(user_watchlists)");
      const hasServerId = info.rows.some(r => r.name === 'server_id');
      if (!hasServerId) {
        await db.execute(`CREATE TABLE IF NOT EXISTS user_watchlists_new (
          token_hash TEXT NOT NULL, server_id TEXT NOT NULL DEFAULT 'server1', email TEXT,
          engine_active INTEGER NOT NULL DEFAULT 0, sniper TEXT NOT NULL DEFAULT '[]',
          cacador TEXT NOT NULL DEFAULT '[]', pescador TEXT NOT NULL DEFAULT '[]',
          baleeiro TEXT NOT NULL DEFAULT '[]', atualizado_em INTEGER NOT NULL,
          PRIMARY KEY (token_hash, server_id))`);
        await db.execute(`INSERT INTO user_watchlists_new
          (token_hash, server_id, email, engine_active, sniper, cacador, pescador, baleeiro, atualizado_em)
          SELECT token_hash, 'server1', email, engine_active, sniper, cacador, pescador, baleeiro, atualizado_em FROM user_watchlists`);
        await db.execute('DROP TABLE user_watchlists');
        await db.execute('ALTER TABLE user_watchlists_new RENAME TO user_watchlists');
      }
    } catch (err) { logger.warn('⚠️ Migração user_watchlists:', err.message); }

    try { await db.execute("ALTER TABLE signals ADD COLUMN server_id TEXT DEFAULT 'server1'"); }
    catch (err) { if (!/duplicate column/i.test(err.message || '')) logger.warn('⚠️ Migração signals.server_id:', err.message); }

    await db.execute('SELECT 1');
    tursoInitialized = true;
    logger.info('✅ Turso inicializado.');
    return true;
  } catch (err) {
    logger.error('❌ Erro ao inicializar Turso:', err.message);
    return false;
  }
}

const tursoInitPromise = initializeTurso();

// ========== WEB PUSH ==========
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY || '';
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY || '';
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || 'mailto:admin@example.com';
let pushConfigured = false;

if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) {
  try {
    webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
    pushConfigured = true;
    logger.info('Web Push configurado.');
  } catch (err) {
    logger.error('❌ Falha Web Push:', err.message || String(err));
  }
} else {
  logger.warn('VAPID não configurado. Push desativado.');
}

const _subsCache = new Map();
const SUBS_CACHE_TTL = 5 * 60 * 1000;

async function _getSubsDeToken(tokenHash) {
  const agora = Date.now();
  const cached = _subsCache.get(tokenHash);
  if (cached && agora < cached.expira) return cached.subs;
  const result = await db.execute({
    sql: 'SELECT id, subscription, token_hash, email, updated_at FROM push_subscriptions WHERE token_hash = ?',
    args: [tokenHash]
  });
  const subs = result.rows.map(row => ({
    id: row.id,
    data: { subscription: JSON.parse(row.subscription), tokenHash: row.token_hash, email: row.email }
  }));
  _subsCache.set(tokenHash, { subs, expira: agora + SUBS_CACHE_TTL });
  return subs;
}

function invalidarCacheSubs(tokenHash) {
  if (tokenHash) _subsCache.delete(tokenHash);
  else _subsCache.clear();
}

async function sendPushToWatchers(watchers, payload) {
  if (!pushConfigured) { logger.warn('⚠️ [PUSH-SKIP] pushConfigured=false'); return; }
  if (!tursoInitialized) { logger.warn('⚠️ [PUSH-SKIP] Turso indisponível'); return; }
  if (!watchers || watchers.length === 0) { logger.warn('⚠️ [PUSH-SKIP] watchers vazio'); return; }
  logger.info(`📤 [PUSH] ${watchers.length} watcher(s) | "${payload.title?.slice(0,40)}"`);
  try {
    let body = JSON.stringify(payload);
    if (body.length > 4000) {
      if (payload.data && Array.isArray(payload.data.reasons)) payload.data.reasons = payload.data.reasons.slice(0, 5);
      body = JSON.stringify(payload);
    }
    const deletions = [];
    for (const tokenHash of watchers) {
      let subs;
      try { subs = await _getSubsDeToken(tokenHash); }
      catch (err) { logger.error(`❌ [PUSH] ${tokenHash.slice(0, 8)}: ${err.message}`); continue; }
      for (const sub of subs) {
        if (!sub.data.subscription) continue;
        try {
          await webpush.sendNotification(sub.data.subscription, body);
        } catch (err) {
          logger.error(`   ❌ push falhou | status=${err.statusCode || '?'}`);
          if (err.statusCode === 404 || err.statusCode === 410) {
            deletions.push(db.execute({ sql: 'DELETE FROM push_subscriptions WHERE id = ?', args: [sub.id] }));
            _subsCache.delete(tokenHash);
          }
        }
      }
    }
    if (deletions.length) await Promise.all(deletions);
  } catch (err) { logger.error('sendPushToWatchers erro:', err.message); }
}

// ========== MAPEAMENTO DE ATIVOS ==========
const assetGroups = {
  'Cestas de Moedas': ['WLDAUD', 'WLDEUR', 'WLDGBP', 'WLDXAU', 'WLDUSD'],
  'Forex': ['frxAUDCAD', 'frxAUDCHF', 'frxAUDJPY', 'frxAUDNZD', 'frxAUDUSD', 'frxEURCAD', 'frxEURCHF', 'frxEURAUD', 'frxEURGBP', 'frxEURJPY', 'frxEURNZD', 'frxEURUSD', 'frxGBPAUD', 'frxGBPCAD', 'frxGBPCHF', 'frxGBPJPY', 'frxGBPNOK', 'frxGBPNZD', 'frxGBPUSD', 'frxNZDJPY', 'frxNZDUSD', 'frxUSDCAD', 'frxUSDCHF', 'frxUSDJPY', 'frxUSDMXN', 'frxUSDNOK', 'frxUSDPLN', 'frxUSDSEK', 'frxGBPPLN'],
  'Metais': ['frxXAUUSD', 'frxXAGUSD', 'frxXPDUSD', 'frxXPTUSD'],
  'Índices Sintéticos': ['RDBEAR', 'RDBULL', 'RB100', 'RB200', 'stpRNG', 'stpRNG2', 'stpRNG3', 'stpRNG4', 'stpRNG5', 'R_10', 'R_25', 'R_50', 'R_75', 'R_90', 'R_100', '1HZ10V', '1HZ15V', '1HZ25V', '1HZ30V', '1HZ50V', '1HZ75V', '1HZ90V', '1HZ100V', '1HZ150V', '1HZ250V'],
  'Índices OTC': ['OTC_AS51', 'OTC_SX5E', 'OTC_FCHI', 'OTC_GDAXI', 'OTC_AEX', 'OTC_FTSE', 'OTC_SPC', 'OTC_NDX', 'OTC_DJI', 'OTC_HSI', 'OTC_N225', 'OTC_SSMI'],
  'Criptomoedas': ['cryBTCUSD', 'cryETHUSD', 'cryLTCUSD', 'cryBCHUSD', 'cryBNBUSD', 'cryDSHUSD', 'cryIOTUSD', 'cryNEOUSD', 'cryTRXUSD', 'cryXLMUSD', 'cryXMRUSD', 'cryXRPUSD', 'cryZECUSD', 'cryBTCETH', 'cryBTCLTC']
};

const fullAssets = {
  WLDAUD: 'AUD Basket', WLDEUR: 'EUR Basket', WLDGBP: 'GBP Basket', WLDXAU: 'Gold Basket', WLDUSD: 'USD Basket',
  frxAUDCAD: 'AUD/CAD', frxAUDCHF: 'AUD/CHF', frxAUDJPY: 'AUD/JPY', frxAUDNZD: 'AUD/NZD', frxAUDUSD: 'AUD/USD',
  frxEURCAD: 'EUR/CAD', frxEURCHF: 'EUR/CHF', frxEURAUD: 'EUR/AUD', frxEURGBP: 'EUR/GBP', frxEURJPY: 'EUR/JPY',
  frxEURNZD: 'EUR/NZD', frxEURUSD: 'EUR/USD', frxGBPAUD: 'GBP/AUD', frxGBPCAD: 'GBP/CAD', frxGBPCHF: 'GBP/CHF',
  frxGBPJPY: 'GBP/JPY', frxGBPNOK: 'GBP/NOK', frxGBPNZD: 'GBP/NZD', frxGBPUSD: 'GBP/USD', frxNZDJPY: 'NZD/JPY',
  frxNZDUSD: 'NZD/USD', frxUSDCAD: 'USD/CAD', frxUSDCHF: 'USD/CHF', frxUSDJPY: 'USD/JPY', frxUSDMXN: 'USD/MXN',
  frxUSDNOK: 'USD/NOK', frxUSDPLN: 'USD/PLN', frxUSDSEK: 'USD/SEK', frxGBPPLN: 'GBP/PLN',
  frxXAUUSD: 'Gold/USD', frxXAGUSD: 'Silver/USD', frxXPDUSD: 'Palladium/USD', frxXPTUSD: 'Platinum/USD',
  RDBEAR: 'Bear Market Index', RDBULL: 'Bull Market Index', RB100: 'Range Break 100', RB200: 'Range Break 200',
  stpRNG: 'Step Index', stpRNG2: 'Step 200', stpRNG3: 'Step 300', stpRNG4: 'Step 400', stpRNG5: 'Step 500',
  R_10: 'Volatility 10', R_25: 'Volatility 25', R_50: 'Volatility 50', R_75: 'Volatility 75', R_90: 'Volatility 90', R_100: 'Volatility 100',
  '1HZ10V': 'Volatility 10 (1s)', '1HZ15V': 'Volatility 15 (1s)', '1HZ25V': 'Volatility 25 (1s)', '1HZ30V': 'Volatility 30 (1s)',
  '1HZ50V': 'Volatility 50 (1s)', '1HZ75V': 'Volatility 75 (1s)', '1HZ90V': 'Volatility 90 (1s)', '1HZ100V': 'Volatility 100 (1s)',
  '1HZ150V': 'Volatility 150 (1s)', '1HZ250V': 'Volatility 250 (1s)',
  OTC_AS51: 'Australia 200', OTC_SX5E: 'Euro 50', OTC_FCHI: 'France 40', OTC_GDAXI: 'Germany 40',
  OTC_AEX: 'Netherlands 25', OTC_FTSE: 'UK 100', OTC_SPC: 'US 500', OTC_NDX: 'US Tech 100',
  OTC_DJI: 'Wall Street 30', OTC_HSI: 'Hong Kong 50', OTC_N225: 'Japan 225', OTC_SSMI: 'Swiss 20',
  cryBTCUSD: 'Bitcoin/USD', cryETHUSD: 'Ethereum/USD', cryLTCUSD: 'Litecoin/USD', cryBCHUSD: 'Bitcoin Cash/USD',
  cryBNBUSD: 'Binance Coin/USD', cryDSHUSD: 'Dash/USD', cryIOTUSD: 'IOTA/USD', cryNEOUSD: 'Neo/USD',
  cryTRXUSD: 'TRON/USD', cryXLMUSD: 'Stellar/USD', cryXMRUSD: 'Monero/USD', cryXRPUSD: 'Ripple/USD',
  cryZECUSD: 'Zcash/USD', cryBTCETH: 'BTC/ETH', cryBTCLTC: 'BTC/LTC'
};

function cleanSymbolName(symbol) {
  if (!symbol || typeof symbol !== 'string') return '?';
  if (fullAssets[symbol]) return fullAssets[symbol];
  let nome = symbol.replace('frx', '').replace('cry', '').replace('OTC_', '');
  if (nome.length === 6) nome = nome.slice(0, 3) + '/' + nome.slice(3);
  return nome;
}
function getFriendlyName(symbol) { return fullAssets[symbol] || cleanSymbolName(symbol); }

// ========== PLANOS ==========
const PLANOS = {
  7:    { nome: '7 Dias',  maxAtivosPorModo: 3,  prioridade: false },
  30:   { nome: '1 Mês',   maxAtivosPorModo: 5,  prioridade: false },
  90:   { nome: '3 Meses', maxAtivosPorModo: 7,  prioridade: false },
  180:  { nome: '6 Meses', maxAtivosPorModo: 10, prioridade: false },
  365:  { nome: '1 Ano',   maxAtivosPorModo: 10, prioridade: true  },
  9999: { nome: 'Admin',   maxAtivosPorModo: 10, prioridade: true  }
};
const PLANO_DEFAULT = { nome: 'Sem plano', maxAtivosPorModo: 0, prioridade: false };
function getPlano(periodDays) { return PLANOS[periodDays] || PLANO_DEFAULT; }

// ========== AUTH ==========
const TOKEN_CACHE_TTL_MS = 5 * 60 * 1000;
const tokenValidationCache = new Map();
const ADMIN_SECRET = process.env.ADMIN_SECRET || '';

setInterval(() => {
  const now = Date.now();
  for (const [tok, e] of tokenValidationCache.entries()) if (e.expiresAt <= now) tokenValidationCache.delete(tok);
}, 60 * 1000);

app.post('/api/validate-token', async (req, res) => {
  const { token } = req.body || {};
  if (!token || typeof token !== 'string') return res.status(400).json({ valid: false, message: 'Token não fornecido' });
  const API_URL = ANALYSIS_SERVERS[DEFAULT_SERVER_ID].url;
  try {
    const r = await fetch(`${API_URL}/validate-token`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token }) });
    const data = await r.json().catch(() => ({}));
    return res.status(r.status).json(data);
  } catch (err) {
    logger.error('[VALIDATE-PROXY] Erro:', err.message);
    return res.status(503).json({ valid: false, message: 'Serviço de validação indisponível' });
  }
});

async function authMiddleware(req, res, next) {
  const authHeader = req.headers['authorization'];
  if (!authHeader || !authHeader.startsWith('Bearer ')) return res.status(401).json({ error: 'Token não fornecido' });
  const token = authHeader.split('Bearer ')[1].trim();
  if (!token || token.length > 5000) return res.status(401).json({ error: 'Token inválido' });

  if (ADMIN_SECRET && token === ADMIN_SECRET) {
    const tokenHash = crypto.createHash('sha256').update(token).digest('hex').slice(0, 16);
    req.user = { token, tokenHash, email: 'admin@local', name: 'Admin', periodDays: 9999, plano: PLANOS[9999], isAdmin: true };
    return next();
  }

  const cached = tokenValidationCache.get(token);
  if (cached && cached.expiresAt > Date.now()) {
    if (!cached.valid) return res.status(401).json({ error: 'Token inválido ou expirado' });
    req.user = cached.user;
    return next();
  }

  const API_URL = ANALYSIS_SERVERS[DEFAULT_SERVER_ID].url;
  try {
    const response = await fetch(`${API_URL}/validate-token`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token }) });
    const data = await response.json().catch(() => ({}));
    const valid = data && data.valid === true;
    const tokenHash = crypto.createHash('sha256').update(token).digest('hex').slice(0, 16);
    const user = valid ? {
      token, tokenHash,
      email: data.email || data.user?.email || null,
      name: data.name || data.user?.name || null,
      periodDays: data.periodDays || 0,
      plano: getPlano(data.periodDays || 0),
      isAdmin: false
    } : null;
    tokenValidationCache.set(token, { valid, user, expiresAt: Date.now() + TOKEN_CACHE_TTL_MS });
    if (!valid) return res.status(401).json({ error: 'Token inválido ou expirado' });
    req.user = user;
    next();
  } catch (err) {
    logger.error('Erro ao validar token:', err.message);
    return res.status(503).json({ error: 'Serviço de validação indisponível' });
  }
}

// ========== ESTADO ==========
let cronEmExecucao = false;
const MODOS_OK = ['SNIPER', 'CAÇADOR', 'PESCADOR', 'BALEEIRO'];
const CADENCIAS = { SNIPER: 1, 'CAÇADOR': 3, PESCADOR: 10, BALEEIRO: 30 };

const PRONTIDAO_CONFIG = {
  SNIPER:    { cooldownMs: 15 * 60 * 1000, ciclosForaParaArrefecer: 10, scoreDeltaMin: 5  },
  'CAÇADOR': { cooldownMs: 20 * 60 * 1000, ciclosForaParaArrefecer: 5,  scoreDeltaMin: 5  },
  PESCADOR:  { cooldownMs: 60 * 60 * 1000, ciclosForaParaArrefecer: 4,  scoreDeltaMin: 8  },
  BALEEIRO:  { cooldownMs: 120 * 60 * 1000, ciclosForaParaArrefecer: 3, scoreDeltaMin: 10 }
};
function getProntidaoConfig(mode) { return PRONTIDAO_CONFIG[mode] || PRONTIDAO_CONFIG['CAÇADOR']; }

const SCORE_PRONTIDAO_MIN = { SNIPER: 25, 'CAÇADOR': 28, PESCADOR: 30, BALEEIRO: 35 };
const SCORE_QUASE_ENTRADA = { SNIPER: 40, 'CAÇADOR': 42, PESCADOR: 45, BALEEIRO: 48 };
function getScoreProntidaoMin(mode) { return SCORE_PRONTIDAO_MIN[mode] || 30; }
function getScoreQuaseEntrada(mode) { return SCORE_QUASE_ENTRADA[mode] || 40; }

const DEFAULT_PREFS = {
  SNIPER:    { scoreEarly: 25, scoreMature: 40 },
  'CAÇADOR': { scoreEarly: 28, scoreMature: 42 },
  PESCADOR:  { scoreEarly: 30, scoreMature: 45 },
  BALEEIRO:  { scoreEarly: 35, scoreMature: 48 }
};

const prefsCache = new Map();
const PREFS_CACHE_TTL = 5 * 60 * 1000;

function sanitizePrefs(raw) {
  const out = {};
  for (const mode of MODOS_OK) {
    const d = DEFAULT_PREFS[mode];
    const p = raw?.[mode] || {};
    const se = Math.max(0, Math.min(60, parseInt(p.scoreEarly, 10) || d.scoreEarly));
    const sm = Math.max(se + 5, Math.min(95, parseInt(p.scoreMature, 10) || d.scoreMature));
    out[mode] = { scoreEarly: se, scoreMature: sm };
  }
  return out;
}

async function getUserPreferences(tokenHash) {
  if (!tokenHash || !tursoInitialized) return sanitizePrefs({});
  const cached = prefsCache.get(tokenHash);
  if (cached && cached.expiresAt > Date.now()) return cached.prefs;
  try {
    const res = await db.execute({
      sql: 'SELECT sniper_early, sniper_mature, cacador_early, cacador_mature, pescador_early, pescador_mature, baleeiro_early, baleeiro_mature FROM user_preferences WHERE token_hash = ?',
      args: [tokenHash]
    });
    const row = res.rows[0];
    const raw = row ? {
      SNIPER:    { scoreEarly: row.sniper_early,    scoreMature: row.sniper_mature    },
      'CAÇADOR': { scoreEarly: row.cacador_early,   scoreMature: row.cacador_mature   },
      PESCADOR:  { scoreEarly: row.pescador_early,  scoreMature: row.pescador_mature  },
      BALEEIRO:  { scoreEarly: row.baleeiro_early,  scoreMature: row.baleeiro_mature  }
    } : {};
    const prefs = sanitizePrefs(raw);
    prefsCache.set(tokenHash, { prefs, expiresAt: Date.now() + PREFS_CACHE_TTL });
    return prefs;
  } catch (err) {
    logger.error('Erro getUserPreferences:', err.message);
    return sanitizePrefs({});
  }
}

async function saveUserPreferences(tokenHash, raw) {
  if (!tursoInitialized) throw new Error('Turso indisponível');
  const s = sanitizePrefs(raw);
  await db.execute({
    sql: `INSERT INTO user_preferences (
      token_hash, sniper_early, sniper_mature, cacador_early, cacador_mature,
      pescador_early, pescador_mature, baleeiro_early, baleeiro_mature, atualizado_em
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(token_hash) DO UPDATE SET
      sniper_early=excluded.sniper_early, sniper_mature=excluded.sniper_mature,
      cacador_early=excluded.cacador_early, cacador_mature=excluded.cacador_mature,
      pescador_early=excluded.pescador_early, pescador_mature=excluded.pescador_mature,
      baleeiro_early=excluded.baleeiro_early, baleeiro_mature=excluded.baleeiro_mature,
      atualizado_em=excluded.atualizado_em`,
    args: [
      tokenHash,
      s.SNIPER.scoreEarly, s.SNIPER.scoreMature,
      s['CAÇADOR'].scoreEarly, s['CAÇADOR'].scoreMature,
      s.PESCADOR.scoreEarly, s.PESCADOR.scoreMature,
      s.BALEEIRO.scoreEarly, s.BALEEIRO.scoreMature,
      Date.now()
    ]
  });
  prefsCache.delete(tokenHash);
  return s;
}

const PRONTIDAO_GLOBAL_COOLDOWN_MS = 10 * 60 * 1000;
const TRADE_TIMEOUT_POR_MODO_MS = { 'SNIPER': 15*60*1000, 'CAÇADOR': 60*60*1000, 'PESCADOR': 12*60*60*1000, 'BALEEIRO': 72*60*60*1000 };
const TRADE_TIMEOUT_EXTEND_POR_MODO_MS = { 'SNIPER': 8*60*1000, 'CAÇADOR': 30*60*1000, 'PESCADOR': 6*60*60*1000, 'BALEEIRO': 48*60*60*1000 };
const TRADE_TIMEOUT_MS_DEFAULT = 20 * 60 * 1000;
const TRADE_TIMEOUT_EXTEND_MS_DEFAULT = 15 * 60 * 1000;
function getTimeoutModo(mode) { return TRADE_TIMEOUT_POR_MODO_MS[mode] || TRADE_TIMEOUT_MS_DEFAULT; }
function getTimeoutExtendModo(mode) { return TRADE_TIMEOUT_EXTEND_POR_MODO_MS[mode] || TRADE_TIMEOUT_EXTEND_MS_DEFAULT; }
const PROGRESSO_MINIMO_EXTENSAO = 0.05;
const EXTENSOES_MAX = 3;
const INTERVALO_ATUALIZACAO_POR_MODO_MS = { 'SNIPER': 3*60*1000, 'CAÇADOR': 10*60*1000, 'PESCADOR': 30*60*1000, 'BALEEIRO': 120*60*1000 };
function getIntervaloAtualizacao(mode) { return INTERVALO_ATUALIZACAO_POR_MODO_MS[mode] || 10 * 60 * 1000; }

async function getUserWatchlist(tokenHash, serverId = DEFAULT_SERVER_ID) {
  if (!tursoInitialized) return null;
  const sid = resolveServerId(serverId);
  try {
    const res = await db.execute({
      sql: 'SELECT email, engine_active, sniper, cacador, pescador, baleeiro FROM user_watchlists WHERE token_hash = ? AND server_id = ?',
      args: [tokenHash, sid]
    });
    const row = res.rows[0];
    if (!row) return { tokenHash, serverId: sid, email: null, engineActive: false, SNIPER: [], 'CAÇADOR': [], PESCADOR: [], BALEEIRO: [] };
    return {
      tokenHash, serverId: sid, email: row.email || null, engineActive: !!row.engine_active,
      SNIPER: JSON.parse(row.sniper || '[]'), 'CAÇADOR': JSON.parse(row.cacador || '[]'),
      PESCADOR: JSON.parse(row.pescador || '[]'), BALEEIRO: JSON.parse(row.baleeiro || '[]')
    };
  } catch (err) { logger.error('Erro getUserWatchlist:', err.message); return null; }
}

async function saveUserWatchlist(tokenHash, patch, serverId = DEFAULT_SERVER_ID) {
  if (!tursoInitialized) return;
  const sid = resolveServerId(serverId);
  const current = await getUserWatchlist(tokenHash, sid) || {};
  const merged = {
    email: patch.email !== undefined ? patch.email : (current.email || null),
    engineActive: patch.engineActive !== undefined ? patch.engineActive : (current.engineActive || false),
    SNIPER: patch.SNIPER !== undefined ? patch.SNIPER : (current.SNIPER || []),
    'CAÇADOR': patch['CAÇADOR'] !== undefined ? patch['CAÇADOR'] : (current['CAÇADOR'] || []),
    PESCADOR: patch.PESCADOR !== undefined ? patch.PESCADOR : (current.PESCADOR || []),
    BALEEIRO: patch.BALEEIRO !== undefined ? patch.BALEEIRO : (current.BALEEIRO || [])
  };
  await db.execute({
    sql: `INSERT INTO user_watchlists (token_hash, server_id, email, engine_active, sniper, cacador, pescador, baleeiro, atualizado_em)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(token_hash, server_id) DO UPDATE SET
      email=excluded.email, engine_active=excluded.engine_active,
      sniper=excluded.sniper, cacador=excluded.cacador,
      pescador=excluded.pescador, baleeiro=excluded.baleeiro,
      atualizado_em=excluded.atualizado_em`,
    args: [tokenHash, sid, merged.email, merged.engineActive ? 1 : 0,
      JSON.stringify(merged.SNIPER), JSON.stringify(merged['CAÇADOR']),
      JSON.stringify(merged.PESCADOR), JSON.stringify(merged.BALEEIRO), Date.now()]
  });
}

async function getAllUserWatchlists() {
  if (!tursoInitialized) return [];
  const agora = Date.now();
  if (_watchlistsCache && agora < _watchlistsCacheExpira) return _watchlistsCache;
  try {
    const res = await db.execute('SELECT * FROM user_watchlists');
    const resultado = res.rows.map(row => ({
      tokenHash: row.token_hash, serverId: row.server_id || DEFAULT_SERVER_ID,
      email: row.email || null, engineActive: !!row.engine_active,
      SNIPER: JSON.parse(row.sniper || '[]'), 'CAÇADOR': JSON.parse(row.cacador || '[]'),
      PESCADOR: JSON.parse(row.pescador || '[]'), BALEEIRO: JSON.parse(row.baleeiro || '[]')
    }));
    _watchlistsCache = resultado;
    _watchlistsCacheExpira = agora + WATCHLISTS_CACHE_TTL;
    return resultado;
  } catch (err) { logger.error('Erro getAllUserWatchlists:', err.message); return []; }
}
function invalidarCacheWatchlists() { _watchlistsCache = null; _watchlistsCacheExpira = 0; }
function contarAtivosWatchlist(wl) {
  if (!wl) return 0;
  return (wl.SNIPER || []).length + (wl['CAÇADOR'] || []).length + (wl.PESCADOR || []).length + (wl.BALEEIRO || []).length;
}

// ========== ESTADO EM MEMÓRIA ==========
const tradesAbertos = new Map();
const cooldownPosTrade = new Map();
const ultimoSinalPorPar = new Map();
const prontidaoHistorico = new Map();
const prontidaoAtiva = new Set();
const prontidaoForaContagem = new Map();
const COOLDOWN_POS_TRADE_MS = 10 * 60 * 1000;
const prontidaoUltimoEnvio = new Map();
const prontidaoUltimoScoreEnviado = new Map();
const arrefecimentoUltimoEnvio = new Map();
const prontidaoGlobalPorSymbol = new Map();
const ultimaDirecaoPorPar = new Map();

setInterval(() => {
  const agora = Date.now();
  for (const [key, expira] of cooldownPosTrade.entries()) if (agora > expira) cooldownPosTrade.delete(key);
  for (const [key, historico] of prontidaoHistorico.entries()) {
    const ultima = historico[historico.length - 1];
    if (!ultima || agora - ultima.t > 30 * 60 * 1000) {
      prontidaoHistorico.delete(key); prontidaoAtiva.delete(key);
      prontidaoForaContagem.delete(key); prontidaoUltimoEnvio.delete(key);
      prontidaoUltimoScoreEnviado.delete(key); arrefecimentoUltimoEnvio.delete(key);
    }
  }
  for (const [sym, ts] of prontidaoGlobalPorSymbol.entries()) if (agora - ts > 30 * 60 * 1000) prontidaoGlobalPorSymbol.delete(sym);
  for (const [key, data] of ultimaDirecaoPorPar.entries()) if (agora - data.timestamp > 30 * 60 * 1000) ultimaDirecaoPorPar.delete(key);
}, 60 * 1000);

// ========== PERSISTÊNCIA ==========
async function persistTradeOpen(tradeKey, trade) {
  if (!tursoInitialized) return;
  try { await db.execute({ sql: 'INSERT INTO open_trades (trade_key, data, atualizado_em) VALUES (?, ?, ?) ON CONFLICT(trade_key) DO UPDATE SET data=excluded.data, atualizado_em=excluded.atualizado_em', args: [tradeKey, JSON.stringify(trade), Date.now()] }); }
  catch (err) { logger.error('Erro persistTradeOpen:', err.message); }
}
async function persistTradeUpdate(tradeKey, trade) {
  if (!tursoInitialized) return;
  try { await db.execute({ sql: 'INSERT INTO open_trades (trade_key, data, atualizado_em) VALUES (?, ?, ?) ON CONFLICT(trade_key) DO UPDATE SET data=excluded.data, atualizado_em=excluded.atualizado_em', args: [tradeKey, JSON.stringify(trade), Date.now()] }); }
  catch (err) { logger.error('Erro persistTradeUpdate:', err.message); }
}
async function removePersistedTrade(tradeKey) {
  if (!tursoInitialized) return;
  try { await db.execute({ sql: 'DELETE FROM open_trades WHERE trade_key = ?', args: [tradeKey] }); }
  catch (err) { logger.error('Erro removePersistedTrade:', err.message); }
}
async function persistCooldown(tradeKey, expiresAt) {
  if (!tursoInitialized) return;
  try { await db.execute({ sql: 'INSERT INTO cooldowns (trade_key, expires_at, atualizado_em) VALUES (?, ?, ?) ON CONFLICT(trade_key) DO UPDATE SET expires_at=excluded.expires_at, atualizado_em=excluded.atualizado_em', args: [tradeKey, expiresAt, Date.now()] }); }
  catch (err) { logger.error('Erro persistCooldown:', err.message); }
}
async function removePersistedCooldown(tradeKey) {
  if (!tursoInitialized) return;
  try { await db.execute({ sql: 'DELETE FROM cooldowns WHERE trade_key = ?', args: [tradeKey] }); }
  catch (err) { logger.error('Erro removePersistedCooldown:', err.message); }
}
async function persistProntidao(tradeKey, historico, ativa, ultimoScore = null) {
  if (!tursoInitialized) return;
  try {
    await db.execute({
      sql: `INSERT INTO prontidao_state (trade_key, historico, ativa, ultimo_score, atualizado_em)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(trade_key) DO UPDATE SET historico=excluded.historico, ativa=excluded.ativa, ultimo_score=excluded.ultimo_score, atualizado_em=excluded.atualizado_em`,
      args: [tradeKey, JSON.stringify(historico || []), ativa ? 1 : 0,
        (ultimoScore != null && Number.isFinite(Number(ultimoScore))) ? Number(ultimoScore) : null, Date.now()]
    });
  } catch (err) { logger.error('Erro persistProntidao:', err.message); }
}
async function removePersistedProntidao(tradeKey) {
  if (!tursoInitialized) return;
  try { await db.execute({ sql: 'DELETE FROM prontidao_state WHERE trade_key = ?', args: [tradeKey] }); }
  catch (err) { logger.error('Erro removePersistedProntidao:', err.message); }
}

async function loadStateFromTurso() {
  if (!tursoInitialized) return;
  try {
    try {
      const cincoMinAtras = Date.now() - 5 * 60 * 1000;
      const res = await db.execute({ sql: "SELECT symbol, mode, server_id, criado_em FROM signals WHERE tipo = 'SINAL_CONFIRMADO' AND criado_em >= ?", args: [cincoMinAtras] });
      res.rows.forEach(row => { if (row.symbol && row.mode) { const sid = row.server_id || DEFAULT_SERVER_ID; ultimoSinalPorPar.set(buildTradeKey(sid, row.symbol, row.mode), row.criado_em); } });
      logger.info(`♻️ Anti-duplicado: ${res.rows.length} sinal(is) recente(s)`);
    } catch (e) { logger.warn(`⚠️ Anti-dup: ${e.message}`); }

    const tradesRes = await db.execute('SELECT trade_key, data FROM open_trades');
    const agora = Date.now();
    let tradesRestaurados = 0;
    for (const row of tradesRes.rows) {
      const parsed = parseTradeKey(row.trade_key);
      if (!parsed) continue;
      const newKey = buildTradeKey(parsed.serverId, parsed.symbol, parsed.mode);
      const t = JSON.parse(row.data);
      const timeoutAt = t.timeoutAt || (t.timestamp + getTimeoutModo(t.mode));
      if (agora > timeoutAt) t.timeoutAt = agora + 60 * 1000;
      if (t.peakPercentual === undefined) t.peakPercentual = 0;
      if (!t._ctxUltimoEnvio) t._ctxUltimoEnvio = {};
      if (!t._ctxContagem) t._ctxContagem = {};
      if (t._ctxCiclosSemEstado === undefined) t._ctxCiclosSemEstado = 0;
      tradesAbertos.set(newKey, t);
      tradesRestaurados++;
    }

    const cdRes = await db.execute('SELECT trade_key, expires_at FROM cooldowns');
    let cdRestaurados = 0;
    for (const row of cdRes.rows) {
      const parsed = parseTradeKey(row.trade_key);
      if (!parsed) continue;
      const newKey = buildTradeKey(parsed.serverId, parsed.symbol, parsed.mode);
      if (row.expires_at && row.expires_at > agora) { cooldownPosTrade.set(newKey, row.expires_at); cdRestaurados++; }
      else await db.execute({ sql: 'DELETE FROM cooldowns WHERE trade_key = ?', args: [row.trade_key] });
    }

    const prRes = await db.execute('SELECT trade_key, historico, ativa, ultimo_score FROM prontidao_state');
    let prRestaurados = 0;
    for (const row of prRes.rows) {
      const parsed = parseTradeKey(row.trade_key);
      if (!parsed) continue;
      const newKey = buildTradeKey(parsed.serverId, parsed.symbol, parsed.mode);
      const hist = JSON.parse(row.historico || '[]');
      if (Array.isArray(hist) && hist.length > 0) prontidaoHistorico.set(newKey, hist);
      if (row.ativa) prontidaoAtiva.add(newKey);
      if (row.ultimo_score != null && Number.isFinite(Number(row.ultimo_score))) prontidaoUltimoScoreEnviado.set(newKey, Number(row.ultimo_score));
      prRestaurados++;
    }

    logger.info(`♻️ Estado restaurado: ${tradesRestaurados} trade(s), ${cdRestaurados} cooldown(s), ${prRestaurados} prontidão(ões)`);
  } catch (err) { logger.error(`❌ Erro ao carregar estado: ${err.message}`); }
}

// ========== HELPERS ==========

// ⭐ PATCH #PANEL-RELAY-STRICT — direção só quando autorizada
function extrairDirecaoPrep(dados) {
  const _effDir = dados.consolidated?.effectiveDirection;
  const _effSrc = dados.consolidated?.effectiveDirectionSource;
  if ((_effDir === 'CALL' || _effDir === 'PUT') && _effSrc !== 'blocked_by_market_state' && _effSrc !== 'none') return _effDir;
  const _diag = dados.consolidated?.diagnostico_mercado;
  if (_diag?.estado) {
    const _autoriza = _diag.estado === 'PULLBACK_EM_CURSO' || _diag.estado === 'TENDENCIA_FORTE_ALINHADA';
    if (_autoriza &&
        (_diag.direcao === 'CALL' || _diag.direcao === 'PUT') &&
        _diag.acao_recomendada !== 'NAO_ENTRAR_CALL_AGUARDAR_PULLBACK' &&
        _diag.acao_recomendada !== 'NAO_ENTRAR_PUT_AGUARDAR_PULLBACK' &&
        _diag.acao_recomendada !== 'NAO_ENTRAR_AGUARDAR_PULLBACK') {
      return _diag.direcao;
    }
  }
  return null;
}

function diagnosticoProximidade(reasons, diag = null) {
  if (diag?.estado) {
    switch (diag.estado) {
      case 'REVERSAO_CONFIRMADA': return { nivel: 'LONGE', detalhe: '🔄 reversão confirmada — aguarda cruzamento MACD', estado: diag.estado };
      case 'REVERSAO_EM_CURSO': return { nivel: 'LONGE', detalhe: '🔄 reversão em curso — aguarda confirmação', estado: diag.estado };
      case 'TOPO_LOCAL_ALTA': return { nivel: 'PERTO', detalhe: '⛰️ topo local CALL — aguarda pullback p/ 2ª perna', estado: diag.estado };
      case 'FUNDO_LOCAL_BAIXA': return { nivel: 'PERTO', detalhe: '🕳️ fundo local PUT — aguarda pullback p/ 2ª perna', estado: diag.estado };
      case 'PULLBACK_EM_CURSO': return { nivel: 'PERTO', detalhe: '📉 pullback em curso — aguarda fim', estado: diag.estado };
      case 'TENDENCIA_EXAUSTA': return { nivel: 'BLOQUEADO', detalhe: '⚠️ tendência exausta — aguarda correção', estado: diag.estado };
      case 'TENDENCIA_FORTE_ALINHADA': return { nivel: diag.acao_recomendada === 'ENTRADA_VALIDA' ? 'PERTO' : 'EM FORMAÇÃO', detalhe: diag.acao_recomendada === 'ENTRADA_VALIDA' ? '✅ tendência forte — entrada válida' : '✅ tendência forte — aguarda trigger', estado: diag.estado };
      case 'TENDENCIA_FRACA': return { nivel: 'LONGE', detalhe: '⏸️ tendência fraca — aguarda alinhamento', estado: diag.estado };
      case 'INDEFINIDO': return { nivel: 'LONGE', detalhe: '❓ indefinido — aguarda dados', estado: diag.estado };
      default:
        if (diag.estado.startsWith('RESPIRACAO')) return { nivel: 'BLOQUEADO', detalhe: '🌬️ respiração — mercado exausto', estado: diag.estado };
    }
  }
  const texto = (reasons || []).join(' ');
  if (/REVERSÃO CONFIRMADA|REVERSAO CONFIRMADA/i.test(texto)) return { nivel: 'LONGE', detalhe: 'reversão confirmada' };
  if (/EXAUSTÃO MULTI-TF|EXAUSTAO MULTI-TF/i.test(texto)) return { nivel: 'BLOQUEADO', detalhe: 'exaustão multi-TF' };
  if (/em correção real|em correcao real/i.test(texto)) return { nivel: 'LONGE', detalhe: 'pullback a terminar' };
  if (/MACRO AUTHORITY SUAVIZADA|motor cede/i.test(texto)) return { nivel: 'LONGE', detalhe: 'macro cede' };
  if (/MICRO-AGUARDA-FLIP/i.test(texto)) return { nivel: 'PERTO', detalhe: 'micro contra mas a virar' };
  if (/RESPIRAÇÃO|mercado precisa respirar/i.test(texto)) return { nivel: 'BLOQUEADO', detalhe: 'mercado precisa respirar' };
  if (/micro timing bloqueou/i.test(texto)) return { nivel: 'BLOQUEADO', detalhe: 'micro timing contra' };
  if (/CONFLITO|pullback em curso|aguarda histograma|aguarda alinhamento|aguarda flip/i.test(texto)) return { nivel: 'PERTO', detalhe: 'gatilho em ajuste' };
  return { nivel: 'FORMACAO', detalhe: 'aguardando alinhamento' };
}

function avaliarEsticamento(reasons, diag = null) {
  if (diag?.estado) {
    if (diag.estado === 'TOPO_LOCAL_ALTA') return { esticado: true, nivel: 'ALTO', motivo: 'Topo local CALL', estado: diag.estado };
    if (diag.estado === 'FUNDO_LOCAL_BAIXA') return { esticado: true, nivel: 'ALTO', motivo: 'Fundo local PUT', estado: diag.estado };
    if (diag.estado.startsWith('RESPIRACAO')) return { esticado: true, nivel: 'ALTO', motivo: 'Respiração', estado: diag.estado };
    if (diag.estado === 'TENDENCIA_EXAUSTA') return { esticado: true, nivel: 'ALTO', motivo: 'Tendência exausta', estado: diag.estado };
    if (diag.estado === 'REVERSAO_CONFIRMADA') return { esticado: true, nivel: 'ALTO', motivo: 'Reversão confirmada', estado: diag.estado };
  }
  const texto = (reasons || []).join(' ');
  const motorAutorizouPorExplosao = /ADX\s*≥\s*35 explosivo|sobrecompra é continuação/i.test(texto);
  const motorAutorizouPorLock = /DeMarker lock trend_lock_(alta|baixa) a favor do/i.test(texto);
  if (/RESPIRAÇÃO/i.test(texto)) {
    const match = texto.match(/RESPIRAÇÃO\s+(SIMPLES|DUPLA)/i);
    return { esticado: true, nivel: 'ALTO', motivo: `Respiração ${match ? match[1] : 'SIMPLES'}` };
  }
  if (motorAutorizouPorExplosao || motorAutorizouPorLock) {
    return { esticado: true, nivel: 'MÉDIO', motivo: motorAutorizouPorLock ? 'DeMarker lock a favor' : 'ADX explosivo' };
  }
  if (/SINAL ANULADO.*DeMarker extremo/i.test(texto)) return { esticado: true, nivel: 'ALTO', motivo: 'DeMarker extremo' };
  if (/hist a desacelerar/i.test(texto)) return { esticado: true, nivel: 'ALTO', motivo: 'Macro a desacelerar' };
  if (/Reversão ativa detectada/i.test(texto)) return { esticado: true, nivel: 'ALTO', motivo: 'Reversão ativa' };
  return { esticado: false, nivel: 'BAIXO', motivo: 'mercado saudável' };
}

function detectarRespiracaoOuPullback(dados, trade, percentualPercorrido = 0) {
  const reasons = (dados?.consolidated?.score_reasons || []).join(' ');
  const nota = dados?.consolidated?.primaryTrendNote || '';
  const texto = `${reasons} ${nota}`;
  const indicado = /RESPIRAÇÃO|mercado precisa respirar|pullback em curso|pullback saudável|correção saudável|reteste|aguarda alinhamento|aguarda flip/i.test(texto);
  if (!indicado || !trade) return false;
  const distanciaEntrada = Math.abs((dados?.consolidated?.price ?? trade.currentPrice) - trade.entry);
  const alvoTotal = Math.abs(trade.takeProfit - trade.entry);
  const pertoDaEntrada = alvoTotal > 0 && distanciaEntrada <= alvoTotal * 0.18;
  const progressoValido = percentualPercorrido >= -0.12 && percentualPercorrido < 0.35;
  return pertoDaEntrada && progressoValido;
}

function calcularCooldownDinamico(tipoFecho, dados, esticPreCalculado) {
  const zona = dados?.consolidated?.zona || 'C';
  const score = dados?.consolidated?.score || 0;
  const reasons = (dados?.consolidated?.score_reasons || []).join(' ');
  const estic = esticPreCalculado || avaliarEsticamento(dados?.consolidated?.score_reasons, dados?.consolidated?.diagnostico_mercado);
  if (estic.esticado && estic.nivel === 'ALTO') return 15 * 60 * 1000;
  if (tipoFecho === 'WIN' && zona === 'A' && score >= 60 && !estic.esticado) return 3 * 60 * 1000;
  if (tipoFecho === 'STOP' && zona === 'A' && score >= 55 && !estic.esticado) return 5 * 60 * 1000;
  if (tipoFecho === 'TIMEOUT' && /pullback/i.test(reasons)) return 4 * 60 * 1000;
  return COOLDOWN_POS_TRADE_MS;
}

function confirmaExaustaoMultiTF(dados, trade) {
  const tfs = dados?.timeframes || {};
  const mode = trade?.mode;
  const CONFIRM_TF = { 'SNIPER': { trigger: 'M1', confirm: 'M5' }, 'CAÇADOR': { trigger: 'M5', confirm: 'M15' }, 'PESCADOR': { trigger: 'H1', confirm: 'H4' }, 'BALEEIRO': { trigger: 'H4', confirm: 'H24' } };
  const cfg = CONFIRM_TF[mode];
  if (!cfg) return { confirmado: false, motivo: 'modo desconhecido' };
  const confTF = tfs[cfg.confirm];
  if (!confTF) return { confirmado: false, motivo: `${cfg.confirm} sem dados` };
  const confRSI = confTF.rsi ?? 50;
  const confStatus = confTF.macd_phase?.status || {};
  const isCall = trade.signal === 'CALL';
  const macdInvertido = isCall ? confStatus.histograma === '❌ NEGATIVO' : confStatus.histograma === '✅ POSITIVO';
  if (macdInvertido) return { confirmado: true, motivo: `${cfg.confirm} MACD virou contra` };
  const rsiExtremo = isCall ? confRSI >= 75 : confRSI <= 25;
  if (rsiExtremo) return { confirmado: true, motivo: `${cfg.confirm} RSI ${confRSI.toFixed(0)} extremo` };
  return { confirmado: false, motivo: `${cfg.confirm} ainda suporta` };
}

// ========== FORMATAÇÃO DE MENSAGENS ==========

// ⭐ PATCH D — mensagem clara quando motor bloqueou MATURE
function formatarMensagemPrep(symbol, direcao, dados, extras = {}) {
  const nomeAmigavel = getFriendlyName(symbol);
  const dirLabel = direcao === 'CALL' ? 'COMPRA (CALL)' : 'VENDA (PUT)';
  const score = extras.scoreAtual ?? dados.consolidated.score;
  const scoreQuase = extras.scoreQuase ?? 40;
  const nivel = extras.nivelProntidao || 'EARLY';

  const proximidadeDiag = diagnosticoProximidade(dados.consolidated.score_reasons || [], dados.consolidated.diagnostico_mercado);
  const bloqueio = proximidadeDiag && proximidadeDiag.nivel === 'BLOQUEADO';
  const matureBloqueadoPorMotor = dados.consolidated?.mature_aprovado === false;

  let proximidade, detalhe, emoji;
  if (bloqueio) {
    emoji = '⏸️'; proximidade = 'BLOQUEADO';
    detalhe = proximidadeDiag.detalhe || 'bloqueio ativo — aguarda normalizar';
  } else if (nivel === 'MATURE') {
    emoji = '🔥'; proximidade = 'PERTO DE ENTRAR'; detalhe = 'setup quase confirmado — prepara a entrada';
  } else if (matureBloqueadoPorMotor) {
    emoji = '⏸️'; proximidade = 'INFORMATIVO';
    detalhe = dados.consolidated?.mature_motivo_bloqueio
      ? `motor disse: NÃO ENTRAR (${String(dados.consolidated.mature_motivo_bloqueio).substring(0, 100)})`
      : 'motor disse: NÃO ENTRAR';
  } else {
    emoji = '👀'; proximidade = 'EM FORMAÇÃO'; detalhe = 'gatilho em ajuste — prepara-te';
  }

  const prefixoTitulo = bloqueio
    ? `⏸️ Em formação (bloqueado): ${nomeAmigavel}`
    : (matureBloqueadoPorMotor
        ? `⏸️ Informativo (motor: não entrar): ${nomeAmigavel}`
        : (nivel === 'MATURE' ? `🔥 Perto de entrar: ${nomeAmigavel}` : `👀 Em formação: ${nomeAmigavel}`));

  return {
    titulo: prefixoTitulo,
    corpo: `${dirLabel} · ${nomeAmigavel}\n⚡ Score ${score}/100 · ${proximidade}\n💡 ${detalhe}`,
    detalhes: {
      tipo: 'PRONTIDAO', nomeAmigavel, direcao, modo: extras.mode || null,
      score, zona: dados.consolidated.zona, proximidade, detalhe,
      nivelProntidao: nivel, scoreQuase,
      readinessScore: dados.consolidated.readiness_score ?? null,
      diagnosticoMercado: dados.consolidated.diagnostico_mercado || null,
      reasons: (dados.consolidated.score_reasons || []).slice(0, 14),
      price: dados.consolidated.price
    }
  };
}

function formatarMensagemArrefecimento(symbol, score, dados) {
  const nomeAmigavel = getFriendlyName(symbol);
  return {
    titulo: `😴 Prontidão encerrada: ${nomeAmigavel}`,
    corpo: `${nomeAmigavel}\n📉 Score atual ${score}/100 · saiu da Zona B\n✅ A espera anterior foi cancelada`,
    detalhes: { tipo: 'ARREFECIMENTO', nomeAmigavel, score, motivo: 'Setup perdeu força e saiu da Zona B', reasons: (dados?.consolidated?.score_reasons || []).slice(0, 5) }
  };
}

function formatarMensagemSinal(symbol, dados, mode) {
  const { consolidated, suggestion } = dados;
  const emoji = consolidated.signal === 'CALL' ? '🟢' : '🔴';
  const dirLabel = consolidated.signal === 'CALL' ? 'COMPRA (CALL)' : 'VENDA (PUT)';
  const nomeAmigavel = getFriendlyName(symbol);
  const confNum = Number(consolidated?.confidence);
  const conf = (Number.isFinite(confNum) ? (confNum * 100).toFixed(1) : '0.0');
  const zona = consolidated.zona === 'B' ? 'B' : 'A';
  const tituloBase = zona === 'A' ? '🚨 SINAL CONFIRMADO' : '⚠️ SINAL MODERADO';
  const origem = consolidated?._origemSinal || 'motor';
  const origemLabel = origem === 'PULLBACK_RESUMPTION' ? '🔄 Pullback Resumption'
    : origem === 'LIMIAR_TENDENCIA' ? '🚀 Limiar Tendência'
    : '🧠 Motor 3 Camadas';
  return {
    titulo: `${tituloBase}: ${nomeAmigavel} · ${origemLabel}`,
    corpo: `${emoji} ${dirLabel} · ${nomeAmigavel}\n💲 Entrada ${suggestion.entry} · 🎯 TP ${suggestion.takeProfit} · 🛑 SL ${suggestion.stopLoss}\n⚡ Score ${consolidated.score}/100 · Confiança ${conf}% · Zona ${zona}\n🔎 Origem: ${origemLabel}`,
    detalhes: {
      tipo: 'SINAL_CONFIRMADO', nomeAmigavel, direcao: consolidated.signal, modo: mode,
      entry: suggestion.entry, takeProfit: suggestion.takeProfit, stopLoss: suggestion.stopLoss,
      score: consolidated.score,
      readinessScore: consolidated.readiness_score ?? null,
      confidence: conf,
      confidenceBruta: Number.isFinite(Number(consolidated?.confianca_bruta)) ? (Number(consolidated.confianca_bruta) * 100).toFixed(1) : null,
      price: consolidated.price, origem, origemLabel,
      diagnosticoMercado: consolidated.diagnostico_mercado || null,
      reasons: (consolidated.score_reasons || []).slice(0, 14), zona
    }
  };
}

function formatarMensagemAtualizacao(trade, minutosDecorridos) {
  const nome = getFriendlyName(trade.symbol);
  const mins = minutosDecorridos || Math.floor((Date.now() - trade.timestamp) / 60000);
  return { titulo: `⏱️ Atualização (${mins}min): ${nome}`, corpo: `${nome} · ${trade.signal}\n💵 Preço atual ${trade.currentPrice} (entrada ${trade.entry})\n📈 Mantém a posição — trade em curso`, detalhes: { tipo: '5MIN', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss, duracao: mins + 'min' } };
}
function formatarMensagemAceleracao(trade) {
  const nome = getFriendlyName(trade.symbol);
  return { titulo: `🚀 Mercado acelerando: ${nome}`, corpo: `${nome} · ${trade.signal}\n💵 Preço ${trade.currentPrice} — movimento forte\n🎯 Deixe correr até o TP ${trade.takeProfit}`, detalhes: { tipo: 'ACELERACAO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit } };
}
function formatarMensagemSeguindo(trade) {
  const nome = getFriendlyName(trade.symbol);
  return { titulo: `✅ Seguindo o sinal: ${nome}`, corpo: `${nome} · ${trade.signal}\n💵 Preço ${trade.currentPrice} · tendência confirmada\n📊 +30% do alvo percorrido`, detalhes: { tipo: 'SEGUINDO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit } };
}
function formatarMensagemZeroRisco(trade) {
  const nome = getFriendlyName(trade.symbol);
  return { titulo: `🛡️ Zero Risco: ${nome}`, corpo: `${nome} · ${trade.signal}\n✅ +50% do alvo — move SL para a entrada\n🎯 Entrada ${trade.entry} (proteção ativa)`, detalhes: { tipo: 'ZERO_RISCO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit } };
}
function formatarMensagemQuaseLa(trade) {
  const nome = getFriendlyName(trade.symbol);
  return { titulo: `⏳ Quase no alvo: ${nome}`, corpo: `${nome} · ${trade.signal}\n💵 Preço ${trade.currentPrice} · 🎯 Alvo ${trade.takeProfit}\n📊 +80% percorrido — atenção máxima`, detalhes: { tipo: 'QUASE_LA', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit } };
}
function formatarMensagemAutoClose90(trade, currentPrice, minutos) {
  const nome = getFriendlyName(trade.symbol);
  const duracao = minutos || Math.floor((Date.now() - trade.timestamp) / 60000);
  const distanciaTotal = Math.abs(trade.takeProfit - trade.entry);
  const distanciaPercorrida = trade.signal === 'CALL' ? (currentPrice - trade.entry) : (trade.entry - currentPrice);
  const pct = distanciaTotal > 0 ? ((distanciaPercorrida / distanciaTotal) * 100).toFixed(0) : '90';
  return { titulo: `✅ Fecho em lucro parcial: ${nome}`, corpo: `${nome} · ${trade.signal}\n💲 +${pct}% do alvo — ${currentPrice}\n⏱️ Duração: ${duracao}min · fecha a posição`, detalhes: { tipo: 'AUTO_CLOSE_90', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, takeProfit: trade.takeProfit, currentPrice, duracao: duracao + 'min', percentualPercorrido: pct + '%' } };
}
function formatarMensagemPullbackDoPico(trade, currentPrice, minutos, percentualAtual, picoPercentual) {
  const nome = getFriendlyName(trade.symbol);
  const duracao = minutos || Math.floor((Date.now() - trade.timestamp) / 60000);
  return { titulo: `📉 Pullback do pico: ${nome}`, corpo: `${nome} · ${trade.signal}\n⚠️ Pico foi ${(picoPercentual * 100).toFixed(0)}% do alvo, agora ${(percentualAtual * 100).toFixed(0)}%\n✅ Fecha em lucro parcial (${currentPrice})`, detalhes: { tipo: 'PULLBACK_PICO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, takeProfit: trade.takeProfit, currentPrice, duracao: duracao + 'min', percentualPercorrido: (percentualAtual * 100).toFixed(1) + '%', picoPercentual: (picoPercentual * 100).toFixed(1) + '%' } };
}
function formatarMensagemWin(trade) {
  const nome = getFriendlyName(trade.symbol);
  const duracao = Math.floor((Date.now() - trade.timestamp) / 60000);
  return { titulo: `🎯 WIN: ${nome}`, corpo: `${nome} · ${trade.signal} ✅\n💲 Alvo ${trade.takeProfit} atingido!\n⏱️ Duração: ${duracao}min · fecha a posição`, detalhes: { tipo: 'WIN', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, takeProfit: trade.takeProfit, currentPrice: trade.currentPrice, duracao: duracao + 'min' } };
}
function formatarMensagemStop(trade) {
  const nome = getFriendlyName(trade.symbol);
  const duracao = Math.floor((Date.now() - trade.timestamp) / 60000);
  return { titulo: `🛑 Stop Loss: ${nome}`, corpo: `${nome} · ${trade.signal}\n📉 Preço ${trade.currentPrice} bateu SL ${trade.stopLoss}\n⏱️ Duração: ${duracao}min · fecha a posição`, detalhes: { tipo: 'STOP', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, stopLoss: trade.stopLoss, currentPrice: trade.currentPrice, duracao: duracao + 'min' } };
}
function formatarMensagemTempoEsgotado(trade) {
  const nome = getFriendlyName(trade.symbol);
  const duracao = Math.floor((Date.now() - trade.timestamp) / 60000);
  return { titulo: `⏳ Tempo esgotado: ${nome}`, corpo: `${nome} · ${trade.signal}\n💵 Preço perto da entrada (${trade.currentPrice})\n✅ Considera fechar no breakeven`, detalhes: { tipo: 'TEMPO_ESGOTADO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss, duracao: duracao + 'min' } };
}
function formatarMensagemExaustao(trade, currentPrice, tempoDecorridoMin, percentualPercorrido, motivo) {
  const nome = getFriendlyName(trade.symbol);
  const pct = (percentualPercorrido * 100).toFixed(1);
  return { titulo: `⚠️ Exaustão detetada: ${nome}`, corpo: `${nome} · ${trade.signal}\n📉 Tendência a perder força — ${motivo}\n💡 Considera fechar ANTES do SL tocar (${pct}% do alvo · ${tempoDecorridoMin}min)`, detalhes: { tipo: 'EXAUSTAO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss, motivo, percentualPercorrido: pct + '%', duracao: tempoDecorridoMin + 'min' } };
}
function formatarMensagemRespiracaoSaudavel(trade, currentPrice, minutos, motivo, tf) {
  const nome = getFriendlyName(trade.symbol);
  const duracao = minutos || Math.floor((Date.now() - trade.timestamp) / 60000);
  return { titulo: `🌬️ Respiração saudável: ${nome}`, corpo: `${nome} · ${trade.signal}\n🌬️ ${motivo}\n✅ Tendência intacta — mantém posição (${duracao}min)`, detalhes: { tipo: 'RESPIRACAO_SAUDAVEL', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss, motivo, timeframe: tf || null, duracao: duracao + 'min' } };
}
function formatarMensagemCorrecaoReal(trade, currentPrice, minutos, motivo, tf) {
  const nome = getFriendlyName(trade.symbol);
  const duracao = minutos || Math.floor((Date.now() - trade.timestamp) / 60000);
  return { titulo: `📉 Correção real: ${nome}`, corpo: `${nome} · ${trade.signal}\n📉 ${motivo}\n⚠️ Profundidade aumentou — aguarda o flip do ${tf || 'trigger'} (${duracao}min)`, detalhes: { tipo: 'CORRECAO_REAL', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss, motivo, timeframe: tf || null, duracao: duracao + 'min' } };
}
function formatarMensagemReversaoConfirmada(trade, currentPrice, minutos, motivo, tf) {
  const nome = getFriendlyName(trade.symbol);
  const duracao = minutos || Math.floor((Date.now() - trade.timestamp) / 60000);
  return { titulo: `⚠️ Reversão confirmada: ${nome}`, corpo: `${nome} · ${trade.signal}\n🔄 ${motivo}\n💡 Considera SAIR antes do SL (${duracao}min)`, detalhes: { tipo: 'REVERSAO_CONFIRMADA', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss, motivo, timeframe: tf || null, duracao: duracao + 'min' } };
}
function formatarMensagemTimeout(trade, currentPrice, tempoDecorridoMin, motivo) {
  const nome = getFriendlyName(trade.symbol);
  const distanciaTotal = Math.abs(trade.takeProfit - trade.entry);
  const distanciaPercorrida = distanciaTotal > 0 ? (trade.signal === 'CALL' ? (currentPrice - trade.entry) : (trade.entry - currentPrice)) : 0;
  const pct = distanciaTotal > 0 ? ((distanciaPercorrida / distanciaTotal) * 100).toFixed(1) : '0.0';
  return { titulo: `🕰️ Encerrado por timeout: ${nome}`, corpo: `${nome} · ${trade.signal}\n⏱️ ${tempoDecorridoMin}min sem avanço suficiente (${pct}% do alvo)\n❌ Motivo: ${motivo} · fecha a posição`, detalhes: { tipo: 'TIMEOUT', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss, duracao: tempoDecorridoMin + 'min', motivo, percentualPercorrido: pct + '%', extensoes: trade.extensoes || 0 } };
}

// ========== REGISTAR SINAL ==========
async function registrarEEnviarSinal(symbol, mode, tipo, msg, extra = {}, watchers = [], serverId = DEFAULT_SERVER_ID) {
  const sid = resolveServerId(serverId);
  const { titulo, corpo, detalhes } = msg;
  logger.info(`[SINAL][${sid}] ${symbol} (${mode}) [${tipo}] ${titulo} · ${watchers.length} watcher(s)`);
  if (tursoInitialized) {
    try {
      await db.execute({
        sql: `INSERT INTO signals (symbol, mode, tipo, titulo, corpo, detalhes, watchers, score, confidence, zona, entry, take_profit, stop_loss, nivel_prontidao, server_id, origem, criado_em)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [symbol, mode, tipo, titulo, corpo,
          detalhes ? JSON.stringify(detalhes) : null, JSON.stringify(watchers),
          extra.score ?? null, extra.confidence ?? null,
          extra.zona ?? detalhes?.zona ?? null,
          extra.entry ?? detalhes?.entry ?? null,
          extra.takeProfit ?? detalhes?.takeProfit ?? null,
          extra.stopLoss ?? detalhes?.stopLoss ?? null,
          extra.nivelProntidao ?? detalhes?.nivelProntidao ?? null,
          sid, 'motor', Date.now()]
      });
    } catch (err) { logger.error('Erro ao gravar sinal:', err.message); }
  }
  const _TIPOS_COM_HISTORICO = ['SINAL_CONFIRMADO', 'PRONTIDAO', 'ARREFECIMENTO', 'EXAUSTAO', 'RESPIRACAO_SAUDAVEL', 'CORRECAO_REAL', 'REVERSAO_CONFIRMADA'];
  const _openUrl = _TIPOS_COM_HISTORICO.includes(tipo) ? '/?open=signals' : '/';
  await sendPushToWatchers(watchers, { title: titulo, body: corpo, tag: `${symbol}_${mode}_${tipo}_${sid}`, data: { symbol, mode, tipo, serverId: sid, url: _openUrl } });
}

// ========== ANÁLISE ==========
async function buscarSinalAnalise(symbol, mode, serverId = DEFAULT_SERVER_ID) {
  const server = getServer(serverId);
  const API_URL = server.url;
  const adminKey = process.env.ADMIN_SECRET;
  if (!adminKey) { logger.error('❌ ADMIN_SECRET não configurado!'); return null; }
  try {
    const response = await fetch(`${API_URL}/analyze`, {
      method: 'POST',
      headers: { 'x-admin-key': adminKey, 'Content-Type': 'application/json' },
      body: JSON.stringify({ symbol, mode })
    });
    if (!response.ok) { logger.error(`❌ Erro HTTP ${response.status} ao buscar ${symbol} [${server.id}]`); return null; }
    return await response.json();
  } catch (err) { logger.error(`❌ Erro conexão ${symbol} [${server.id}]:`, err.message); return null; }
}

async function _reenviarSinalConfirmado(symbol, mode, tradeKey, watchers, serverId = DEFAULT_SERVER_ID) {
  setTimeout(async () => {
    try {
      const t = tradesAbertos.get(tradeKey);
      if (!t) return;
      if (Date.now() - t.timestamp > 2 * 60 * 1000) return;
      const sid = resolveServerId(serverId);
      const nomeAmigavel = getFriendlyName(symbol);
      await sendPushToWatchers(watchers, {
        title: `🚨 Lembrete: ${nomeAmigavel}`,
        body: `Sinal ${t.signal} ainda ativo — entrada ${t.entry} · TP ${t.takeProfit} · SL ${t.stopLoss}\n⚡ Se não recebeste o sinal anterior, entra agora`,
        tag: `${symbol}_${mode}_SINAL_CONFIRMADO_RETRY_${sid}`,
        data: { symbol, mode, tipo: 'SINAL_CONFIRMADO_RETRY', serverId: sid, url: '/' }
      });
    } catch (err) { logger.error('Erro retry SINAL_CONFIRMADO:', err.message); }
  }, 30 * 1000);
}

// ⭐═══════════════════════════════════════════════════════════════════
// FUNÇÃO PRINCIPAL — analisarEEnviarSinais
// Contém PATCH A, B, C aplicados
// ⭐═══════════════════════════════════════════════════════════════════
async function analisarEEnviarSinais(symbol, mode, watchers = [], serverId = DEFAULT_SERVER_ID) {
  const sid = resolveServerId(serverId);
  const dados = await buscarSinalAnalise(symbol, mode, sid);
  if (!dados || !dados.success) return;

  const _diag = dados.consolidated?.diagnostico_mercado;
  logger.info(`🔬 [RX][${sid}] ${symbol} (${mode}) → signal=${dados.consolidated?.signal} zona=${dados.consolidated?.zona} score=${dados.consolidated?.score} readiness=${dados.consolidated?.readiness_score} mature=${dados.consolidated?.mature_aprovado} diagEstado=${_diag?.estado || 'NONE'} diagAcao=${_diag?.acao_recomendada || 'NONE'}`);

  const agora = Date.now();
  const currentPrice = dados.consolidated.price;
  const tradeKey = buildTradeKey(sid, symbol, mode);

  if (cooldownPosTrade.has(tradeKey)) {
    if (agora < cooldownPosTrade.get(tradeKey)) return;
    cooldownPosTrade.delete(tradeKey);
    removePersistedCooldown(tradeKey);
  }

  const trade = tradesAbertos.get(tradeKey);
  if (trade) {
    trade.currentPrice = currentPrice;
    const esticTradeCache = avaliarEsticamento(dados.consolidated.score_reasons, dados.consolidated.diagnostico_mercado);
    const distanciaTotal = Math.abs(trade.takeProfit - trade.entry);
    const distanciaPercorrida = distanciaTotal > 0 ? (trade.signal === 'CALL' ? (currentPrice - trade.entry) : (trade.entry - currentPrice)) : 0;
    const percentualPercorrido = distanciaTotal > 0 ? (distanciaPercorrida / distanciaTotal) : 0;
    const tempoDecorridoMin = Math.floor((agora - trade.timestamp) / 60000);

    if (percentualPercorrido > (trade.peakPercentual || 0)) trade.peakPercentual = percentualPercorrido;
    const pullbackDoPico = (trade.peakPercentual || 0) - percentualPercorrido;

    const timeoutModo = getTimeoutModo(trade.mode);
    const timeoutExtendModo = getTimeoutExtendModo(trade.mode);
    const emRespiracaoOuPullback = detectarRespiracaoOuPullback(dados, trade, percentualPercorrido);

    {
      const PUSH_CTX_CONFIG = {
        COOLDOWN_MIN: { RESPIRACAO_SAUDAVEL: 12, CORRECAO_REAL: 18, REVERSAO_CONFIRMADA: 30 },
        MAX_POR_TRADE: { RESPIRACAO_SAUDAVEL: 4, CORRECAO_REAL: 3, REVERSAO_CONFIRMADA: 2 },
        HYSTERESIS_CICLOS: 2
      };
      const _txt = (dados.consolidated.score_reasons || []).join(' ');
      const _ehRespiracao = /RESPIRAÇÃO\s+(SIMPLES|DUPLA)|mercado precisa respirar/i.test(_txt) || /pullback em curso|pullback saudável|correção saudável/i.test(_txt);
      const _ehCorrecaoReal = /em correção real|em correcao real/i.test(_txt);
      const _ehReversaoConf = /REVERSÃO CONFIRMADA|REVERSAO CONFIRMADA/i.test(_txt);
      const _estadoAtual = _ehReversaoConf ? 'REVERSAO' : _ehCorrecaoReal ? 'CORRECAO' : _ehRespiracao ? 'RESPIRACAO' : null;
      const _tipoPush = _estadoAtual === 'REVERSAO' ? 'REVERSAO_CONFIRMADA' : _estadoAtual === 'CORRECAO' ? 'CORRECAO_REAL' : _estadoAtual === 'RESPIRACAO' ? 'RESPIRACAO_SAUDAVEL' : null;

      if (!trade._ctxUltimoEnvio) trade._ctxUltimoEnvio = {};
      if (!trade._ctxContagem) trade._ctxContagem = {};
      if (trade._ctxCiclosSemEstado === undefined) trade._ctxCiclosSemEstado = 0;
      if (trade._ctxEstado === undefined) trade._ctxEstado = null;

      if (_estadoAtual === null) {
        trade._ctxCiclosSemEstado++;
        if (trade._ctxCiclosSemEstado >= PUSH_CTX_CONFIG.HYSTERESIS_CICLOS) trade._ctxEstado = null;
      } else { trade._ctxCiclosSemEstado = 0; }

      const _transicao = _estadoAtual !== null && trade._ctxEstado !== _estadoAtual;
      if (_transicao) {
        const _ultimo = trade._ctxUltimoEnvio[_tipoPush] || 0;
        const _contagem = trade._ctxContagem[_tipoPush] || 0;
        const _cooldownMs = PUSH_CTX_CONFIG.COOLDOWN_MIN[_tipoPush] * 60 * 1000;
        const _maxTrade = PUSH_CTX_CONFIG.MAX_POR_TRADE[_tipoPush];
        const _cooldownOk = (Date.now() - _ultimo) >= _cooldownMs;
        const _limiteOk = _contagem < _maxTrade;
        if (_cooldownOk && _limiteOk) {
          const _tfExtraido = (() => { const m = _txt.match(/(RESPIRAÇÃO\s+(?:SIMPLES|DUPLA)[^|]{0,40}|M[0-9]+|H[0-9]+)/i); return m ? m[1] : null; })();
          const _motivoCurto = (() => {
            if (_ehReversaoConf) { const m = _txt.match(/REVERS[ÃA]O CONFIRMADA[^|]*/i); return m ? m[0].substring(0, 100) : 'reversão confirmada'; }
            if (_ehCorrecaoReal) { const m = _txt.match(/em corre[çc][ãa]o real[^|]*/i); return m ? m[0].substring(0, 100) : 'correção real'; }
            const m = _txt.match(/RESPIRAÇÃO\s+(?:SIMPLES|DUPLA)[^|]*/i) || _txt.match(/pullback[^|]*/i);
            return m ? m[0].substring(0, 100) : 'mercado a respirar';
          })();
          if (_estadoAtual === 'REVERSAO') {
            await registrarEEnviarSinal(symbol, mode, 'REVERSAO_CONFIRMADA', formatarMensagemReversaoConfirmada(trade, currentPrice, tempoDecorridoMin, _motivoCurto, _tfExtraido), { score: dados.consolidated.score }, watchers, sid);
          } else if (_estadoAtual === 'CORRECAO') {
            await registrarEEnviarSinal(symbol, mode, 'CORRECAO_REAL', formatarMensagemCorrecaoReal(trade, currentPrice, tempoDecorridoMin, _motivoCurto, _tfExtraido), { score: dados.consolidated.score }, watchers, sid);
          } else if (_estadoAtual === 'RESPIRACAO') {
            await registrarEEnviarSinal(symbol, mode, 'RESPIRACAO_SAUDAVEL', formatarMensagemRespiracaoSaudavel(trade, currentPrice, tempoDecorridoMin, _motivoCurto, _tfExtraido), { score: dados.consolidated.score }, watchers, sid);
          }
          trade._ctxUltimoEnvio[_tipoPush] = Date.now();
          trade._ctxContagem[_tipoPush] = _contagem + 1;
          trade._ctxEstado = _estadoAtual;
        }
      }
    }

    const timeoutAtual = trade.timeoutAt || (trade.timestamp + timeoutModo);
    if (agora > timeoutAtual && emRespiracaoOuPullback && (trade.extensoesPullback || 0) < 3) {
      trade.timeoutAt = agora + timeoutExtendModo;
      trade.extensoesPullback = (trade.extensoesPullback || 0) + 1;
      trade.percentualNoUltimoCheck = percentualPercorrido;
      persistTradeUpdate(tradeKey, trade);
    }
    if (agora > timeoutAtual && !(emRespiracaoOuPullback && (trade.extensoesPullback || 0) < 3)) {
      const extensoes = trade.extensoes || 0;
      const progressoAnterior = trade.percentualNoUltimoCheck || 0;
      const progressoNovo = percentualPercorrido - progressoAnterior;
      const estaAvancando = progressoNovo >= PROGRESSO_MINIMO_EXTENSAO;
      if (estaAvancando && extensoes < EXTENSOES_MAX) {
        trade.timeoutAt = agora + timeoutExtendModo;
        trade.extensoes = extensoes + 1;
        trade.percentualNoUltimoCheck = percentualPercorrido;
        persistTradeUpdate(tradeKey, trade);
      } else {
        const motivo = extensoes >= EXTENSOES_MAX ? `limite de ${EXTENSOES_MAX} extensões` : `sem avanço suficiente`;
        await registrarEEnviarSinal(symbol, mode, 'TIMEOUT', formatarMensagemTimeout(trade, currentPrice, tempoDecorridoMin, motivo), { score: dados.consolidated.score }, watchers, sid);
        tradesAbertos.delete(tradeKey);
        removePersistedTrade(tradeKey);
        const cdMsTimeout = calcularCooldownDinamico('TIMEOUT', dados, esticTradeCache);
        cooldownPosTrade.set(tradeKey, agora + cdMsTimeout);
        persistCooldown(tradeKey, agora + cdMsTimeout);
        return;
      }
    }

    let msgObj = null, tipo = null, fecharTrade = false;
    const LIMITE_PRATICO_TP = 0.90;
    const tempoMinAutoClose = Math.floor((agora - trade.timestamp) / 60000);
    const autoCloseAtivo = tempoMinAutoClose >= 3;
    let tpEfetivo = null;
    if (trade.signal === 'CALL') tpEfetivo = trade.entry + (trade.takeProfit - trade.entry) * LIMITE_PRATICO_TP;
    else if (trade.signal === 'PUT') tpEfetivo = trade.entry - (trade.entry - trade.takeProfit) * LIMITE_PRATICO_TP;
    const tpAlvoRealmenteAtingido = (trade.signal === 'CALL' && currentPrice >= trade.takeProfit) || (trade.signal === 'PUT' && currentPrice <= trade.takeProfit);
    const tpEfetivoAtingido = autoCloseAtivo && tpEfetivo !== null && ((trade.signal === 'CALL' && currentPrice >= tpEfetivo) || (trade.signal === 'PUT' && currentPrice <= tpEfetivo));

    if (tpAlvoRealmenteAtingido) { msgObj = formatarMensagemWin(trade); tipo = 'WIN'; fecharTrade = true; }
    else if (tpEfetivoAtingido && !tpAlvoRealmenteAtingido) { msgObj = formatarMensagemAutoClose90(trade, currentPrice, tempoMinAutoClose); tipo = 'WIN'; fecharTrade = true; }
    else if (trade.signal === 'CALL' && currentPrice <= trade.stopLoss) { msgObj = formatarMensagemStop(trade); tipo = 'STOP'; fecharTrade = true; }
    else if (trade.signal === 'PUT' && currentPrice >= trade.stopLoss) { msgObj = formatarMensagemStop(trade); tipo = 'STOP'; fecharTrade = true; }
    else if (!trade.avisoExaustaoEnviado && percentualPercorrido >= 0.10 && percentualPercorrido < 0.50) {
      const contraDirecao = (trade.signal === 'CALL' && /sobrecompra|RSI extremo alto/i.test(esticTradeCache.motivo)) || (trade.signal === 'PUT' && /sobrevenda|RSI extremo baixo/i.test(esticTradeCache.motivo));
      if (esticTradeCache.esticado && esticTradeCache.nivel === 'ALTO' && contraDirecao) {
        const confirmacao = confirmaExaustaoMultiTF(dados, trade);
        if (confirmacao.confirmado) { msgObj = formatarMensagemExaustao(trade, currentPrice, tempoDecorridoMin, percentualPercorrido, `${esticTradeCache.motivo} · ${confirmacao.motivo}`); tipo = 'EXAUSTAO'; trade.avisoExaustaoEnviado = true; }
      }
    }
    else if (!emRespiracaoOuPullback && !trade.avisoTempoEsgotadoEnviado && tempoDecorridoMin >= 10 && percentualPercorrido < 0.15) { msgObj = formatarMensagemTempoEsgotado(trade); tipo = 'TEMPO_ESGOTADO'; trade.avisoTempoEsgotadoEnviado = true; fecharTrade = true; }
    else if (!trade.avisoAceleracaoEnviado && tempoDecorridoMin <= 2 && percentualPercorrido >= 0.40) { msgObj = formatarMensagemAceleracao(trade); tipo = 'ACELERACAO'; trade.avisoAceleracaoEnviado = true; trade.avisoSeguindoEnviado = true; }
    else if (!trade.avisoSeguindoEnviado && percentualPercorrido >= 0.30) { msgObj = formatarMensagemSeguindo(trade); tipo = 'SEGUINDO'; trade.avisoSeguindoEnviado = true; }
    else if ((!trade.ultimaAtualizacaoEnviada || (agora - trade.ultimaAtualizacaoEnviada) >= getIntervaloAtualizacao(trade.mode)) && percentualPercorrido > 0.10 && percentualPercorrido < 0.50) { msgObj = formatarMensagemAtualizacao(trade, tempoDecorridoMin); tipo = '5MIN'; trade.ultimaAtualizacaoEnviada = agora; }
    else if (!trade.avisoZeroRiscoEnviado && percentualPercorrido >= 0.50) { msgObj = formatarMensagemZeroRisco(trade); tipo = 'ZERO_RISCO'; trade.avisoZeroRiscoEnviado = true; }
    else if (!trade.avisoQuaseLaEnviado && percentualPercorrido >= 0.80) { msgObj = formatarMensagemQuaseLa(trade); tipo = 'QUASE_LA'; trade.avisoQuaseLaEnviado = true; }
    else if (!trade.avisoPullbackDoPicoEnviado && (trade.peakPercentual || 0) >= 0.60 && pullbackDoPico >= 0.20 && percentualPercorrido >= 0.25) {
      msgObj = formatarMensagemPullbackDoPico(trade, currentPrice, tempoDecorridoMin, percentualPercorrido, trade.peakPercentual);
      tipo = 'WIN'; fecharTrade = true; trade.avisoPullbackDoPicoEnviado = true;
    }

    if (fecharTrade) {
      tradesAbertos.delete(tradeKey);
      removePersistedTrade(tradeKey);
      const cdMs = calcularCooldownDinamico(tipo, dados, esticTradeCache);
      cooldownPosTrade.set(tradeKey, agora + cdMs);
      persistCooldown(tradeKey, agora + cdMs);
    } else {
      persistTradeUpdate(tradeKey, trade);
    }

    if (msgObj) await registrarEEnviarSinal(symbol, mode, tipo, msgObj, { score: dados.consolidated.score }, watchers, sid);
    return;
  }

  const ultimoTs = ultimoSinalPorPar.get(tradeKey) || 0;
  const CINCO_MIN_MS = 5 * 60 * 1000;
  if (Date.now() - ultimoTs < CINCO_MIN_MS) {
    logger.info(`⏭️ Anti-duplicado: sinal há ${Math.round((Date.now() - ultimoTs)/1000)}s para ${symbol}/${mode} [${sid}]`);
    return;
  }

  // ⭐ PATCH A — corrigido: dados.risk_validation (não consolidated.risk_validation)
  const consolidadoPush = dados.consolidated || {};
  const sinalPush = String(consolidadoPush.signal || 'HOLD').toUpperCase();
  const permitidoPush = consolidadoPush.permitido !== false &&
    consolidadoPush._permitidoReal !== false &&
    dados.risk_validation?.operavel !== false;

  if (!permitidoPush && !tradesAbertos.has(tradeKey)) {
    logger.info(`⏸️ [PUSH-GUARD][${sid}] ${symbol} (${mode}) bloqueado: permitido=false`);
    return;
  }

  if (sinalPush !== 'HOLD' && (dados.consolidated.zona === 'A' || dados.consolidated.zona === 'B')) {
    if (dados.suggestion && dados.suggestion.action === 'ENTRADA' &&
        dados.suggestion.entry != null && dados.suggestion.takeProfit != null && dados.suggestion.stopLoss != null) {

      const esticSinal = avaliarEsticamento(dados.consolidated.score_reasons, dados.consolidated.diagnostico_mercado);

      const temAvisoCritico = (dados.consolidated.score_reasons || []).some(r =>
        typeof r === 'string' && (r.includes('TP/SL apertados') || r.includes('zona CRÍTICO'))
      );

      // ⭐ PATCH C — aceita quando motor compensou o risco (sizing reduzido)
      const _motorCompensouRisco =
        dados.consolidated._tpslCriticoSizing != null &&
        dados.consolidated._tpslCriticoSizing > 0 &&
        (dados.consolidated.score ?? 0) >= 55 &&
        (dados.consolidated.zona === 'A' || dados.consolidated.zona === 'B') &&
        (dados.consolidated.confidence ?? 0) >= 0.50;

      if (((esticSinal.esticado && esticSinal.nivel === 'ALTO') || temAvisoCritico) && !_motorCompensouRisco) {
        logger.info(`⛔ [FILTRO CRÍTICO][${sid}] ${symbol} (${mode}) SINAL ignorado — ${temAvisoCritico ? 'TP/SL Crítico' : esticSinal.motivo}`);
        prontidaoAtiva.delete(tradeKey);
        prontidaoHistorico.delete(tradeKey);
        prontidaoForaContagem.delete(tradeKey);
        prontidaoUltimoScoreEnviado.delete(tradeKey);
        return;
      }

      if (_motorCompensouRisco && temAvisoCritico) {
        logger.warn(`⚠️ [TP-SL-CRITICO-CONFLUENCE][${sid}] ${symbol} (${mode}) relay com aviso — motor compensou (sizing=${dados.consolidated._tpslCriticoSizing}× score=${dados.consolidated.score} conf=${(dados.consolidated.confidence*100).toFixed(0)}%)`);
      }

      // ⭐ PATCH B — TRAVA-DIRECIONAL respeita reversões autorizadas
      const _diagRev = dados.consolidated?.diagnostico_mercado;
      const _origemRev = dados.consolidated?._origemSinal;
      const _ehReversaoAutorizada =
        _origemRev === 'REVERSAL_EARLY' || _origemRev === 'CASCADE_ENTRY' ||
        _origemRev === 'REVERSAO_A_FAVOR' || _origemRev === 'MACRO_REVERSAO_LED' ||
        _diagRev?.estado === 'REVERSAO_CONFIRMADA' ||
        _diagRev?.estado === 'REVERSAO_LIDERADA' ||
        _diagRev?.estado === 'REVERSAL_EARLY_ENTRY';

      const ultimaDir = ultimaDirecaoPorPar.get(tradeKey);
      if (ultimaDir && ultimaDir.sinal !== dados.consolidated.signal) {
        const minutosPassados = (Date.now() - ultimaDir.timestamp) / 60000;
        if (minutosPassados < 30) {
          if (_ehReversaoAutorizada) {
            logger.info(`✅ [DIRECTION-LOCK-OVERRIDE][${sid}] ${symbol} (${mode}) ${dados.consolidated.signal} ACEITE — reversão autorizada (${_origemRev || _diagRev?.estado || 'N/A'})`);
          } else {
            logger.warn(`⛔ [DIRECTION-LOCK][${sid}] ${symbol} (${mode}) ${dados.consolidated.signal} BLOQUEADO. Último foi ${ultimaDir.sinal} há ${minutosPassados.toFixed(0)}min`);
            return;
          }
        }
      }

      const novoTrade = {
        symbol, mode, serverId: sid,
        signal: dados.consolidated.signal,
        entry: dados.suggestion.entry,
        takeProfit: dados.suggestion.takeProfit,
        stopLoss: dados.suggestion.stopLoss,
        zonaEntrada: dados.consolidated.zona,
        watchers: [...watchers],
        timestamp: agora,
        avisoSeguindoEnviado: false, avisoZeroRiscoEnviado: false,
        avisoQuaseLaEnviado: false, avisoAceleracaoEnviado: false,
        avisoTempoEsgotadoEnviado: false, avisoExaustaoEnviado: false,
        ultimaAtualizacaoEnviada: null,
        timeoutAt: agora + getTimeoutModo(mode),
        extensoes: 0, percentualNoUltimoCheck: 0,
        peakPercentual: 0, avisoPullbackDoPicoEnviado: false,
        _ctxEstado: null, _ctxUltimoEnvio: {}, _ctxContagem: {}, _ctxCiclosSemEstado: 0
      };

      tradesAbertos.set(tradeKey, novoTrade);
      persistTradeOpen(tradeKey, novoTrade);
      ultimoSinalPorPar.set(tradeKey, agora);
      ultimaDirecaoPorPar.set(tradeKey, { sinal: dados.consolidated.signal, timestamp: agora });

      try {
        const msgSinal = formatarMensagemSinal(symbol, dados, mode);
        await registrarEEnviarSinal(symbol, mode, 'SINAL_CONFIRMADO', msgSinal, {
          score: dados.consolidated.score, confidence: dados.consolidated.confidence,
          zona: dados.consolidated.zona,
          entry: dados.suggestion.entry, takeProfit: dados.suggestion.takeProfit, stopLoss: dados.suggestion.stopLoss
        }, watchers, sid);
      } catch (errSinal) { logger.error(`❌ FALHA SINAL_CONFIRMADO ${symbol} [${sid}]: ${errSinal.message}`); }

      _reenviarSinalConfirmado(symbol, mode, tradeKey, watchers, sid);
      prontidaoAtiva.delete(tradeKey);
      prontidaoHistorico.delete(tradeKey);
      prontidaoForaContagem.delete(tradeKey);
      prontidaoUltimoEnvio.delete(tradeKey);
      prontidaoUltimoScoreEnviado.delete(tradeKey);
      arrefecimentoUltimoEnvio.delete(tradeKey);
      removePersistedProntidao(tradeKey);
    }
  }
  else if (dados.consolidated.signal === 'HOLD') {
    const scoreAtual = (dados.consolidated.readiness_score ?? dados.consolidated.score ?? 0);
    const scoreAtualNum = Number(scoreAtual) || 0;

    if (prontidaoAtiva.has(tradeKey)) {
      const cfgArr = getProntidaoConfig(mode);
      const zonaAtual = dados.consolidated.zona;
      const scoreMinArr = getScoreProntidaoMin(mode);
      const saiuDeProntidao = (zonaAtual === 'C' && scoreAtualNum < scoreMinArr);
      if (saiuDeProntidao) {
        const fora = (prontidaoForaContagem.get(tradeKey) || 0) + 1;
        prontidaoForaContagem.set(tradeKey, fora);
        if (fora >= cfgArr.ciclosForaParaArrefecer) {
          const ultimoArref = arrefecimentoUltimoEnvio.get(tradeKey) || 0;
          if ((agora - ultimoArref) >= cfgArr.cooldownMs) {
            await registrarEEnviarSinal(symbol, mode, 'ARREFECIMENTO', formatarMensagemArrefecimento(symbol, dados.consolidated.score, dados), { score: dados.consolidated.score }, watchers, sid);
            arrefecimentoUltimoEnvio.set(tradeKey, agora);
          }
          prontidaoAtiva.delete(tradeKey);
          prontidaoHistorico.delete(tradeKey);
          prontidaoForaContagem.delete(tradeKey);
          prontidaoUltimoScoreEnviado.delete(tradeKey);
          removePersistedProntidao(tradeKey);
          return;
        }
      } else { prontidaoForaContagem.delete(tradeKey); }
    }

    const _acaoDiag = _diag?.acao_recomendada;
    const _estadoDiag = _diag?.estado;
    if (_acaoDiag === 'NAO_ENTRAR_CALL_AGUARDAR_PULLBACK' || _acaoDiag === 'NAO_ENTRAR_PUT_AGUARDAR_PULLBACK') {
      prontidaoForaContagem.delete(tradeKey);
      return;
    }
    if (_acaoDiag === 'NAO_ENTRAR_AGUARDAR_PULLBACK' || _acaoDiag === 'AGUARDAR_ALINHAMENTO' || _acaoDiag === 'AGUARDAR_DEFINICAO') {
      prontidaoForaContagem.delete(tradeKey);
      return;
    }

    const regimeAtualPush = dados.consolidated.regime || 'UNKNOWN';
    if (regimeAtualPush === 'CHOP') { prontidaoForaContagem.delete(tradeKey); return; }

    const estic = avaliarEsticamento(dados.consolidated.score_reasons, dados.consolidated.diagnostico_mercado);
    if (estic.esticado && estic.nivel === 'ALTO') { prontidaoForaContagem.delete(tradeKey); return; }

    const matureAprovadoMotor = dados.consolidated?.mature_aprovado;
    const matureMotivo = dados.consolidated?.mature_motivo_bloqueio || null;
    const proximidadePront = diagnosticoProximidade(dados.consolidated.score_reasons, dados.consolidated.diagnostico_mercado);
    const bloqueioAtivo = proximidadePront && proximidadePront.nivel === 'BLOQUEADO';

    let deveForcarEarly;
    if (matureAprovadoMotor === true) deveForcarEarly = false;
    else if (matureAprovadoMotor === false) deveForcarEarly = true;
    else {
      const scoreQuaseEntrada = getScoreQuaseEntrada(mode);
      deveForcarEarly = bloqueioAtivo || scoreAtualNum < scoreQuaseEntrada;
    }

    let _forcarSomenteEarly = deveForcarEarly;

    const prefsPorWatcher = new Map();
    for (const tk of watchers) {
      const prefs = await getUserPreferences(tk);
      prefsPorWatcher.set(tk, prefs[mode] || DEFAULT_PREFS[mode]);
    }

    const scoreGlobalMin = prefsPorWatcher.size > 0
      ? Math.min(...Array.from(prefsPorWatcher.values()).map(p => p.scoreEarly))
      : getScoreProntidaoMin(mode);

    if (dados.consolidated.zona === 'C' && scoreAtual < scoreGlobalMin) { prontidaoForaContagem.delete(tradeKey); return; }

    prontidaoForaContagem.delete(tradeKey);
    const historico = prontidaoHistorico.get(tradeKey) || [];
    historico.push({ score: scoreAtual, t: agora });
    if (historico.length > 5) historico.shift();
    prontidaoHistorico.set(tradeKey, historico);
    persistProntidao(tradeKey, historico, prontidaoAtiva.has(tradeKey),
      prontidaoUltimoScoreEnviado.has(tradeKey) ? prontidaoUltimoScoreEnviado.get(tradeKey) : null);

    const cfg = getProntidaoConfig(mode);
    const ultimoEnvio = prontidaoUltimoEnvio.get(tradeKey) || 0;
    const ultimoScoreEnviado = prontidaoUltimoScoreEnviado.get(tradeKey);
    const primeiraVez = !prontidaoUltimoEnvio.has(tradeKey);
    const passouCooldown = (agora - ultimoEnvio) >= cfg.cooldownMs;
    const scoreSubiuOSuficiente = ultimoScoreEnviado === undefined || (scoreAtualNum - ultimoScoreEnviado) >= (cfg.scoreDeltaMin || 5);
    const podeEnviarAgora = primeiraVez || (passouCooldown && scoreSubiuOSuficiente);

    const globalKey = `${sid}_${symbol}`;
    const ultimoGlobal = prontidaoGlobalPorSymbol.get(globalKey) || 0;
    const podeEnviarGlobal = (agora - ultimoGlobal) >= PRONTIDAO_GLOBAL_COOLDOWN_MS;

    if (podeEnviarAgora && podeEnviarGlobal) {
      const direcaoPrep = extrairDirecaoPrep(dados);
      if (direcaoPrep) {
        const subindo = historico.length >= 3 && historico[historico.length - 1].score > historico[0].score;
        const earlyTks = [], matureTks = [];
        for (const tk of watchers) {
          const p = prefsPorWatcher.get(tk) || DEFAULT_PREFS[mode];
          if (scoreAtual < p.scoreEarly) continue;
          if (!_forcarSomenteEarly && scoreAtual >= p.scoreMature) matureTks.push(tk);
          else earlyTks.push(tk);
        }
        if (matureTks.length > 0) {
          await registrarEEnviarSinal(symbol, mode, 'PRONTIDAO',
            formatarMensagemPrep(symbol, direcaoPrep, dados, { subindo, historico, mode, nivelProntidao: 'MATURE', scoreAtual, scoreQuase: scoreAtual }),
            { score: scoreAtual, nivelProntidao: 'MATURE' }, matureTks, sid);
        }
        if (earlyTks.length > 0) {
          await registrarEEnviarSinal(symbol, mode, 'PRONTIDAO',
            formatarMensagemPrep(symbol, direcaoPrep, dados, { subindo, historico, mode, nivelProntidao: 'EARLY', scoreAtual, scoreQuase: 999 }),
            { score: scoreAtual, nivelProntidao: 'EARLY' }, earlyTks, sid);
        }
        if (matureTks.length > 0 || earlyTks.length > 0) {
          prontidaoAtiva.add(tradeKey);
          prontidaoUltimoEnvio.set(tradeKey, agora);
          prontidaoUltimoScoreEnviado.set(tradeKey, scoreAtualNum);
          prontidaoGlobalPorSymbol.set(globalKey, agora);
          prontidaoForaContagem.set(tradeKey, 0);
          persistProntidao(tradeKey, prontidaoHistorico.get(tradeKey), true, scoreAtualNum);
        }
      }
    }
  }
}

// ========== CRON ==========
cron.schedule('* * * * *', async () => {
  if (cronEmExecucao) { logger.warn('⏭️ Ciclo anterior ainda em execução — skip'); return; }
  cronEmExecucao = true;
  try {
    const allUsers = await getAllUserWatchlists();
    const activeUsers = allUsers.filter(u => u.engineActive);
    const queue = new Map();
    for (const tradeKey of tradesAbertos.keys()) {
      const parsed = parseTradeKey(tradeKey);
      if (!parsed) continue;
      queue.set(`${parsed.serverId}|${parsed.symbol}|${parsed.mode}`, parsed);
    }
    const minutoAtual = Math.floor(Date.now() / 60000);
    for (const u of activeUsers) {
      const sid = u.serverId || DEFAULT_SERVER_ID;
      for (const mode of MODOS_OK) {
        const cad = CADENCIAS[mode] || 3;
        if (minutoAtual % cad !== 0) continue;
        for (const symbol of u[mode] || []) queue.set(`${sid}|${symbol}|${mode}`, { serverId: sid, symbol, mode });
      }
    }
    if (queue.size === 0) return;
    logger.info(`📦 Ciclo: ${queue.size} par(es) · ${activeUsers.length} user(s) ativos`);
    for (const { serverId, symbol, mode } of queue.values()) {
      const tradeKey = buildTradeKey(serverId, symbol, mode);
      const trade = tradesAbertos.get(tradeKey);
      let watchers;
      if (trade && Array.isArray(trade.watchers)) watchers = trade.watchers;
      else watchers = activeUsers.filter(u => (u.serverId || DEFAULT_SERVER_ID) === serverId && (u[mode] || []).includes(symbol)).map(u => u.tokenHash);
      await analisarEEnviarSinais(symbol, mode, watchers, serverId);
      await new Promise(r => setTimeout(r, 1200));
    }
  } catch (err) {
    logger.error(`Erro no cron: ${err.message}\n${err.stack || ''}`);
  } finally { cronEmExecucao = false; }
}, { timezone: 'America/Sao_Paulo' });

// ========== API ENDPOINTS ==========
app.get('/health', (req, res) => res.json({
  status: 'ok', uptime: Math.floor(process.uptime()),
  tradesAbertos: tradesAbertos.size, cooldowns: cooldownPosTrade.size,
  prontidoes: prontidaoAtiva.size, turso: tursoInitialized,
  servers: getAllServersPublic()
}));

app.get('/api/servers', authMiddleware, (req, res) => {
  res.json({ servers: getAllServersPublic(), defaultServerId: DEFAULT_SERVER_ID });
});

app.get('/api/vapid-public-key', (req, res) => {
  if (!pushConfigured) return res.status(503).json({ error: 'Push não configurado' });
  res.json({ publicKey: VAPID_PUBLIC_KEY });
});

app.post('/api/push/subscribe', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  const { subscription } = req.body;
  if (!subscription || !subscription.endpoint) return res.status(400).json({ error: 'Subscrição inválida' });
  try {
    const docId = Buffer.from(subscription.endpoint).toString('base64').replace(/[/+=]/g, '_').slice(0, 400);
    await db.execute({
      sql: `INSERT INTO push_subscriptions (id, subscription, token_hash, email, updated_at) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET subscription=excluded.subscription, token_hash=excluded.token_hash, email=excluded.email, updated_at=excluded.updated_at`,
      args: [docId, JSON.stringify(subscription), req.user.tokenHash, req.user.email || null, Date.now()]
    });
    invalidarCacheSubs(req.user.tokenHash);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/push/unsubscribe', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  const { endpoint } = req.body;
  if (!endpoint) return res.status(400).json({ error: 'endpoint obrigatório' });
  try {
    const docId = Buffer.from(endpoint).toString('base64').replace(/[/+=]/g, '_').slice(0, 400);
    await db.execute({ sql: 'DELETE FROM push_subscriptions WHERE id = ?', args: [docId] });
    invalidarCacheSubs(req.user.tokenHash);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/push/test', authMiddleware, async (req, res) => {
  if (!pushConfigured) return res.status(503).json({ success: false, error: 'Push não configurado' });
  try {
    await sendPushToWatchers([req.user.tokenHash], { title: '🔔 Teste', body: 'Notificações a funcionar!', tag: 'teste_' + Date.now(), data: { url: '/' } });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/user-preferences', authMiddleware, async (req, res) => {
  try {
    const preferences = await getUserPreferences(req.user.tokenHash);
    res.json({ success: true, preferences, defaults: DEFAULT_PREFS });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/user-preferences', authMiddleware, async (req, res) => {
  try {
    const { preferences } = req.body || {};
    if (!preferences || typeof preferences !== 'object') return res.status(400).json({ error: 'preferences obrigatório' });
    const saved = await saveUserPreferences(req.user.tokenHash, preferences);
    res.json({ success: true, preferences: saved });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/user-me', authMiddleware, (req, res) => {
  res.json({
    tokenHash: req.user.tokenHash, email: req.user.email, name: req.user.name,
    periodDays: req.user.periodDays, plano: req.user.plano,
    maxAtivosPorModo: req.user.plano?.maxAtivosPorModo ?? 10
  });
});

app.get('/api/engine-config', authMiddleware, async (req, res) => {
  const sid = resolveServerId(req.query.server);
  if (!tursoInitialized) return res.json({ active: false, serverId: sid, watchlist: { SNIPER: [], 'CAÇADOR': [], PESCADOR: [], BALEEIRO: [] } });
  try {
    const wl = await getUserWatchlist(req.user.tokenHash, sid);
    res.json({
      active: wl.engineActive, serverId: sid, plano: req.user.plano,
      maxAtivosPorModo: req.user.plano?.maxAtivosPorModo ?? 10,
      watchlist: { SNIPER: wl.SNIPER, 'CAÇADOR': wl['CAÇADOR'], PESCADOR: wl.PESCADOR, BALEEIRO: wl.BALEEIRO }
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/engine-start', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  try {
    const sid = resolveServerId(req.query.server);
    const wl = await getUserWatchlist(req.user.tokenHash, sid);
    const totalAtivos = contarAtivosWatchlist(wl);
    if (totalAtivos === 0) return res.status(400).json({ success: false, error: 'Adiciona pelo menos 1 ativo', code: 'WATCHLIST_EMPTY', totalAtivos: 0 });
    await saveUserWatchlist(req.user.tokenHash, { engineActive: true, email: req.user.email || null }, sid);
    invalidarCacheWatchlists();
    res.json({ success: true, active: true, serverId: sid, totalAtivos });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/engine-stop', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  try {
    const sid = resolveServerId(req.query.server);
    await saveUserWatchlist(req.user.tokenHash, { engineActive: false }, sid);
    invalidarCacheWatchlists();
    res.json({ success: true, active: false, serverId: sid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/engine-watchlist', authMiddleware, async (req, res) => {
  const sid = resolveServerId(req.query.server);
  if (!tursoInitialized) return res.json({ serverId: sid, watchlist: { SNIPER: [], 'CAÇADOR': [], PESCADOR: [], BALEEIRO: [] } });
  try {
    const wl = await getUserWatchlist(req.user.tokenHash, sid);
    res.json({ serverId: sid, watchlist: { SNIPER: wl.SNIPER, 'CAÇADOR': wl['CAÇADOR'], PESCADOR: wl.PESCADOR, BALEEIRO: wl.BALEEIRO } });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/engine-watchlist', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  const { watchlist } = req.body || {};
  if (!watchlist || typeof watchlist !== 'object') return res.status(400).json({ error: 'watchlist deve ser objeto' });
  try {
    const sid = resolveServerId(req.query.server);
    const maxAtivos = req.user.plano?.maxAtivosPorModo ?? 10;
    if (maxAtivos <= 0) return res.status(403).json({ error: 'Sem plano ativo.' });
    const current = await getUserWatchlist(req.user.tokenHash, sid);
    const cortado = {
      SNIPER: Array.isArray(watchlist.SNIPER) && [...new Set(watchlist.SNIPER)].length > maxAtivos,
      'CAÇADOR': Array.isArray(watchlist['CAÇADOR']) && [...new Set(watchlist['CAÇADOR'])].length > maxAtivos,
      PESCADOR: Array.isArray(watchlist.PESCADOR) && [...new Set(watchlist.PESCADOR)].length > maxAtivos,
      BALEEIRO: Array.isArray(watchlist.BALEEIRO) && [...new Set(watchlist.BALEEIRO)].length > maxAtivos
    };
    const final = {
      SNIPER: Array.isArray(watchlist.SNIPER) ? [...new Set(watchlist.SNIPER)].slice(0, maxAtivos) : current.SNIPER,
      'CAÇADOR': Array.isArray(watchlist['CAÇADOR']) ? [...new Set(watchlist['CAÇADOR'])].slice(0, maxAtivos) : current['CAÇADOR'],
      PESCADOR: Array.isArray(watchlist.PESCADOR) ? [...new Set(watchlist.PESCADOR)].slice(0, maxAtivos) : current.PESCADOR,
      BALEEIRO: Array.isArray(watchlist.BALEEIRO) ? [...new Set(watchlist.BALEEIRO)].slice(0, maxAtivos) : current.BALEEIRO,
      email: req.user.email || null
    };
    await saveUserWatchlist(req.user.tokenHash, final, sid);
    invalidarCacheWatchlists();
    const houveCorte = Object.values(cortado).some(Boolean);
    res.json({ success: true, watchlist: final, serverId: sid, plano: req.user.plano, maxAtivosPorModo: maxAtivos, cortado: houveCorte ? cortado : null, mensagem: houveCorte ? `Plano ${req.user.plano?.nome} permite ${maxAtivos} por modo.` : null });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/engine-watchlist', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  try {
    const sid = resolveServerId(req.query.server);
    await saveUserWatchlist(req.user.tokenHash, { SNIPER: [], 'CAÇADOR': [], PESCADOR: [], BALEEIRO: [] }, sid);
    invalidarCacheWatchlists();
    res.json({ success: true, serverId: sid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ⭐═══════════════════════════════════════════════════════════════════
// SCAN-GROUP — mantém o comportamento atual, ordenando por zona e score
// ⭐═══════════════════════════════════════════════════════════════════
app.post('/api/scan-group', authMiddleware, async (req, res) => {
  const { group, mode } = req.body;
  if (!group || !mode) return res.status(400).json({ error: 'group e mode obrigatórios.' });
  const symbols = assetGroups[group];
  if (!symbols) return res.status(400).json({ error: `Grupo "${group}" não reconhecido.` });
  if (!MODOS_OK.includes(mode)) return res.status(400).json({ error: `Modo "${mode}" inválido.` });
  const sid = resolveServerId(req.query.server);
  try {
    const allResults = [];
    for (let i = 0; i < symbols.length; i += 5) {
      const batch = symbols.slice(i, i + 5);
      const batchResults = await Promise.allSettled(batch.map(async (symbol) => {
        const data = await buscarSinalAnalise(symbol, mode, sid);
        if (!data || !data.success) return { symbol, name: getFriendlyName(symbol), signal: 'HOLD', zona: '?', score: 0, reasons: ['Erro'], error: true };
        const consolidated = data.consolidated || {};
        // Score usa readiness_score (proximidade real) ou score (efetivo de entrada)
        const score = (consolidated.readiness_score ?? consolidated.score ?? 0) || 0;
        const zona = consolidated.zona || '?';
        const signal = consolidated.signal || 'HOLD';

        const ZONA_B_MIN = { 'SNIPER': 45, 'CAÇADOR': 50, 'PESCADOR': 55, 'BALEEIRO': 60 };
        const zonaBMin = ZONA_B_MIN[mode] || 50;
        const zonaAMin = zonaBMin + 10;
        const distZonaB = Math.max(0, zonaBMin - score);
        const distZonaA = Math.max(0, zonaAMin - score);

        let mensagemProntidao = null;
        if (signal !== 'HOLD' && zona === 'A') mensagemProntidao = `🚨 Sinal CONFIRMADO ${signal} — score ${score}`;
        else if (signal !== 'HOLD' && zona === 'B') mensagemProntidao = `⚠️ Sinal MODERADO ${signal} — score ${score}`;
        else if (zona === 'B') {
          const dirPrep = extrairDirecaoPrep(data);
          mensagemProntidao = `👀 Em Zona B (${dirPrep || 'aguarda direção'}) — score ${score}`;
        } else if (score >= zonaBMin - 8 && score < zonaBMin) mensagemProntidao = `🟡 Quase em Zona B — ${score}/${zonaBMin} (faltam ${distZonaB})`;
        else if (score >= zonaBMin - 20) mensagemProntidao = `🔵 Em formação — ${score} (faltam ${distZonaB} p/ Zona B)`;
        else mensagemProntidao = `⏪ Aguarda — ${distZonaB} pts p/ Zona B`;

        const estic = avaliarEsticamento(consolidated.score_reasons, consolidated.diagnostico_mercado);
        if (estic.esticado && estic.nivel === 'ALTO') mensagemProntidao = `🔥 ESTICADO — ${estic.motivo}`;

        const proximidade = diagnosticoProximidade(consolidated.score_reasons, consolidated.diagnostico_mercado);
        const _diagScan = consolidated.diagnostico_mercado;
        if (_diagScan?.acao_recomendada) {
          const a = _diagScan.acao_recomendada;
          if (a === 'NAO_ENTRAR_CALL_AGUARDAR_PULLBACK') mensagemProntidao = `⛰️ TOPO LOCAL CALL — aguarda pullback`;
          else if (a === 'NAO_ENTRAR_PUT_AGUARDAR_PULLBACK') mensagemProntidao = `🕳️ FUNDO LOCAL PUT — aguarda pullback`;
          else if (a === 'AGUARDAR_ALINHAMENTO') mensagemProntidao = `⏸️ TENDÊNCIA FRACA — aguarda alinhamento`;
          else if (a === 'AGUARDAR_CONFIRMACAO_REVERSAO') mensagemProntidao = `🔄 REVERSÃO — aguarda confirmação`;
          else if (a === 'AGUARDAR_FIM_PULLBACK') mensagemProntidao = `📉 PULLBACK EM CURSO`;
          else if (a === 'ENTRADA_VALIDA') mensagemProntidao = `✅ ENTRADA VÁLIDA — ${_diagScan.direcao || signal} confirmado`;
        }

        return {
          symbol, name: getFriendlyName(symbol), signal, zona, score,
          readinessScore: consolidated.readiness_score ?? null,
          matureAprovado: consolidated.mature_aprovado ?? false,
          matureMotivoBloqueio: consolidated.mature_motivo_bloqueio ?? null,
          origem: consolidated._origemSinal || 'motor',
          proximidade, esticamento: estic,
          diagnosticoMercado: consolidated.diagnostico_mercado || null,
          mensagemProntidao,
          distanciaZonaB: distZonaB, distanciaZonaA: distZonaA,
          reasons: (consolidated.score_reasons || []).slice(0, 4)
        };
      }));
      allResults.push(...batchResults);
      if (i + 5 < symbols.length) await new Promise(r => setTimeout(r, 800));
    }
    res.json({ success: true, serverId: sid, results: allResults.filter(r => r.status === 'fulfilled').map(r => r.value) });
  } catch (err) {
    logger.error('Erro /api/scan-group:', err.message);
    res.status(500).json({ error: 'Erro interno ao analisar grupo.' });
  }
});

app.get('/api/signals', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.json({ signals: [] });
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const mode = req.query.mode || null;
    const serverFilter = req.query.server || null;
    let sql = 'SELECT * FROM signals WHERE 1=1';
    const args = [];
    if (mode) { sql += ' AND mode = ?'; args.push(mode); }
    if (serverFilter && isValidServerId(serverFilter)) { sql += ' AND server_id = ?'; args.push(serverFilter); }
    sql += ' ORDER BY criado_em DESC LIMIT ?';
    args.push(limit);
    const result = await db.execute({ sql, args });
    const tokenHash = req.user.tokenHash;
    const signals = result.rows.filter(row => { try { const w = JSON.parse(row.watchers || '[]'); return Array.isArray(w) && w.includes(tokenHash); } catch { return false; } })
      .map(row => ({
        id: row.id, symbol: row.symbol, mode: row.mode, tipo: row.tipo,
        titulo: row.titulo, corpo: row.corpo,
        detalhes: row.detalhes ? JSON.parse(row.detalhes) : null,
        watchers: JSON.parse(row.watchers || '[]'),
        score: row.score, confidence: row.confidence, zona: row.zona,
        entry: row.entry, takeProfit: row.take_profit, stopLoss: row.stop_loss,
        nivelProntidao: row.nivel_prontidao, origem: row.origem,
        serverId: row.server_id || DEFAULT_SERVER_ID,
        criadoEm: new Date(row.criado_em).toISOString()
      }));
    res.json({ signals });
  } catch (err) { logger.error(`Erro /api/signals: ${err.message}`); res.status(500).json({ error: err.message }); }
});

app.delete('/api/signals', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  const mode = req.query.mode || null;
  const serverFilter = req.query.server || null;
  try {
    let sql = 'SELECT * FROM signals WHERE 1=1';
    const args = [];
    if (mode) { sql += ' AND mode = ?'; args.push(mode); }
    if (serverFilter && isValidServerId(serverFilter)) { sql += ' AND server_id = ?'; args.push(serverFilter); }
    const result = await db.execute({ sql, args });
    const tokenHash = req.user.tokenHash;
    let deletados = 0, atualizados = 0;
    for (const row of result.rows) {
      let watchers = []; try { watchers = JSON.parse(row.watchers || '[]'); } catch { watchers = []; }
      if (!Array.isArray(watchers) || !watchers.includes(tokenHash)) continue;
      const restantes = watchers.filter(t => t !== tokenHash);
      if (restantes.length === 0) { await db.execute({ sql: 'DELETE FROM signals WHERE id = ?', args: [row.id] }); deletados++; }
      else { await db.execute({ sql: 'UPDATE signals SET watchers = ? WHERE id = ?', args: [JSON.stringify(restantes), row.id] }); atualizados++; }
    }
    res.json({ success: true, deletados, atualizados, total: deletados + atualizados });
  } catch (err) { logger.error('Erro DELETE /api/signals:', err.message); res.status(500).json({ error: err.message }); }
});

app.post('/api/analysis-history', authMiddleware, async (req, res) => res.json({ success: true, skipped: true }));
app.get('/api/analysis-history', authMiddleware, async (req, res) => res.json({ history: [] }));

app.get('/api/stats', authMiddleware, async (req, res) => {
  const sid = resolveServerId(req.query.server);
  const wl = tursoInitialized ? await getUserWatchlist(req.user.tokenHash, sid).catch(() => null) : null;
  let openTradesServer = 0;
  for (const key of tradesAbertos.keys()) if (key.startsWith(`${sid}_`)) openTradesServer++;
  const stats = {
    serverId: sid, engineActive: !!wl?.engineActive,
    watchlistCount: wl ? [wl.SNIPER, wl['CAÇADOR'], wl.PESCADOR, wl.BALEEIRO].reduce((a, arr) => a + arr.length, 0) : 0,
    openTrades: openTradesServer, uptime: process.uptime(),
    turso: tursoInitialized, pushConfigured
  };
  if (tursoInitialized) {
    try {
      const tokenHash = req.user.tokenHash;
      const all = await db.execute({ sql: 'SELECT watchers, criado_em FROM signals WHERE server_id = ?', args: [sid] });
      const tokenSignals = all.rows.filter(r => { try { return JSON.parse(r.watchers || '[]').includes(tokenHash); } catch { return false; } });
      stats.totalSignals = tokenSignals.length;
      const hojeInicio = new Date(new Date().setHours(0,0,0,0)).getTime();
      stats.signalsToday = tokenSignals.filter(r => r.criado_em >= hojeInicio).length;
    } catch (err) { logger.error(`Erro stats: ${err.message}`); }
  }
  res.json(stats);
});

// ========== SERVE FRONTEND ==========
app.get(/^\/(?!.*\.(png|jpg|jpeg|gif|svg|ico|json|js|css|woff|woff2|ttf|webp)).*$/, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', async () => {
  logger.info(`🚀 Servidor na porta ${PORT}`);
  try { await tursoInitPromise; } catch (e) { logger.error('Falha Turso:', e.message); }
  logger.info(`Turso: ${tursoInitialized ? 'Conectado' : 'Não'}`);
  logger.info(`Push: ${pushConfigured ? 'Configurado' : 'Não configurado'}`);
  logger.info(`🌐 Servidores de análise:`);
  for (const id of VALID_SERVER_IDS) logger.info(`   · ${id} (${ANALYSIS_SERVERS[id].name}) → ${ANALYSIS_SERVERS[id].url}`);
  logger.info(`v2.40: RELAY-STRICT-FINAL (4 correções aplicadas)`);
  try { await loadStateFromTurso(); } catch (e) { logger.error('loadStateFromTurso falhou:', e.message); }
});

const SELF_URL = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
setInterval(async () => { try { await fetch(`${SELF_URL}/health`); } catch (err) { logger.error('Self-ping:', err.message); } }, 4 * 60 * 1000);

process.on('SIGTERM', () => { logger.info('SIGTERM - encerrando...'); process.exit(0); });
