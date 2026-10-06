// ===================== server.js (Painel de Sinais) — TURSO EDITION =====================
// Migração Firestore → Turso (libSQL). Todas as colecções foram convertidas em tabelas SQLite.
// v2.21 — Turso (libSQL) substitui Firestore. Cache em memória mantida.

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
    name: reason?.name,
    code: reason?.code,
    stack: reason?.stack
  });
  console.error('RAW REJECTION:', reason);
});

process.on('uncaughtException', (err) => {
  logger.error('Uncaught Exception:', {
    message: err?.message || String(err),
    name: err?.name,
    code: err?.code,
    stack: err?.stack
  });
  console.error('RAW ERROR:', err);
  console.error('RAW STACK:', err?.stack);
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json({ limit: '5mb' }));

// Rotas PWA explícitas ANTES do static
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

// ========== TURSO (libSQL) ==========
let db = null;
let tursoInitialized = false;

// ⭐ Cache em memória — mantido das optimizações anteriores
let _watchlistsCache = null;
let _watchlistsCacheExpira = 0;
const WATCHLISTS_CACHE_TTL = 5 * 60 * 1000;

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS push_subscriptions (
  id TEXT PRIMARY KEY,
  subscription TEXT NOT NULL,
  token_hash TEXT NOT NULL,
  email TEXT,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_push_sub_token ON push_subscriptions(token_hash);

CREATE TABLE IF NOT EXISTS user_watchlists (
  token_hash TEXT PRIMARY KEY,
  email TEXT,
  engine_active INTEGER NOT NULL DEFAULT 0,
  sniper TEXT NOT NULL DEFAULT '[]',
  cacador TEXT NOT NULL DEFAULT '[]',
  pescador TEXT NOT NULL DEFAULT '[]',
  baleeiro TEXT NOT NULL DEFAULT '[]',
  atualizado_em INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS user_preferences (
  token_hash TEXT PRIMARY KEY,
  sniper_early INTEGER NOT NULL DEFAULT 25,
  sniper_mature INTEGER NOT NULL DEFAULT 40,
  cacador_early INTEGER NOT NULL DEFAULT 28,
  cacador_mature INTEGER NOT NULL DEFAULT 42,
  pescador_early INTEGER NOT NULL DEFAULT 30,
  pescador_mature INTEGER NOT NULL DEFAULT 45,
  baleeiro_early INTEGER NOT NULL DEFAULT 35,
  baleeiro_mature INTEGER NOT NULL DEFAULT 48,
  atualizado_em INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS open_trades (
  trade_key TEXT PRIMARY KEY,
  data TEXT NOT NULL,
  atualizado_em INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cooldowns (
  trade_key TEXT PRIMARY KEY,
  expires_at INTEGER NOT NULL,
  atualizado_em INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS prontidao_state (
  trade_key TEXT PRIMARY KEY,
  historico TEXT NOT NULL DEFAULT '[]',
  ativa INTEGER NOT NULL DEFAULT 0,
  atualizado_em INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  mode TEXT NOT NULL,
  tipo TEXT NOT NULL,
  titulo TEXT NOT NULL,
  corpo TEXT NOT NULL,
  detalhes TEXT,
  watchers TEXT NOT NULL DEFAULT '[]',
  score REAL,
  confidence REAL,
  zona TEXT,
  entry REAL,
  take_profit REAL,
  stop_loss REAL,
  nivel_prontidao TEXT,
  origem TEXT DEFAULT 'motor',
  criado_em INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_signals_mode ON signals(mode);
CREATE INDEX IF NOT EXISTS idx_signals_tipo ON signals(tipo);
CREATE INDEX IF NOT EXISTS idx_signals_criado ON signals(criado_em);
`;

async function initializeTurso() {
  const url = process.env.TURSO_DATABASE_URL;
  const authToken = process.env.TURSO_AUTH_TOKEN;
  if (!url || !authToken) {
    logger.warn('Turso não configurado (TURSO_DATABASE_URL / TURSO_AUTH_TOKEN ausentes).');
    return false;
  }
  try {
    db = createClient({ url, authToken });
    // Aplicar schema — idempotente (IF NOT EXISTS)
    const statements = SCHEMA_SQL.split(';').map(s => s.trim()).filter(s => s.length > 0);
    for (const stmt of statements) {
      await db.execute(stmt);
    }
    // Teste de ligação
    await db.execute('SELECT 1');
    tursoInitialized = true;
    logger.info('✅ Turso inicializado + schema aplicado.');
    return true;
  } catch (err) {
    logger.error('❌ Erro ao inicializar Turso:', err.message);
    return false;
  }
}

// Inicializa Turso no arranque (mas o app.listen espera pela conclusão)
const tursoInitPromise = initializeTurso();

// ========== WEB PUSH (VAPID) ==========
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
    logger.error('❌ Falha ao configurar Web Push:', err.message || String(err));
    logger.error('   Push desativado — servidor continua a arrancar normalmente.');
  }
} else {
  logger.warn('VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY não configurados. Push desativado.');
}

// ⭐ Cache de subscrições em memória
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
    data: {
      subscription: JSON.parse(row.subscription),
      tokenHash: row.token_hash,
      email: row.email
    }
  }));
  _subsCache.set(tokenHash, { subs, expira: agora + SUBS_CACHE_TTL });
  return subs;
}

function invalidarCacheSubs(tokenHash) {
  if (tokenHash) _subsCache.delete(tokenHash);
  else _subsCache.clear();
}

async function sendPushToWatchers(watchers, payload) {
  if (!pushConfigured || !tursoInitialized || !watchers || watchers.length === 0) return;
  try {
    const body = JSON.stringify(payload);
    const deletions = [];

    for (const tokenHash of watchers) {
      let subs;
      try {
        subs = await _getSubsDeToken(tokenHash);
      } catch (err) {
        logger.error(`Erro a obter subs de ${tokenHash.slice(0, 8)}:`, err.message);
        continue;
      }

      for (const sub of subs) {
        if (!sub.data.subscription) continue;
        try {
          await webpush.sendNotification(sub.data.subscription, body);
        } catch (err) {
          if (err.statusCode === 404 || err.statusCode === 410) {
            deletions.push(
              db.execute({ sql: 'DELETE FROM push_subscriptions WHERE id = ?', args: [sub.id] })
            );
            _subsCache.delete(tokenHash);
          } else {
            logger.error('Erro push:', err.message);
          }
        }
      }
    }
    if (deletions.length) await Promise.all(deletions);
  } catch (err) {
    logger.error('sendPushToWatchers erro:', err.message);
  }
}

// ========== MAPEAMENTO DE ATIVOS (nomes amigáveis) ==========
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

function getFriendlyName(symbol) {
  if (!symbol) return '?';
  return fullAssets[symbol] || cleanSymbolName(symbol);
}

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

function getPlano(periodDays) {
  return PLANOS[periodDays] || PLANO_DEFAULT;
}
// ========== MIDDLEWARE DE AUTENTICAÇÃO ==========
const TOKEN_CACHE_TTL_MS = 5 * 60 * 1000;
const tokenValidationCache = new Map();
const ADMIN_SECRET = process.env.ADMIN_SECRET || '';

setInterval(() => {
  const now = Date.now();
  for (const [tok, e] of tokenValidationCache.entries()) {
    if (e.expiresAt <= now) tokenValidationCache.delete(tok);
  }
}, 60 * 1000);

app.post('/api/validate-token', async (req, res) => {
  const { token } = req.body || {};
  if (!token || typeof token !== 'string') {
    return res.status(400).json({ valid: false, message: 'Token não fornecido' });
  }
  const API_URL = process.env.ANALYSIS_API_URL || 'http://localhost:3001';
  try {
    const r = await fetch(`${API_URL}/validate-token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token })
    });
    const data = await r.json().catch(() => ({}));
    logger.info(`[VALIDATE-PROXY] token=${token.slice(0, 8)}... status=${r.status} periodDays=${data.periodDays ?? 'null'}`);
    return res.status(r.status).json(data);
  } catch (err) {
    logger.error('[VALIDATE-PROXY] Erro:', err.message);
    return res.status(503).json({ valid: false, message: 'Serviço de validação indisponível' });
  }
});

async function authMiddleware(req, res, next) {
  const authHeader = req.headers['authorization'];
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Token não fornecido' });
  }
  const token = authHeader.split('Bearer ')[1].trim();
  if (!token || token.length > 5000) {
    return res.status(401).json({ error: 'Token inválido' });
  }

  if (ADMIN_SECRET && token === ADMIN_SECRET) {
    const tokenHash = crypto.createHash('sha256').update(token).digest('hex').slice(0, 16);
    req.user = {
      token, tokenHash,
      email: 'admin@local', name: 'Admin',
      periodDays: 9999,
      plano: PLANOS[9999],
      isAdmin: true
    };
    return next();
  }

  const cached = tokenValidationCache.get(token);
  if (cached && cached.expiresAt > Date.now()) {
    if (!cached.valid) return res.status(401).json({ error: 'Token inválido ou expirado' });
    req.user = cached.user;
    return next();
  }

  const API_URL = process.env.ANALYSIS_API_URL || 'http://localhost:3001';
  try {
    const response = await fetch(`${API_URL}/validate-token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token })
    });
    const data = await response.json().catch(() => ({}));
    const valid = data && data.valid === true;
    logger.info(`[AUTH] ${API_URL}/validate-token → status=${response.status} valid=${valid} periodDays=${data.periodDays ?? 'AUSENTE'}`);

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

// ========== ESTADO GLOBAL ==========
let cronEmExecucao = false;
const MODOS_OK = ['SNIPER', 'CAÇADOR', 'PESCADOR', 'BALEEIRO'];
const CADENCIAS = { SNIPER: 1, 'CAÇADOR': 3, PESCADOR: 10, BALEEIRO: 30 };

const PRONTIDAO_CONFIG = {
  SNIPER:    { cooldownMs: 10 * 60 * 1000, ciclosForaParaArrefecer: 10 },
  'CAÇADOR': { cooldownMs: 10 * 60 * 1000, ciclosForaParaArrefecer: 5  },
  PESCADOR:  { cooldownMs: 30 * 60 * 1000, ciclosForaParaArrefecer: 4  },
  BALEEIRO:  { cooldownMs: 60 * 60 * 1000, ciclosForaParaArrefecer: 3  }
};
function getProntidaoConfig(mode) {
  return PRONTIDAO_CONFIG[mode] || PRONTIDAO_CONFIG['CAÇADOR'];
}

const SCORE_PRONTIDAO_MIN = {
  SNIPER: 25, 'CAÇADOR': 28, PESCADOR: 30, BALEEIRO: 35
};
const SCORE_QUASE_ENTRADA = {
  SNIPER: 40, 'CAÇADOR': 42, PESCADOR: 45, BALEEIRO: 48
};
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

// ========== PREFERÊNCIAS DE UTILIZADOR (Turso) ==========
async function getUserPreferences(tokenHash) {
  if (!tokenHash) return sanitizePrefs({});
  if (!tursoInitialized) return sanitizePrefs({});
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

const TRADE_TIMEOUT_POR_MODO_MS = {
  'SNIPER':   20 * 60 * 1000,
  'CAÇADOR':  90 * 60 * 1000,
  'PESCADOR': 12 * 60 * 60 * 1000,
  'BALEEIRO': 72 * 60 * 60 * 1000
};
const TRADE_TIMEOUT_EXTEND_POR_MODO_MS = {
  'SNIPER':   15 * 60 * 1000,
  'CAÇADOR':  45 * 60 * 1000,
  'PESCADOR': 6 * 60 * 60 * 1000,
  'BALEEIRO': 48 * 60 * 60 * 1000
};
const TRADE_TIMEOUT_MS_DEFAULT = 20 * 60 * 1000;
const TRADE_TIMEOUT_EXTEND_MS_DEFAULT = 15 * 60 * 1000;

function getTimeoutModo(mode) {
  return TRADE_TIMEOUT_POR_MODO_MS[mode] || TRADE_TIMEOUT_MS_DEFAULT;
}
function getTimeoutExtendModo(mode) {
  return TRADE_TIMEOUT_EXTEND_POR_MODO_MS[mode] || TRADE_TIMEOUT_EXTEND_MS_DEFAULT;
}

const PROGRESSO_MINIMO_EXTENSAO = 0.05;
const EXTENSOES_MAX = 3;

// ========== WATCHLIST POR UTILIZADOR (Turso) ==========
async function getUserWatchlist(tokenHash) {
  if (!tursoInitialized) return null;
  try {
    const res = await db.execute({
      sql: 'SELECT email, engine_active, sniper, cacador, pescador, baleeiro FROM user_watchlists WHERE token_hash = ?',
      args: [tokenHash]
    });
    const row = res.rows[0];
    if (!row) {
      return {
        tokenHash, email: null, engineActive: false,
        SNIPER: [], 'CAÇADOR': [], PESCADOR: [], BALEEIRO: []
      };
    }
    return {
      tokenHash,
      email: row.email || null,
      engineActive: !!row.engine_active,
      SNIPER: JSON.parse(row.sniper || '[]'),
      'CAÇADOR': JSON.parse(row.cacador || '[]'),
      PESCADOR: JSON.parse(row.pescador || '[]'),
      BALEEIRO: JSON.parse(row.baleeiro || '[]')
    };
  } catch (err) {
    logger.error('Erro getUserWatchlist:', err.message);
    return null;
  }
}

async function saveUserWatchlist(tokenHash, patch) {
  if (!tursoInitialized) return;
  const current = await getUserWatchlist(tokenHash) || {};
  const merged = {
    email: patch.email !== undefined ? patch.email : (current.email || null),
    engineActive: patch.engineActive !== undefined ? patch.engineActive : (current.engineActive || false),
    SNIPER: patch.SNIPER !== undefined ? patch.SNIPER : (current.SNIPER || []),
    'CAÇADOR': patch['CAÇADOR'] !== undefined ? patch['CAÇADOR'] : (current['CAÇADOR'] || []),
    PESCADOR: patch.PESCADOR !== undefined ? patch.PESCADOR : (current.PESCADOR || []),
    BALEEIRO: patch.BALEEIRO !== undefined ? patch.BALEEIRO : (current.BALEEIRO || [])
  };
  await db.execute({
    sql: `INSERT INTO user_watchlists (
      token_hash, email, engine_active, sniper, cacador, pescador, baleeiro, atualizado_em
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(token_hash) DO UPDATE SET
      email=excluded.email, engine_active=excluded.engine_active,
      sniper=excluded.sniper, cacador=excluded.cacador,
      pescador=excluded.pescador, baleeiro=excluded.baleeiro,
      atualizado_em=excluded.atualizado_em`,
    args: [
      tokenHash,
      merged.email,
      merged.engineActive ? 1 : 0,
      JSON.stringify(merged.SNIPER),
      JSON.stringify(merged['CAÇADOR']),
      JSON.stringify(merged.PESCADOR),
      JSON.stringify(merged.BALEEIRO),
      Date.now()
    ]
  });
}

// ⭐ Cache em memória (5 min TTL)
async function getAllUserWatchlists() {
  if (!tursoInitialized) return [];
  const agora = Date.now();
  if (_watchlistsCache && agora < _watchlistsCacheExpira) return _watchlistsCache;

  try {
    const res = await db.execute('SELECT * FROM user_watchlists');
    const resultado = res.rows.map(row => ({
      tokenHash: row.token_hash,
      email: row.email || null,
      engineActive: !!row.engine_active,
      SNIPER: JSON.parse(row.sniper || '[]'),
      'CAÇADOR': JSON.parse(row.cacador || '[]'),
      PESCADOR: JSON.parse(row.pescador || '[]'),
      BALEEIRO: JSON.parse(row.baleeiro || '[]')
    }));
    _watchlistsCache = resultado;
    _watchlistsCacheExpira = agora + WATCHLISTS_CACHE_TTL;
    return resultado;
  } catch (err) {
    logger.error('Erro getAllUserWatchlists:', err.message);
    return [];
  }
}

function invalidarCacheWatchlists() {
  _watchlistsCache = null;
  _watchlistsCacheExpira = 0;
}

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
const arrefecimentoUltimoEnvio = new Map();
const prontidaoGlobalPorSymbol = new Map();

setInterval(() => {
  const agora = Date.now();
  for (const [key, expira] of cooldownPosTrade.entries()) if (agora > expira) cooldownPosTrade.delete(key);
  for (const [key, historico] of prontidaoHistorico.entries()) {
    const ultima = historico[historico.length - 1];
    if (!ultima || agora - ultima.t > 30 * 60 * 1000) {
      prontidaoHistorico.delete(key);
      prontidaoAtiva.delete(key);
      prontidaoForaContagem.delete(key);
      prontidaoUltimoEnvio.delete(key);
      arrefecimentoUltimoEnvio.delete(key);
    }
  }
  for (const [sym, ts] of prontidaoGlobalPorSymbol.entries()) {
    if (agora - ts > 30 * 60 * 1000) prontidaoGlobalPorSymbol.delete(sym);
  }
}, 60 * 1000);

// ========== PERSISTÊNCIA (Turso) ==========
async function persistTradeOpen(tradeKey, trade) {
  if (!tursoInitialized) return;
  try {
    await db.execute({
      sql: `INSERT INTO open_trades (trade_key, data, atualizado_em) VALUES (?, ?, ?)
            ON CONFLICT(trade_key) DO UPDATE SET data=excluded.data, atualizado_em=excluded.atualizado_em`,
      args: [tradeKey, JSON.stringify(trade), Date.now()]
    });
  } catch (err) { logger.error('Erro persistTradeOpen:', err.message); }
}

async function persistTradeUpdate(tradeKey, trade) {
  if (!tursoInitialized) return;
  try {
    await db.execute({
      sql: `INSERT INTO open_trades (trade_key, data, atualizado_em) VALUES (?, ?, ?)
            ON CONFLICT(trade_key) DO UPDATE SET data=excluded.data, atualizado_em=excluded.atualizado_em`,
      args: [tradeKey, JSON.stringify(trade), Date.now()]
    });
  } catch (err) { logger.error('Erro persistTradeUpdate:', err.message); }
}

async function removePersistedTrade(tradeKey) {
  if (!tursoInitialized) return;
  try { await db.execute({ sql: 'DELETE FROM open_trades WHERE trade_key = ?', args: [tradeKey] }); }
  catch (err) { logger.error('Erro removePersistedTrade:', err.message); }
}

async function persistCooldown(tradeKey, expiresAt) {
  if (!tursoInitialized) return;
  try {
    await db.execute({
      sql: `INSERT INTO cooldowns (trade_key, expires_at, atualizado_em) VALUES (?, ?, ?)
            ON CONFLICT(trade_key) DO UPDATE SET expires_at=excluded.expires_at, atualizado_em=excluded.atualizado_em`,
      args: [tradeKey, expiresAt, Date.now()]
    });
  } catch (err) { logger.error('Erro persistCooldown:', err.message); }
}

async function removePersistedCooldown(tradeKey) {
  if (!tursoInitialized) return;
  try { await db.execute({ sql: 'DELETE FROM cooldowns WHERE trade_key = ?', args: [tradeKey] }); }
  catch (err) { logger.error('Erro removePersistedCooldown:', err.message); }
}

async function persistProntidao(tradeKey, historico, ativa) {
  if (!tursoInitialized) return;
  try {
    await db.execute({
      sql: `INSERT INTO prontidao_state (trade_key, historico, ativa, atualizado_em) VALUES (?, ?, ?, ?)
            ON CONFLICT(trade_key) DO UPDATE SET historico=excluded.historico, ativa=excluded.ativa, atualizado_em=excluded.atualizado_em`,
      args: [tradeKey, JSON.stringify(historico || []), ativa ? 1 : 0, Date.now()]
    });
  } catch (err) { logger.error('Erro persistProntidao:', err.message); }
}

async function removePersistedProntidao(tradeKey) {
  if (!tursoInitialized) return;
  try { await db.execute({ sql: 'DELETE FROM prontidao_state WHERE trade_key = ?', args: [tradeKey] }); }
  catch (err) { logger.error('Erro removePersistedProntidao:', err.message); }
}

// ⭐ Arranque resiliente — adaptado para Turso
async function loadStateFromTurso() {
  if (!tursoInitialized) {
    logger.warn('⏭️ loadStateFromTurso: Turso indisponível, a saltar.');
    return;
  }
  try {
    // Anti-duplicado (últimos 5min)
    try {
      const cincoMinAtras = Date.now() - 5 * 60 * 1000;
      const res = await db.execute({
        sql: "SELECT symbol, mode, criado_em FROM signals WHERE tipo = 'SINAL_CONFIRMADO' AND criado_em >= ?",
        args: [cincoMinAtras]
      });
      res.rows.forEach(row => {
        if (row.symbol && row.mode) {
          ultimoSinalPorPar.set(`${row.symbol}_${row.mode}`, row.criado_em);
        }
      });
      logger.info(`♻️ Anti-duplicado restaurado: ${res.rows.length} sinal(is) recente(s)`);
    } catch (e) {
      logger.warn(`⚠️ Falha ao restaurar anti-duplicado: ${e.message}`);
    }

    // Trades abertos
    const tradesRes = await db.execute('SELECT trade_key, data FROM open_trades');
    const agora = Date.now();
    let tradesRestaurados = 0;
    for (const row of tradesRes.rows) {
      const t = JSON.parse(row.data);
      const timestampTrade = t.timestamp || 0;
      const timeoutModo = getTimeoutModo(t.mode);
      const timeoutAt = t.timeoutAt || (timestampTrade + timeoutModo);
      if (agora > timeoutAt) t.timeoutAt = agora + 60 * 1000;
      if (!t.timeoutAt) t.timeoutAt = timestampTrade + timeoutModo;
      tradesAbertos.set(row.trade_key, t);
      tradesRestaurados++;
    }

    // Cooldowns
    const cdRes = await db.execute('SELECT trade_key, expires_at FROM cooldowns');
    let cdRestaurados = 0, cdExpirados = 0;
    for (const row of cdRes.rows) {
      if (row.expires_at && row.expires_at > agora) {
        cooldownPosTrade.set(row.trade_key, row.expires_at);
        cdRestaurados++;
      } else {
        await db.execute({ sql: 'DELETE FROM cooldowns WHERE trade_key = ?', args: [row.trade_key] });
        cdExpirados++;
      }
    }

    // Prontidões
    const prRes = await db.execute('SELECT trade_key, historico, ativa FROM prontidao_state');
    let prRestaurados = 0;
    for (const row of prRes.rows) {
      const hist = JSON.parse(row.historico || '[]');
      if (Array.isArray(hist) && hist.length > 0) prontidaoHistorico.set(row.trade_key, hist);
      if (row.ativa) prontidaoAtiva.add(row.trade_key);
      prRestaurados++;
    }

    logger.info(`♻️ Estado restaurado: ${tradesRestaurados} trade(s), ${cdRestaurados} cooldown(s), ${prRestaurados} prontidão(ões) · ${cdExpirados} cooldown(s) expirado(s)`);
  } catch (err) {
    logger.error(`❌ Erro ao carregar estado do Turso: ${err.message}`);
  }
}

// ========== HELPERS ==========
function extrairDirecaoPrep(dados) {
  const nota = dados.consolidated.primaryTrendNote || '';
  const reasonsTexto = (dados.consolidated.score_reasons || []).join(' ');
  if (/RESPIRAÇÃO\s+(SIMPLES|DUPLA)|mercado precisa respirar/i.test(reasonsTexto)) return null;
  const notaindicaIncerteza = /NÃO confirmada|não confirmada|SEM DIREÇÃO DEFINIDA|sem direção definida|FRÁGIL|aguarda alinhamento/i.test(nota);
  if (!notaindicaIncerteza) {
    const matchNota = nota.match(/Tendência primária \([^)]+\):\s*(ALTA|BAIXA)/i);
    if (matchNota) return matchNota[1].toUpperCase() === 'ALTA' ? 'CALL' : 'PUT';
  }
  const razaoTrend = (dados.consolidated.score_reasons || []).find(r => r.includes('🧭'));
  if (razaoTrend && !/NEUTRAL|neutro|não confirmada|SEM DIREÇÃO/i.test(razaoTrend)) {
    if (/Tendência de fundo:\s*ALTA|Tendência assumida:\s*(UP|ALTA)|reversão para UP/i.test(razaoTrend)) return 'CALL';
    if (/Tendência de fundo:\s*BAIXA|Tendência assumida:\s*(DOWN|BAIXA)|reversão para DOWN/i.test(razaoTrend)) return 'PUT';
  }
  const sinais = [];
  for (const tf of ['m1_timing', 'm5_timing', 'm15_timing', 'h1_timing', 'h4_timing']) {
    const s = dados.consolidated[tf]?.sinal;
    if (s === 'PUT' || s === 'CALL') sinais.push(s);
  }
  if (sinais.length > 0) {
    const puts = sinais.filter(s => s === 'PUT').length;
    const calls = sinais.length - puts;
    const ratio = Math.max(puts, calls) / sinais.length;
    if (ratio >= 0.6) return puts > calls ? 'PUT' : 'CALL';
  }
  return null;
}

function diagnosticoProximidade(reasons) {
  const texto = (reasons || []).join(' ');

  // ═══════════════════════════════════════════════════════════════════════
  // ⭐ NOVOS GATES (Issues 2 e 3 + Zone B sanity + DeMarker novo formato)
  // ═══════════════════════════════════════════════════════════════════════

  // Zone B sanity POSITIVA — entrada moderada foi validada
  if (/✅ Zona B validada|mantém\s+(CALL|PUT)\s+em zona B/i.test(texto)) {
    return { nivel: 'PERTO', detalhe: 'zona B validada — entrada moderada aprovada' };
  }
  // Zone B sanity NEGATIVA — foi rebaixada para HOLD
  if (/⛔ Entrada\s+(CALL|PUT)\s+em zona B rebaixada/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'zona B rebaixada — macro contra direção' };
  }
  // Bug B mode-aware (Issue 2)
  if (/hist a desacelerar \d+%.*limite \d+%.*para (SNIPER|CAÇADOR|PESCADOR|BALEEIRO)/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'macro a desacelerar — aguarda estabilizar' };
  }
  // REVERSAO_ACCEL mode-aware (Issue 3)
  if (/opõe-se com hist a ACELERAR|Reversão ativa detectada em/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'reversão ativa — aguarda alinhar' };
  }
  // DeMarker bloqueio com trigger/macro explícito (novo formato)
  if (/CALL BLOQUEADO.*DeMarker|PUT BLOQUEADO.*DeMarker/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'DeMarker em extremo — aguarda normalizar' };
  }

  // ═══════════════════════════════════════════════════════════════════════
  // CASOS ANTIGOS (mantidos na ordem original)
  // ═══════════════════════════════════════════════════════════════════════

  if (/RESPIRAÇÃO\s+(SIMPLES|DUPLA)|mercado precisa respirar/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'mercado precisa respirar — aguarda normalizar' };
  }
  if (/Tendência NEUTRAL.*reversão não confirmada|Reversão NÃO confirmada/i.test(texto)) {
    return { nivel: 'LONGE', detalhe: 'motor em NEUTRAL — reversão ainda por confirmar' };
  }
  if (/SEM DIREÇÃO DEFINIDA|Tendência indefinida|SEM DIREÇÃO/i.test(texto)) {
    return { nivel: 'LONGE', detalhe: 'mercado sem direção clara' };
  }
  if (/SINAL ANULADO.*DeMarker extremo|DEMARKER EXTREMO/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'mercado em extremo — aguarda normalizar' };
  }
  // FIX #81
  if (/SINAL ANULADO:\s*\S+\s+DeM\s+[\d.]+\s+em\s+(fundo|topo)\s+extremo/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'exaustão nos TFs-chave — aguarda respirar' };
  }
  if (/Prontidão reduzida por exaustão DeMarker/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'exaustão nos TFs-chave — aguarda respirar' };
  }
  if (/micro timing bloqueou|Micro timing.*BLOQUEOU|DeM.*contra (CALL|PUT)|sobrecompra micro|sobrevenda micro/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'micro timing contra — aguarda alinhar' };
  }
  if (/\(micro timing\) está contra a tendência/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'micro timing contra a tendência' };
  }
  if (/\[FIX #40b\]|hist a desacelerar|trigger em conflito NÃO aceite/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'momentum a desacelerar — aguarda estabilizar' };
  }
  if (/Pullback.*BLOQUEADO por estrutura esticada|estrutura no extremo.*aguarda correção/i.test(texto)) {
    return { nivel: 'BLOQUEADO', detalhe: 'estrutura esticada — aguarda correção' };
  }
  if (/Zona A rebaixada para B|Zona final rebaixada para B/i.test(texto)) {
    return { nivel: 'PERTO', detalhe: 'confiança baixa — aguarda reforço' };
  }
  if (/ADX muito fraco/i.test(texto)) {
    return { nivel: 'LONGE', detalhe: 'macro sem força' };
  }
  if (/CONFLITO|pullback em curso|aguarda histograma|aguarda alinhamento|aguarda flip/i.test(texto)) {
    return { nivel: 'PERTO', detalhe: 'gatilho em ajuste' };
  }
  return { nivel: 'FORMACAO', detalhe: 'aguardando alinhamento' };
}

function avaliarEsticamento(reasons) {
  const texto = (reasons || []).join(' ');
  const alertas = [];

  // ═══════════════════════════════════════════════════════════════════════
  // CASOS CRÍTICOS (retornam imediatamente)
  // ═══════════════════════════════════════════════════════════════════════

  if (/RESPIRAÇÃO\s+(SIMPLES|DUPLA)|mercado precisa respirar/i.test(texto)) {
    const match = texto.match(/RESPIRAÇÃO\s+(SIMPLES|DUPLA)/i);
    const tipo = match ? match[1] : 'SIMPLES';
    return { esticado: true, nivel: 'ALTO', motivo: `Respiração ${tipo} — mercado em extremo` };
  }
  if (/SINAL ANULADO: DeMarker extremo|DEMARKER EXTREMO —/i.test(texto)) {
    return { esticado: true, nivel: 'ALTO', motivo: 'DeMarker extremo confirmado' };
  }
  // FIX #81 (exaustão dos TFs-chave)
  const m81 = texto.match(/SINAL ANULADO:\s*(\S+)\s+DeM\s+([\d.]+)\s+em\s+(fundo|topo)\s+extremo/i);
  if (m81) {
    return { esticado: true, nivel: 'ALTO', motivo: `Exaustão ${m81[1]} DeM ${m81[2]} (${m81[3]})` };
  }
  // Fator de prontidão (variações de "mercado precisa respirar")
  if (/Prontidão reduzida por exaustão DeMarker/i.test(texto)) {
    return { esticado: true, nivel: 'ALTO', motivo: 'Exaustão confirmada nos TFs-chave' };
  }

  // ═══════════════════════════════════════════════════════════════════════
  // ⭐ NOVOS GATES (Issues 2/3 + Zone B sanity)
  // ═══════════════════════════════════════════════════════════════════════

  // Bug B mode-aware → macro esticada, aguarda
  if (/hist a desacelerar \d+%.*limite \d+%.*para (SNIPER|CAÇADOR|PESCADOR|BALEEIRO)/i.test(texto)) {
    return { esticado: true, nivel: 'ALTO', motivo: 'Macro a desacelerar — momentum fraco' };
  }
  // REVERSAO_ACCEL mode-aware → reversão em curso
  if (/opõe-se com hist a ACELERAR|Reversão ativa detectada em/i.test(texto)) {
    return { esticado: true, nivel: 'ALTO', motivo: 'Reversão ativa — TFs a virar contra' };
  }
  // Zone B rebaixada → entrada instável
  if (/⛔ Entrada\s+(CALL|PUT)\s+em zona B rebaixada/i.test(texto)) {
    return { esticado: true, nivel: 'MÉDIO', motivo: 'Zona B rebaixada — macro contra' };
  }

  // ═══════════════════════════════════════════════════════════════════════
  // CASOS COM ACUMULAÇÃO (≥ 2 = ALTO, 1 = MÉDIO)
  // ═══════════════════════════════════════════════════════════════════════

  if (/(DeMarker|DeM)\s+0\.[7-9]\d/i.test(texto) || /(DeMarker|DeM).*sobrecompra/i.test(texto)) alertas.push('DeM sobrecompra');
  if (/(DeMarker|DeM)\s+0\.[0-2]\d/i.test(texto) || /(DeMarker|DeM).*sobrevenda/i.test(texto)) alertas.push('DeM sobrevenda');
  if (/RSI\s+(7[5-9]|8\d|9\d)\b/i.test(texto)) alertas.push('RSI extremo alto');
  if (/RSI\s+([0-9]|1\d|2[0-5])\b/i.test(texto)) alertas.push('RSI extremo baixo');
  if (/RSI.*zona alta/i.test(texto)) alertas.push('RSI zona alta');
  if (/RSI.*zona baixa/i.test(texto)) alertas.push('RSI zona baixa');
  if (/(DeMarker|DeM)\s+0\.6[5-9]/i.test(texto)) alertas.push('DeM a esticar');
  if (/(DeMarker|DeM)\s+0\.3[0-5]/i.test(texto)) alertas.push('DeM a esticar (baixo)');

  if (alertas.length >= 2) return { esticado: true, nivel: 'ALTO', motivo: alertas.slice(0, 3).join(' · ') };
  if (alertas.length === 1) return { esticado: true, nivel: 'MÉDIO', motivo: alertas[0] };
  return { esticado: false, nivel: 'BAIXO', motivo: 'mercado saudável' };
}

function detectarRespiracaoOuPullback(dados, trade, percentualPercorrido = 0) {
  const reasons = (dados?.consolidated?.score_reasons || []).join(' ');
  const nota = dados?.consolidated?.primaryTrendNote || '';
  const texto = `${reasons} ${nota}`;
  const indicadoPeloMotor = /RESPIRAÇÃO\\s+(SIMPLES|DUPLA)|mercado precisa respirar|pullback em curso|pullback saudável|correção saudável|reteste|aguarda alinhamento|aguarda flip/i.test(texto);
  if (!indicadoPeloMotor || !trade) return false;

  const distanciaEntrada = Math.abs((dados?.consolidated?.price ?? trade.currentPrice) - trade.entry);
  const alvoTotal = Math.abs(trade.takeProfit - trade.entry);
  const pertoDaEntrada = alvoTotal > 0 && distanciaEntrada <= alvoTotal * 0.18;
  const progressoValido = percentualPercorrido >= -0.12 && percentualPercorrido < 0.35;
  return pertoDaEntrada && progressoValido;
}

function calcularCooldownDinamico(tipoFecho, dados, esticPreCalculado) {
  const COOLDOWN_DEFAULT = COOLDOWN_POS_TRADE_MS;
  const zona = dados?.consolidated?.zona || 'C';
  const score = dados?.consolidated?.score || 0;
  const reasons = (dados?.consolidated?.score_reasons || []).join(' ');
  const estic = esticPreCalculado || avaliarEsticamento(dados?.consolidated?.score_reasons);
  if (estic.esticado && estic.nivel === 'ALTO') return 15 * 60 * 1000;
  if (tipoFecho === 'WIN' && zona === 'A' && score >= 60 && !estic.esticado) return 3 * 60 * 1000;
  if (tipoFecho === 'STOP' && zona === 'A' && score >= 55 && !estic.esticado) return 5 * 60 * 1000;
  if (tipoFecho === 'TIMEOUT' && /pullback/i.test(reasons)) return 4 * 60 * 1000;
  return COOLDOWN_DEFAULT;
}

function confirmaExaustaoMultiTF(dados, trade) {
  const tfs = dados?.timeframes || {};
  const mode = trade?.mode;
  const CONFIRM_TF = {
    'SNIPER':   { trigger: 'M1',  confirm: 'M5'  },
    'CAÇADOR':  { trigger: 'M5',  confirm: 'M15' },
    'PESCADOR': { trigger: 'H1',  confirm: 'H4'  },
    'BALEEIRO': { trigger: 'H4',  confirm: 'H24' }
  };
  const cfg = CONFIRM_TF[mode];
  if (!cfg) return { confirmado: false, motivo: 'modo desconhecido' };
  const confTF = tfs[cfg.confirm];
  if (!confTF) return { confirmado: false, motivo: `${cfg.confirm} sem dados` };
  const confRSI = confTF.rsi ?? 50;
  const confStatus = confTF.macd_phase?.status || {};
  const confHist = confTF.macd_phase?.histogram ?? null;
  const confPrevHist = confTF.macd_phase?.prev_histogram ?? null;
  const isCall = trade.signal === 'CALL';
  const macdInvertido = isCall ? confStatus.histograma === '❌ NEGATIVO' : confStatus.histograma === '✅ POSITIVO';
  if (macdInvertido) return { confirmado: true, motivo: `${cfg.confirm} MACD virou contra` };
  const rsiExtremo = isCall ? confRSI >= 75 : confRSI <= 25;
  if (rsiExtremo) return { confirmado: true, motivo: `${cfg.confirm} RSI ${confRSI.toFixed(0)} extremo` };
  const histEncolhendo = (confHist != null && confPrevHist != null && confPrevHist !== 0)
    ? Math.abs(confHist) < Math.abs(confPrevHist) * 0.60 : false;
  if (histEncolhendo && (isCall ? confRSI >= 68 : confRSI <= 32)) {
    return { confirmado: true, motivo: `${cfg.confirm} hist a encolher + RSI ${confRSI.toFixed(0)}` };
  }
  return { confirmado: false, motivo: `${cfg.confirm} ainda suporta (RSI ${confRSI.toFixed(0)})` };
}
// ========== FORMATAÇÃO DE MENSAGENS ==========

function formatarMensagemPrep(symbol, direcao, dados, extras = {}) {
  const nomeAmigavel = getFriendlyName(symbol);
  const dirLabel = direcao === 'CALL' ? 'COMPRA (CALL)' : 'VENDA (PUT)';
  const score = extras.scoreAtual ?? dados.consolidated.score;
  const scoreQuase = extras.scoreQuase ?? 40;
  const nivel = extras.nivelProntidao || 'EARLY';
  const reasons = (dados.consolidated.score_reasons || []).join(' ');

  // ⭐ FIX — checar bloqueios ANTES de aplicar "MATURE" indiscriminado
  const proximidadeDiag = diagnosticoProximidade(dados.consolidated.score_reasons || []);
  const bloqueio = proximidadeDiag && proximidadeDiag.nivel === 'BLOQUEADO';

  let proximidade, detalhe, emoji;
  if (bloqueio) { emoji = '⏸️'; proximidade = 'BLOQUEADO'; detalhe = proximidadeDiag.detalhe || 'bloqueio ativo — aguarda normalizar'; }
  else if (nivel === 'MATURE') { emoji = '🔥'; proximidade = 'PERTO DE ENTRAR'; detalhe = 'setup quase confirmado — prepara a entrada'; }
  else if (/RESPIRAÇÃO\s+(SIMPLES|DUPLA)|mercado precisa respirar/i.test(reasons)) { emoji = '🌬️'; proximidade = 'BLOQUEADO'; detalhe = 'mercado precisa respirar — aguarda normalizar'; }
  else if (/SINAL ANULADO.*DeMarker extremo|DEMARKER EXTREMO/i.test(reasons)) { emoji = '⛔'; proximidade = 'BLOQUEADO'; detalhe = 'mercado em extremo — aguarda normalizar'; }
  else if (/SINAL ANULADO:\s*\S+\s+DeM\s+[\d.]+\s+em\s+(fundo|topo)\s+extremo/i.test(reasons)) { emoji = '🛑'; proximidade = 'BLOQUEADO'; detalhe = 'exaustão nos TFs-chave — aguarda respirar'; }
  else if (/Prontidão reduzida por exaustão DeMarker/i.test(reasons)) { emoji = '🛑'; proximidade = 'BLOQUEADO'; detalhe = 'exaustão confirmada — aguarda respirar'; }
  else if (/micro timing bloqueou|Micro timing.*BLOQUEOU|DeM.*contra (CALL|PUT)|sobrecompra micro|sobrevenda micro/i.test(reasons)) { emoji = '🚫'; proximidade = 'BLOQUEADO'; detalhe = 'micro timing contra — aguarda alinhar'; }
  else if (/\(micro timing\) está contra a tendência/i.test(reasons)) { emoji = '🚫'; proximidade = 'BLOQUEADO'; detalhe = 'micro timing contra a tendência'; }
  else if (/\[FIX #40b\]|hist a desacelerar|trigger em conflito NÃO aceite/i.test(reasons)) { emoji = '🛑'; proximidade = 'BLOQUEADO'; detalhe = 'momentum a desacelerar — aguarda estabilizar'; }
  else if (/Pullback.*BLOQUEADO por estrutura esticada|estrutura no extremo.*aguarda correção/i.test(reasons)) { emoji = '🛑'; proximidade = 'BLOQUEADO'; detalhe = 'estrutura esticada — aguarda correção'; }
  else if (/Zona A rebaixada para B|Zona final rebaixada para B/i.test(reasons)) { emoji = '👀'; proximidade = 'EM FORMAÇÃO'; detalhe = 'confiança baixa — aguarda reforço'; }
  else if (/ADX muito fraco/i.test(reasons)) { emoji = '💤'; proximidade = 'LONGE'; detalhe = 'tendência macro sem força'; }
  else if (/CONFLITO|pullback em curso|aguarda histograma|aguarda alinhamento|aguarda flip/i.test(reasons)) { emoji = '👀'; proximidade = 'EM FORMAÇÃO'; detalhe = 'gatilho em ajuste — prepara-te'; }
  else { emoji = '📊'; proximidade = 'EM FORMAÇÃO'; detalhe = 'aguardando alinhamento'; }

  const prefixoTitulo = bloqueio
    ? `⏸️ Em formação: ${nomeAmigavel}`
    : (nivel === 'MATURE' ? `🔥 Perto de entrar: ${nomeAmigavel}` : `👀 Em formação: ${nomeAmigavel}`);

  return {
    titulo: prefixoTitulo,
    corpo: `${dirLabel} · ${nomeAmigavel}\n⚡ Score ${score}/100 · ${proximidade}\n💡 ${detalhe}`,
    detalhes: {
      tipo: 'PRONTIDAO', nomeAmigavel, direcao, modo: extras.mode || null,
      score, zona: dados.consolidated.zona, proximidade, detalhe,
      nivelProntidao: nivel, scoreQuase,
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
  const tituloBase = zona === 'A' ? '🚨 SINAL CONFIRMADO' : '⚡ SINAL MODERADO';
  return {
    titulo: `${tituloBase}: ${nomeAmigavel}`,
    corpo: `${emoji} ${dirLabel} · ${nomeAmigavel}\n💰 Entrada ${suggestion.entry} · 🎯 TP ${suggestion.takeProfit} · 🛑 SL ${suggestion.stopLoss}\n⚡ Score ${consolidated.score}/100 · Confiança ${conf}% · Zona ${zona}`,
    detalhes: { tipo: 'SINAL_CONFIRMADO', nomeAmigavel, direcao: consolidated.signal, modo: mode,
      entry: suggestion.entry, takeProfit: suggestion.takeProfit, stopLoss: suggestion.stopLoss,
      score: consolidated.score, confidence: conf, price: consolidated.price,
      reasons: (consolidated.score_reasons || []).slice(0, 14), zona }
  };
}

function formatarMensagem5Min(trade) {
  const nome = getFriendlyName(trade.symbol);
  return { titulo: `⏱️ Atualização (5min): ${nome}`, corpo: `${nome} · ${trade.signal}\n💵 Preço atual ${trade.currentPrice} (entrada ${trade.entry})\n📈 Mantém a posição — trade em curso`, detalhes: { tipo: '5MIN', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss } };
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

function formatarMensagemWin(trade) {
  const nome = getFriendlyName(trade.symbol);
  const duracao = Math.floor((Date.now() - trade.timestamp) / 60000);
  return { titulo: `🎯 WIN: ${nome}`, corpo: `${nome} · ${trade.signal} ✅\n💰 Alvo ${trade.takeProfit} atingido!\n⏱️ Duração: ${duracao}min · fecha a posição`, detalhes: { tipo: 'WIN', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, takeProfit: trade.takeProfit, currentPrice: trade.currentPrice, duracao: duracao + 'min' } };
}

function formatarMensagemStop(trade) {
  const nome = getFriendlyName(trade.symbol);
  const duracao = Math.floor((Date.now() - trade.timestamp) / 60000);
  return { titulo: `🛑 Stop Loss: ${nome}`, corpo: `${nome} · ${trade.signal}\n📉 Preço ${trade.currentPrice} bateu SL ${trade.stopLoss}\n⏱️ Duração: ${duracao}min · fecha a posição`, detalhes: { tipo: 'STOP', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, stopLoss: trade.stopLoss, currentPrice: trade.currentPrice, duracao: duracao + 'min' } };
}

function formatarMensagemTempoEsgotado(trade) {
  const nome = getFriendlyName(trade.symbol);
  const duracao = Math.floor((Date.now() - trade.timestamp) / 60000);
  return { titulo: `⏱️ Tempo esgotado: ${nome}`, corpo: `${nome} · ${trade.signal}\n💵 Preço perto da entrada (${trade.currentPrice})\n✅ Considera fechar no breakeven`, detalhes: { tipo: 'TEMPO_ESGOTADO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss, duracao: duracao + 'min' } };
}

function formatarMensagemExaustao(trade, currentPrice, tempoDecorridoMin, percentualPercorrido, motivo) {
  const nome = getFriendlyName(trade.symbol);
  const pct = (percentualPercorrido * 100).toFixed(1);
  return { titulo: `⚠️ Exaustão detetada: ${nome}`, corpo: `${nome} · ${trade.signal}\n📉 Tendência a perder força — ${motivo}\n💡 Considera fechar ANTES do SL tocar (${pct}% do alvo · ${tempoDecorridoMin}min)`, detalhes: { tipo: 'EXAUSTAO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss, motivo, percentualPercorrido: pct + '%', duracao: tempoDecorridoMin + 'min' } };
}

function formatarMensagemTimeout(trade, currentPrice, tempoDecorridoMin, motivo) {
  const nome = getFriendlyName(trade.symbol);
  const distanciaTotal = Math.abs(trade.takeProfit - trade.entry);
  const distanciaPercorrida = distanciaTotal > 0 ? (trade.signal === 'CALL' ? (currentPrice - trade.entry) : (trade.entry - currentPrice)) : 0;
  const pct = distanciaTotal > 0 ? ((distanciaPercorrida / distanciaTotal) * 100).toFixed(1) : '0.0';
  return { titulo: `🕐 Encerrado por timeout: ${nome}`, corpo: `${nome} · ${trade.signal}\n⏱️ ${tempoDecorridoMin}min sem avanço suficiente (${pct}% do alvo)\n❌ Motivo: ${motivo} · fecha a posição`, detalhes: { tipo: 'TIMEOUT', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode, entry: trade.entry, currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss, duracao: tempoDecorridoMin + 'min', motivo, percentualPercorrido: pct + '%', extensoes: trade.extensoes || 0 } };
}

// ========== REGISTAR SINAL (Turso) ==========
async function registrarEEnviarSinal(symbol, mode, tipo, msg, extra = {}, watchers = []) {
  const { titulo, corpo, detalhes } = msg;
  logger.info(`[SINAL] ${symbol} (${mode}) [${tipo}] ${titulo} — ${corpo} · ${watchers.length} watcher(s)`);

  if (tursoInitialized) {
    try {
      await db.execute({
        sql: `INSERT INTO signals (symbol, mode, tipo, titulo, corpo, detalhes, watchers, score, confidence, zona, entry, take_profit, stop_loss, nivel_prontidao, origem, criado_em)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          symbol, mode, tipo, titulo, corpo,
          detalhes ? JSON.stringify(detalhes) : null,
          JSON.stringify(watchers),
          extra.score ?? null,
          extra.confidence ?? null,
          extra.zona ?? detalhes?.zona ?? null,
          extra.entry ?? detalhes?.entry ?? null,
          extra.takeProfit ?? detalhes?.takeProfit ?? null,
          extra.stopLoss ?? detalhes?.stopLoss ?? null,
          extra.nivelProntidao ?? detalhes?.nivelProntidao ?? null,
          'motor',
          Date.now()
        ]
      });
    } catch (err) { logger.error('Erro ao gravar sinal:', err.message); }
  }

  const _openUrl = (tipo === 'SINAL_CONFIRMADO' || tipo === 'PRONTIDAO' || tipo === 'ARREFECIMENTO' || tipo === 'EXAUSTAO')
    ? '/?open=signals' : '/';
  await sendPushToWatchers(watchers, {
    title: titulo, body: corpo,
    tag: `${symbol}_${mode}_${tipo}`,
    data: { symbol, mode, tipo, url: _openUrl }
  });
}

// ========== ANÁLISE ==========
async function buscarSinalAnalise(symbol, mode) {
  const API_URL = process.env.ANALYSIS_API_URL || 'http://localhost:3001';
  const adminKey = process.env.ADMIN_SECRET;
  if (!adminKey) { logger.error('❌ ADMIN_SECRET não configurado!'); return null; }
  try {
    const response = await fetch(`${API_URL}/analyze`, {
      method: 'POST',
      headers: { 'x-admin-key': adminKey, 'Content-Type': 'application/json' },
      body: JSON.stringify({ symbol, mode })
    });
    if (!response.ok) {
      const texto = await response.text();
      logger.error(`❌ Erro HTTP ${response.status} ao buscar ${symbol}: ${texto}`);
      return null;
    }
    return await response.json();
  } catch (err) {
    logger.error(`❌ Erro de conexão ao buscar análise para ${symbol}:`, err.message);
    return null;
  }
}

async function _reenviarSinalConfirmado(symbol, mode, tradeKey, watchers) {
  setTimeout(async () => {
    try {
      const t = tradesAbertos.get(tradeKey);
      if (!t) return;
      if (Date.now() - t.timestamp > 2 * 60 * 1000) return;
      const nomeAmigavel = getFriendlyName(symbol);
      await sendPushToWatchers(watchers, {
        title: `🚨 Lembrete: ${nomeAmigavel}`,
        body: `Sinal ${t.signal} ainda ativo — entrada ${t.entry} · TP ${t.takeProfit} · SL ${t.stopLoss}\n⚡ Se não recebeste o sinal anterior, entra agora`,
        tag: `${symbol}_${mode}_SINAL_CONFIRMADO_RETRY`,
        data: { symbol, mode, tipo: 'SINAL_CONFIRMADO_RETRY', url: '/' }
      });
      logger.info(`🔁 [SINAL_CONFIRMADO_RETRY] ${symbol} (${mode}) reenviado após 30s`);
    } catch (err) { logger.error('Erro no retry do SINAL_CONFIRMADO:', err.message); }
  }, 30 * 1000);
}

async function analisarEEnviarSinais(symbol, mode, watchers = []) {
  const dados = await buscarSinalAnalise(symbol, mode);
  if (!dados || !dados.success) return;

  logger.info(`🔬 [RX] ${symbol} (${mode}) → signal=${dados.consolidated?.signal} zona=${dados.consolidated?.zona} score=${dados.consolidated?.score} suggestion=${dados.suggestion?.action}`);

  const agora = Date.now();
  const currentPrice = dados.consolidated.price;
  const tradeKey = `${symbol}_${mode}`;

  if (cooldownPosTrade.has(tradeKey)) {
    if (agora < cooldownPosTrade.get(tradeKey)) return;
    cooldownPosTrade.delete(tradeKey);
    removePersistedCooldown(tradeKey);
  }

  const trade = tradesAbertos.get(tradeKey);
  if (trade) {
    trade.currentPrice = currentPrice;
    const esticTradeCache = avaliarEsticamento(dados.consolidated.score_reasons);
    const distanciaTotal = Math.abs(trade.takeProfit - trade.entry);
    const distanciaPercorrida = distanciaTotal > 0 ? (trade.signal === 'CALL' ? (currentPrice - trade.entry) : (trade.entry - currentPrice)) : 0;
    const percentualPercorrido = distanciaTotal > 0 ? (distanciaPercorrida / distanciaTotal) : 0;
    const tempoDecorridoMin = Math.floor((agora - trade.timestamp) / 60000);

    const timeoutModo = getTimeoutModo(trade.mode);
    const timeoutExtendModo = getTimeoutExtendModo(trade.mode);
    const emRespiracaoOuPullback = detectarRespiracaoOuPullback(dados, trade, percentualPercorrido);
    const timeoutAtual = trade.timeoutAt || (trade.timestamp + timeoutModo);
    if (agora > timeoutAtual && emRespiracaoOuPullback && (trade.extensoesPullback || 0) < 3) {
      trade.timeoutAt = agora + timeoutExtendModo;
      trade.extensoesPullback = (trade.extensoesPullback || 0) + 1;
      trade.percentualNoUltimoCheck = percentualPercorrido;
      persistTradeUpdate(tradeKey, trade);
      logger.info(`↩️ [PULLBACK] ${tradeKey} mantido durante respiração (${trade.extensoesPullback}/3)`);
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
        logger.info(`⏭️ Trade ${tradeKey} [${trade.mode}] estendido (${trade.extensoes}/${EXTENSOES_MAX})`);
        persistTradeUpdate(tradeKey, trade);
      } else {
        const motivo = extensoes >= EXTENSOES_MAX ? `limite de ${EXTENSOES_MAX} extensões atingido` : `sem avanço suficiente`;
        await registrarEEnviarSinal(symbol, mode, 'TIMEOUT', formatarMensagemTimeout(trade, currentPrice, tempoDecorridoMin, motivo), { score: dados.consolidated.score }, watchers);
        tradesAbertos.delete(tradeKey);
        removePersistedTrade(tradeKey);
        const cdMsTimeout = calcularCooldownDinamico('TIMEOUT', dados, esticTradeCache);
        cooldownPosTrade.set(tradeKey, agora + cdMsTimeout);
        persistCooldown(tradeKey, agora + cdMsTimeout);
        logger.info(`⏱️ [FIX #57] Cooldown TIMEOUT → ${Math.round(cdMsTimeout/60000)}min | ${symbol} (${mode})`);
        return;
      }
    }

    let msgObj = null, tipo = null, fecharTrade = false;

    if (trade.signal === 'CALL' && currentPrice >= trade.takeProfit) { msgObj = formatarMensagemWin(trade); tipo = 'WIN'; fecharTrade = true; }
    else if (trade.signal === 'PUT' && currentPrice <= trade.takeProfit) { msgObj = formatarMensagemWin(trade); tipo = 'WIN'; fecharTrade = true; }
    else if (trade.signal === 'CALL' && currentPrice <= trade.stopLoss) { msgObj = formatarMensagemStop(trade); tipo = 'STOP'; fecharTrade = true; }
    else if (trade.signal === 'PUT' && currentPrice >= trade.stopLoss) { msgObj = formatarMensagemStop(trade); tipo = 'STOP'; fecharTrade = true; }
    else if (!trade.avisoExaustaoEnviado && percentualPercorrido >= 0.10 && percentualPercorrido < 0.50) {
      const contraDirecao =
        (trade.signal === 'CALL' && /sobrecompra|RSI extremo alto|RSI.*zona alta|DeM a esticar/i.test(esticTradeCache.motivo)) ||
        (trade.signal === 'PUT'  && /sobrevenda|RSI extremo baixo|RSI.*zona baixa|DeM a esticar \(baixo\)/i.test(esticTradeCache.motivo));
      if (esticTradeCache.esticado && esticTradeCache.nivel === 'ALTO' && contraDirecao) {
        const confirmacao = confirmaExaustaoMultiTF(dados, trade);
        if (confirmacao.confirmado) {
          msgObj = formatarMensagemExaustao(trade, currentPrice, tempoDecorridoMin, percentualPercorrido, `${esticTradeCache.motivo} · ${confirmacao.motivo}`);
          tipo = 'EXAUSTAO';
          trade.avisoExaustaoEnviado = true;
          logger.info(`⚠️ [FIX #56+#58] Exaustão CONFIRMADA em ${tradeKey}`);
        } else {
          logger.info(`💨 [FIX #58] ${tradeKey} trigger esticado mas ${confirmacao.motivo} — aguarda confirmação`);
          if (!emRespiracaoOuPullback && !trade.avisoTempoEsgotadoEnviado && tempoDecorridoMin >= 10 && percentualPercorrido < 0.15) { msgObj = formatarMensagemTempoEsgotado(trade); tipo = 'TEMPO_ESGOTADO'; trade.avisoTempoEsgotadoEnviado = true; fecharTrade = true; }
          else if (!trade.avisoAceleracaoEnviado && tempoDecorridoMin <= 2 && percentualPercorrido >= 0.40) { msgObj = formatarMensagemAceleracao(trade); tipo = 'ACELERACAO'; trade.avisoAceleracaoEnviado = true; trade.avisoSeguindoEnviado = true; }
          else if (!trade.avisoSeguindoEnviado && percentualPercorrido >= 0.30) { msgObj = formatarMensagemSeguindo(trade); tipo = 'SEGUINDO'; trade.avisoSeguindoEnviado = true; }
          else if (!trade.aviso5MinEnviado && tempoDecorridoMin >= 5 && percentualPercorrido > 0.10 && percentualPercorrido < 0.50) { msgObj = formatarMensagem5Min(trade); tipo = '5MIN'; trade.aviso5MinEnviado = true; }
          else if (!trade.avisoZeroRiscoEnviado && percentualPercorrido >= 0.50) { msgObj = formatarMensagemZeroRisco(trade); tipo = 'ZERO_RISCO'; trade.avisoZeroRiscoEnviado = true; }
          else if (!trade.avisoQuaseLaEnviado && percentualPercorrido >= 0.80) { msgObj = formatarMensagemQuaseLa(trade); tipo = 'QUASE_LA'; trade.avisoQuaseLaEnviado = true; }
        }
      } else {
        if (!emRespiracaoOuPullback && !trade.avisoTempoEsgotadoEnviado && tempoDecorridoMin >= 10 && percentualPercorrido < 0.15) { msgObj = formatarMensagemTempoEsgotado(trade); tipo = 'TEMPO_ESGOTADO'; trade.avisoTempoEsgotadoEnviado = true; fecharTrade = true; }
        else if (!trade.avisoAceleracaoEnviado && tempoDecorridoMin <= 2 && percentualPercorrido >= 0.40) { msgObj = formatarMensagemAceleracao(trade); tipo = 'ACELERACAO'; trade.avisoAceleracaoEnviado = true; trade.avisoSeguindoEnviado = true; }
        else if (!trade.avisoSeguindoEnviado && percentualPercorrido >= 0.30) { msgObj = formatarMensagemSeguindo(trade); tipo = 'SEGUINDO'; trade.avisoSeguindoEnviado = true; }
        else if (!trade.aviso5MinEnviado && tempoDecorridoMin >= 5 && percentualPercorrido > 0.10 && percentualPercorrido < 0.50) { msgObj = formatarMensagem5Min(trade); tipo = '5MIN'; trade.aviso5MinEnviado = true; }
        else if (!trade.avisoZeroRiscoEnviado && percentualPercorrido >= 0.50) { msgObj = formatarMensagemZeroRisco(trade); tipo = 'ZERO_RISCO'; trade.avisoZeroRiscoEnviado = true; }
        else if (!trade.avisoQuaseLaEnviado && percentualPercorrido >= 0.80) { msgObj = formatarMensagemQuaseLa(trade); tipo = 'QUASE_LA'; trade.avisoQuaseLaEnviado = true; }
      }
    }
    else if (!emRespiracaoOuPullback && !emRespiracaoOuPullback && !trade.avisoTempoEsgotadoEnviado && tempoDecorridoMin >= 10 && percentualPercorrido < 0.15) { msgObj = formatarMensagemTempoEsgotado(trade); tipo = 'TEMPO_ESGOTADO'; trade.avisoTempoEsgotadoEnviado = true; fecharTrade = true; }
    else if (!trade.avisoAceleracaoEnviado && tempoDecorridoMin <= 2 && percentualPercorrido >= 0.40) { msgObj = formatarMensagemAceleracao(trade); tipo = 'ACELERACAO'; trade.avisoAceleracaoEnviado = true; trade.avisoSeguindoEnviado = true; }
    else if (!trade.avisoSeguindoEnviado && percentualPercorrido >= 0.30) { msgObj = formatarMensagemSeguindo(trade); tipo = 'SEGUINDO'; trade.avisoSeguindoEnviado = true; }
    else if (!trade.aviso5MinEnviado && tempoDecorridoMin >= 5 && percentualPercorrido > 0.10 && percentualPercorrido < 0.50) { msgObj = formatarMensagem5Min(trade); tipo = '5MIN'; trade.aviso5MinEnviado = true; }
    else if (!trade.avisoZeroRiscoEnviado && percentualPercorrido >= 0.50) { msgObj = formatarMensagemZeroRisco(trade); tipo = 'ZERO_RISCO'; trade.avisoZeroRiscoEnviado = true; }
    else if (!trade.avisoQuaseLaEnviado && percentualPercorrido >= 0.80) { msgObj = formatarMensagemQuaseLa(trade); tipo = 'QUASE_LA'; trade.avisoQuaseLaEnviado = true; }

    if (fecharTrade) {
      tradesAbertos.delete(tradeKey);
      removePersistedTrade(tradeKey);
      const cdMs = calcularCooldownDinamico(tipo, dados, esticTradeCache);
      cooldownPosTrade.set(tradeKey, agora + cdMs);
      persistCooldown(tradeKey, agora + cdMs);
      logger.info(`⏱️ [FIX #57] Cooldown ${tipo} → ${Math.round(cdMs/60000)}min | ${symbol} (${mode})`);
    } else {
      persistTradeUpdate(tradeKey, trade);
    }

    if (msgObj) await registrarEEnviarSinal(symbol, mode, tipo, msgObj, { score: dados.consolidated.score }, watchers);
    return;
  }

  // Anti-duplicado em memória
  const ultimoTs = ultimoSinalPorPar.get(tradeKey) || 0;
  const CINCO_MIN_MS = 5 * 60 * 1000;
  if (Date.now() - ultimoTs < CINCO_MIN_MS) {
    logger.info(`⏭️ Anti-duplicado (memória): sinal há ${Math.round((Date.now() - ultimoTs)/1000)}s para ${symbol}/${mode} — saltar`);
    return;
  }

  if (dados.consolidated.signal !== 'HOLD' && (dados.consolidated.zona === 'A' || dados.consolidated.zona === 'B')) {
    if (dados.suggestion && dados.suggestion.action === 'ENTRADA' &&
        dados.suggestion.entry != null && dados.suggestion.takeProfit != null && dados.suggestion.stopLoss != null) {

      const esticSinal = avaliarEsticamento(dados.consolidated.score_reasons);
      if (esticSinal.esticado && esticSinal.nivel === 'ALTO') {
        logger.info(`⛔ [FILTRO EXTREMO] ${symbol} (${mode}) SINAL ignorado — ${esticSinal.motivo}`);
        prontidaoAtiva.delete(tradeKey);
        prontidaoHistorico.delete(tradeKey);
        prontidaoForaContagem.delete(tradeKey);
        return;
      }

      const novoTrade = {
        symbol, mode,
        signal: dados.consolidated.signal,
        entry: dados.suggestion.entry,
        takeProfit: dados.suggestion.takeProfit,
        stopLoss: dados.suggestion.stopLoss,
        zonaEntrada: dados.consolidated.zona,
        watchers: [...watchers],
        timestamp: agora,
        avisoSeguindoEnviado: false, avisoZeroRiscoEnviado: false,
        avisoQuaseLaEnviado: false, avisoAceleracaoEnviado: false,
        avisoTempoEsgotadoEnviado: false, aviso5MinEnviado: false,
        avisoExaustaoEnviado: false,
        timeoutAt: agora + getTimeoutModo(mode),
        extensoes: 0, percentualNoUltimoCheck: 0
      };

      tradesAbertos.set(tradeKey, novoTrade);
      persistTradeOpen(tradeKey, novoTrade);
      ultimoSinalPorPar.set(tradeKey, agora);

      const zonaTxt = dados.consolidated.zona === 'A' ? 'SINAL CONFIRMADO' : 'SINAL MODERADO';
      logger.info(`🚀 [${zonaTxt}] ${symbol} (${mode}) → ${dados.consolidated.signal} @ ${dados.suggestion.entry}`);

      try {
        const msgSinal = formatarMensagemSinal(symbol, dados, mode);
        await registrarEEnviarSinal(symbol, mode, 'SINAL_CONFIRMADO', msgSinal, {
          score: dados.consolidated.score, confidence: dados.consolidated.confidence,
          zona: dados.consolidated.zona,
          entry: dados.suggestion.entry, takeProfit: dados.suggestion.takeProfit, stopLoss: dados.suggestion.stopLoss
        }, watchers);
        logger.info(`✅ [PUSH ENVIADO] ${symbol} (${mode}) → SINAL_CONFIRMADO`);
      } catch (errSinal) {
        logger.error(`❌ FALHA AO ENVIAR SINAL_CONFIRMADO ${symbol}: ${errSinal.message}`);
      }

      _reenviarSinalConfirmado(symbol, mode, tradeKey, watchers);
      prontidaoAtiva.delete(tradeKey);
      prontidaoHistorico.delete(tradeKey);
      prontidaoForaContagem.delete(tradeKey);
      prontidaoUltimoEnvio.delete(tradeKey);
      arrefecimentoUltimoEnvio.delete(tradeKey);
      removePersistedProntidao(tradeKey);
    }
  }
  else if (dados.consolidated.signal === 'HOLD' && (dados.consolidated.zona === 'B' || dados.consolidated.zona === 'C')) {
    const scoreAtual = dados.consolidated.score || 0;
    const regimeAtualPush = dados.consolidated.regime || 'UNKNOWN';
    if (regimeAtualPush === 'CHOP') {
      logger.info(`🔇 [FIX #60] PRONTIDAO ignorada em CHOP: ${symbol} (${mode})`);
      prontidaoForaContagem.delete(tradeKey);
      return;
    }
    const estic = avaliarEsticamento(dados.consolidated.score_reasons);
    if (estic.esticado && estic.nivel === 'ALTO') {
      logger.info(`⛔ [FILTRO EXTREMO] ${symbol} (${mode}) PRONTIDAO ignorada — ${estic.motivo}`);
      prontidaoForaContagem.delete(tradeKey);
      return;
    }

    // ⭐ FIX-PRONTIDAO — Se há bloqueio ativo nos reasons (DeMarker extremo, reversão,
    //   respiração, etc.), não enviar MATURE ("setup quase confirmado"). Só EARLY.
    const proximidadePront = diagnosticoProximidade(dados.consolidated.score_reasons);
    const bloqueioAtivo = proximidadePront && proximidadePront.nivel === 'BLOQUEADO';
    let _forcarSomenteEarly = false;
    if (bloqueioAtivo) {
      logger.info(`⛔ [FIX-PRONTIDAO] ${symbol} (${mode}) bloqueio ativo (${proximidadePront.detalhe}) — a enviar apenas EARLY informativo`);
      _forcarSomenteEarly = true;
    }

    const prefsPorWatcher = new Map();
    for (const tk of watchers) {
      const prefs = await getUserPreferences(tk);
      prefsPorWatcher.set(tk, prefs[mode] || DEFAULT_PREFS[mode]);
    }

    const scoreGlobalMin = prefsPorWatcher.size > 0
      ? Math.min(...Array.from(prefsPorWatcher.values()).map(p => p.scoreEarly))
      : getScoreProntidaoMin(mode);

    if (dados.consolidated.zona === 'C' && scoreAtual < scoreGlobalMin) {
      prontidaoForaContagem.delete(tradeKey);
      return;
    }

    prontidaoForaContagem.delete(tradeKey);
    const historico = prontidaoHistorico.get(tradeKey) || [];
    historico.push({ score: scoreAtual, t: agora });
    if (historico.length > 5) historico.shift();
    prontidaoHistorico.set(tradeKey, historico);
    persistProntidao(tradeKey, historico, prontidaoAtiva.has(tradeKey));

    const cfg = getProntidaoConfig(mode);
    const ultimoEnvio = prontidaoUltimoEnvio.get(tradeKey) || 0;
    const podeEnviarAgora = (agora - ultimoEnvio) >= cfg.cooldownMs;
    const ultimoGlobal = prontidaoGlobalPorSymbol.get(symbol) || 0;
    const podeEnviarGlobal = (agora - ultimoGlobal) >= PRONTIDAO_GLOBAL_COOLDOWN_MS;

    if ((!prontidaoAtiva.has(tradeKey) || podeEnviarAgora) && podeEnviarGlobal) {
      const direcaoPrep = extrairDirecaoPrep(dados);
      if (direcaoPrep) {
        const subindo = historico.length >= 3 && historico[historico.length - 1].score > historico[0].score;
        const earlyTks = [], matureTks = [];
        for (const tk of watchers) {
          const p = prefsPorWatcher.get(tk) || DEFAULT_PREFS[mode];
          if (scoreAtual < p.scoreEarly) continue;
          // ⭐ FIX — se há bloqueio ativo, força EARLY mesmo que o score chegue para MATURE
          if (!_forcarSomenteEarly && scoreAtual >= p.scoreMature) matureTks.push(tk);
          else earlyTks.push(tk);
        }

        if (matureTks.length > 0) {
          await registrarEEnviarSinal(symbol, mode, 'PRONTIDAO',
            formatarMensagemPrep(symbol, direcaoPrep, dados, { subindo, historico, mode, nivelProntidao: 'MATURE', scoreAtual, scoreQuase: scoreAtual }),
            { score: scoreAtual, nivelProntidao: 'MATURE' }, matureTks);
        }
        if (earlyTks.length > 0) {
          await registrarEEnviarSinal(symbol, mode, 'PRONTIDAO',
            formatarMensagemPrep(symbol, direcaoPrep, dados, { subindo, historico, mode, nivelProntidao: 'EARLY', scoreAtual, scoreQuase: 999 }),
            { score: scoreAtual, nivelProntidao: 'EARLY' }, earlyTks);
        }
        if (matureTks.length > 0 || earlyTks.length > 0) {
          prontidaoAtiva.add(tradeKey);
          prontidaoUltimoEnvio.set(tradeKey, agora);
          prontidaoGlobalPorSymbol.set(symbol, agora);
          prontidaoForaContagem.set(tradeKey, 0);
          persistProntidao(tradeKey, prontidaoHistorico.get(tradeKey), true);
        }
      }
    }
  }
  else if (dados.consolidated.signal === 'HOLD' && prontidaoAtiva.has(tradeKey)) {
    const cfg = getProntidaoConfig(mode);
    const fora = (prontidaoForaContagem.get(tradeKey) || 0) + 1;
    prontidaoForaContagem.set(tradeKey, fora);
    if (fora >= cfg.ciclosForaParaArrefecer) {
      const ultimoArref = arrefecimentoUltimoEnvio.get(tradeKey) || 0;
      if ((agora - ultimoArref) >= cfg.cooldownMs) {
        await registrarEEnviarSinal(symbol, mode, 'ARREFECIMENTO',
          formatarMensagemArrefecimento(symbol, dados.consolidated.score, dados),
          { score: dados.consolidated.score }, watchers);
        arrefecimentoUltimoEnvio.set(tradeKey, agora);
      }
      prontidaoAtiva.delete(tradeKey);
      prontidaoHistorico.delete(tradeKey);
      prontidaoForaContagem.delete(tradeKey);
      removePersistedProntidao(tradeKey);
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
      const idx = tradeKey.lastIndexOf('_');
      if (idx > 0) {
        queue.set(`${tradeKey.slice(0, idx)}|${tradeKey.slice(idx + 1)}`, {
          symbol: tradeKey.slice(0, idx), mode: tradeKey.slice(idx + 1)
        });
      }
    }

    const minutoAtual = Math.floor(Date.now() / 60000);
    for (const u of activeUsers) {
      for (const mode of MODOS_OK) {
        const cad = CADENCIAS[mode] || 3;
        if (minutoAtual % cad !== 0) continue;
        for (const symbol of u[mode] || []) {
          queue.set(`${symbol}|${mode}`, { symbol, mode });
        }
      }
    }

    if (queue.size === 0) return;
    logger.info(`📦 Ciclo: ${queue.size} par(es) · ${activeUsers.length} user(s) ativos`);

    for (const { symbol, mode } of queue.values()) {
      const tradeKey = `${symbol}_${mode}`;
      const trade = tradesAbertos.get(tradeKey);
      let watchers;
      if (trade && Array.isArray(trade.watchers)) {
        watchers = trade.watchers;
      } else {
        watchers = activeUsers.filter(u => (u[mode] || []).includes(symbol)).map(u => u.tokenHash);
      }
      await analisarEEnviarSinais(symbol, mode, watchers);
      await new Promise(r => setTimeout(r, 1200));
    }
  } catch (err) {
    logger.error(`Erro no cron: ${err.message}\n${err.stack || ''}`);
  } finally {
    cronEmExecucao = false;
  }
}, { timezone: 'America/Sao_Paulo' });

// ========== API ENDPOINTS ==========

app.get('/health', (req, res) => res.json({
  status: 'ok', uptime: Math.floor(process.uptime()),
  tradesAbertos: tradesAbertos.size, cooldowns: cooldownPosTrade.size,
  prontidoes: prontidaoAtiva.size, turso: tursoInitialized
}));

app.get('/api/vapid-public-key', (req, res) => {
  if (!pushConfigured) return res.status(503).json({ error: 'Push não configurado no servidor' });
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
  await sendPushToWatchers([req.user.tokenHash], {
    title: '🔔 Teste', body: 'Notificações a funcionar corretamente!', tag: 'teste_' + Date.now()
  });
  res.json({ success: true });
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
  if (!tursoInitialized) return res.json({ active: false, watchlist: { SNIPER: [], 'CAÇADOR': [], PESCADOR: [], BALEEIRO: [] } });
  try {
    const wl = await getUserWatchlist(req.user.tokenHash);
    res.json({
      active: wl.engineActive, plano: req.user.plano,
      maxAtivosPorModo: req.user.plano?.maxAtivosPorModo ?? 10,
      watchlist: { SNIPER: wl.SNIPER, 'CAÇADOR': wl['CAÇADOR'], PESCADOR: wl.PESCADOR, BALEEIRO: wl.BALEEIRO }
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/engine-start', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  try {
    const wl = await getUserWatchlist(req.user.tokenHash);
    const totalAtivos = contarAtivosWatchlist(wl);
    if (totalAtivos === 0) {
      return res.status(400).json({ success: false, error: 'Adiciona pelo menos 1 ativo à watchlist antes de ativar o motor.', code: 'WATCHLIST_EMPTY', totalAtivos: 0 });
    }
    await saveUserWatchlist(req.user.tokenHash, { engineActive: true, email: req.user.email || null });
    invalidarCacheWatchlists();
    logger.info(`✅ [ENGINE-START] user=${req.user.tokenHash} ativou motor com ${totalAtivos} ativo(s)`);
    res.json({ success: true, active: true, totalAtivos });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/engine-stop', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  try {
    await saveUserWatchlist(req.user.tokenHash, { engineActive: false });
    invalidarCacheWatchlists();
    res.json({ success: true, active: false });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/engine-watchlist', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.json({ watchlist: { SNIPER: [], 'CAÇADOR': [], PESCADOR: [], BALEEIRO: [] } });
  try {
    const wl = await getUserWatchlist(req.user.tokenHash);
    res.json({ watchlist: { SNIPER: wl.SNIPER, 'CAÇADOR': wl['CAÇADOR'], PESCADOR: wl.PESCADOR, BALEEIRO: wl.BALEEIRO } });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/engine-watchlist', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  const { watchlist } = req.body || {};
  if (!watchlist || typeof watchlist !== 'object') return res.status(400).json({ error: 'watchlist deve ser um objeto' });
  try {
    const maxAtivos = req.user.plano?.maxAtivosPorModo ?? 10;
    if (maxAtivos <= 0) return res.status(403).json({ error: 'A tua conta não tem um plano ativo.' });

    const current = await getUserWatchlist(req.user.tokenHash);
    const cortado = {
      SNIPER:    Array.isArray(watchlist.SNIPER)    && [...new Set(watchlist.SNIPER)].length    > maxAtivos,
      'CAÇADOR': Array.isArray(watchlist['CAÇADOR']) && [...new Set(watchlist['CAÇADOR'])].length > maxAtivos,
      PESCADOR:  Array.isArray(watchlist.PESCADOR)  && [...new Set(watchlist.PESCADOR)].length  > maxAtivos,
      BALEEIRO:  Array.isArray(watchlist.BALEEIRO)  && [...new Set(watchlist.BALEEIRO)].length  > maxAtivos
    };
    const final = {
      SNIPER:    Array.isArray(watchlist.SNIPER)    ? [...new Set(watchlist.SNIPER)].slice(0, maxAtivos)    : current.SNIPER,
      'CAÇADOR': Array.isArray(watchlist['CAÇADOR']) ? [...new Set(watchlist['CAÇADOR'])].slice(0, maxAtivos) : current['CAÇADOR'],
      PESCADOR:  Array.isArray(watchlist.PESCADOR)  ? [...new Set(watchlist.PESCADOR)].slice(0, maxAtivos)  : current.PESCADOR,
      BALEEIRO:  Array.isArray(watchlist.BALEEIRO)  ? [...new Set(watchlist.BALEEIRO)].slice(0, maxAtivos)  : current.BALEEIRO,
      email: req.user.email || null
    };
    await saveUserWatchlist(req.user.tokenHash, final);
    invalidarCacheWatchlists();
    const houveCorte = Object.values(cortado).some(Boolean);
    res.json({
      success: true, watchlist: final, plano: req.user.plano, maxAtivosPorModo: maxAtivos,
      cortado: houveCorte ? cortado : null,
      mensagem: houveCorte ? `Plano ${req.user.plano?.nome || 'atual'} permite ${maxAtivos} por modo.` : null
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/engine-watchlist', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  try {
    await saveUserWatchlist(req.user.tokenHash, { SNIPER: [], 'CAÇADOR': [], PESCADOR: [], BALEEIRO: [] });
    invalidarCacheWatchlists();
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/scan-group', authMiddleware, async (req, res) => {
  const { group, mode } = req.body;
  if (!group || !mode) return res.status(400).json({ error: 'Os campos "group" e "mode" são obrigatórios.' });
  const symbols = assetGroups[group];
  if (!symbols) return res.status(400).json({ error: `Grupo "${group}" não reconhecido.` });
  if (!MODOS_OK.includes(mode)) return res.status(400).json({ error: `Modo "${mode}" inválido.` });
  try {
    const allResults = [];
    for (let i = 0; i < symbols.length; i += 5) {
      const batch = symbols.slice(i, i + 5);
      const batchResults = await Promise.allSettled(batch.map(async (symbol) => {
        const data = await buscarSinalAnalise(symbol, mode);
        if (!data || !data.success) return { symbol, name: getFriendlyName(symbol), signal: 'HOLD', zona: '?', score: 0, reasons: ['Erro ao obter análise'], error: true };
        const consolidated = data.consolidated || {};
        const score = consolidated.score || 0;
        const zona = consolidated.zona || '?';
        const signal = consolidated.signal || 'HOLD';

        // ⭐ NOVO — calcular mensagem amigável + distância para zona B/A
        const ZONA_B_MIN = { 'SNIPER': 45, 'CAÇADOR': 50, 'PESCADOR': 55, 'BALEEIRO': 60 };
        const zonaBMin = ZONA_B_MIN[mode] || 50;
        const zonaAMin = zonaBMin + 10;
        const distZonaB = Math.max(0, zonaBMin - score);
        const distZonaA = Math.max(0, zonaAMin - score);

        let mensagemProntidao = null;
        if (signal !== 'HOLD' && zona === 'A') {
          mensagemProntidao = `🚨 Sinal CONFIRMADO ${signal} — score ${score}`;
        } else if (signal !== 'HOLD' && zona === 'B') {
          mensagemProntidao = `⚡ Sinal MODERADO ${signal} — score ${score} (entrada validada)`;
        } else if (zona === 'B') {
          const dirPrep = extrairDirecaoPrep(data);
          const dirTxt = dirPrep ? dirPrep : 'aguarda direção';
          mensagemProntidao = `👀 Em Zona B (${dirTxt}) — score ${score}, ${distZonaA > 0 ? `faltam ${distZonaA} pts p/ Zona A` : 'pronto p/ A'}`;
        } else if (score >= zonaBMin - 8 && score < zonaBMin) {
          mensagemProntidao = `🟡 Quase em Zona B — score ${score}/${zonaBMin} (faltam ${distZonaB} pts)`;
        } else if (score >= zonaBMin - 20) {
          mensagemProntidao = `🔵 Em formação — score ${score} (faltam ${distZonaB} pts p/ Zona B)`;
        } else {
          mensagemProntidao = `⚪ Aguarda — ${distZonaB} pts p/ Zona B`;
        }

        // Extra: se esticado, avisar
        const estic = avaliarEsticamento(consolidated.score_reasons);
        if (estic.esticado && estic.nivel === 'ALTO') {
          mensagemProntidao = `🔥 ESTICADO — ${estic.motivo}`;
        }

        return {
          symbol, name: getFriendlyName(symbol),
          signal,
          zona,
          score,
          proximidade: diagnosticoProximidade(consolidated.score_reasons),
          esticamento: estic,
          mensagemProntidao,
          distanciaZonaB: distZonaB,
          distanciaZonaA: distZonaA,
          reasons: (consolidated.score_reasons || []).slice(0, 4)
        };
      }));
      allResults.push(...batchResults);
      if (i + 5 < symbols.length) await new Promise(r => setTimeout(r, 800));
    }
    res.json({ success: true, results: allResults.filter(r => r.status === 'fulfilled').map(r => r.value) });
  } catch (err) {
    logger.error('Erro no /api/scan-group:', err.message);
    res.status(500).json({ error: 'Erro interno ao analisar grupo.' });
  }
});

app.get('/api/signals', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.json({ signals: [] });
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const mode = req.query.mode || null;
    let sql = 'SELECT * FROM signals WHERE 1=1';
    const args = [];
    if (mode) { sql += ' AND mode = ?'; args.push(mode); }
    sql += ' ORDER BY criado_em DESC LIMIT ?';
    args.push(limit);
    const result = await db.execute({ sql, args });
    const tokenHash = req.user.tokenHash;
    const signals = result.rows
      .filter(row => {
        try {
          const watchers = JSON.parse(row.watchers || '[]');
          return Array.isArray(watchers) && watchers.includes(tokenHash);
        } catch { return false; }
      })
      .map(row => ({
        id: row.id, symbol: row.symbol, mode: row.mode, tipo: row.tipo,
        titulo: row.titulo, corpo: row.corpo,
        detalhes: row.detalhes ? JSON.parse(row.detalhes) : null,
        watchers: JSON.parse(row.watchers || '[]'),
        score: row.score, confidence: row.confidence, zona: row.zona,
        entry: row.entry, takeProfit: row.take_profit, stopLoss: row.stop_loss,
        nivelProntidao: row.nivel_prontidao, origem: row.origem,
        criadoEm: new Date(row.criado_em).toISOString()
      }));
    res.json({ signals });
  } catch (err) {
    logger.error(`Erro ao buscar sinais: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/signals', authMiddleware, async (req, res) => {
  if (!tursoInitialized) return res.status(503).json({ error: 'Turso indisponível' });
  const mode = req.query.mode || null;
  try {
    let sql = 'SELECT * FROM signals WHERE 1=1';
    const args = [];
    if (mode) { sql += ' AND mode = ?'; args.push(mode); }
    const result = await db.execute({ sql, args });

    const tokenHash = req.user.tokenHash;
    let deletados = 0, atualizados = 0;

    for (const row of result.rows) {
      let watchers = [];
      try { watchers = JSON.parse(row.watchers || '[]'); } catch { watchers = []; }
      if (!Array.isArray(watchers) || !watchers.includes(tokenHash)) continue;

      const restantes = watchers.filter(t => t !== tokenHash);
      if (restantes.length === 0) {
        await db.execute({ sql: 'DELETE FROM signals WHERE id = ?', args: [row.id] });
        deletados++;
      } else {
        await db.execute({ sql: 'UPDATE signals SET watchers = ? WHERE id = ?', args: [JSON.stringify(restantes), row.id] });
        atualizados++;
      }
    }

    const total = deletados + atualizados;
    logger.info(`🗑️ [SIGNALS-CLEAR] user=${tokenHash}${mode?' mode='+mode:''} → ${deletados} apagado(s), ${atualizados} atualizado(s)`);
    res.json({ success: true, deletados, atualizados, total, mensagem: `${total} sinal(is) removido(s) do teu histórico` });
  } catch (err) {
    logger.error('Erro em DELETE /api/signals:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/analysis-history', authMiddleware, async (req, res) => {
  res.json({ success: true, skipped: true });
});

app.get('/api/analysis-history', authMiddleware, async (req, res) => {
  res.json({ history: [] });
});

app.get('/api/stats', authMiddleware, async (req, res) => {
  const wl = tursoInitialized ? await getUserWatchlist(req.user.tokenHash).catch(() => null) : null;
  const stats = {
    engineActive: !!wl?.engineActive,
    watchlistCount: wl ? [wl.SNIPER, wl['CAÇADOR'], wl.PESCADOR, wl.BALEEIRO].reduce((a, arr) => a + arr.length, 0) : 0,
    openTrades: tradesAbertos.size, uptime: process.uptime(),
    turso: tursoInitialized, pushConfigured
  };
  if (tursoInitialized) {
    try {
      const tokenHash = req.user.tokenHash;
      const all = await db.execute('SELECT watchers, criado_em FROM signals');
      const tokenSignals = all.rows.filter(r => {
        try { return JSON.parse(r.watchers || '[]').includes(tokenHash); } catch { return false; }
      });
      stats.totalSignals = tokenSignals.length;
      const hojeInicio = new Date(new Date().setHours(0,0,0,0)).getTime();
      stats.signalsToday = tokenSignals.filter(r => r.criado_em >= hojeInicio).length;
    } catch (err) { logger.error(`Erro stats: ${err.message}`); }
  }
  res.json(stats);
});

// ========== SERVE FRONTEND (PWA) ==========
app.get(/^\/(?!.*\.(png|jpg|jpeg|gif|svg|ico|json|js|css|woff|woff2|ttf|webp)).*$/, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', async () => {
  logger.info(`🚀 Servidor rodando na porta ${PORT}`);
  try {
    await tursoInitPromise;
  } catch (e) {
    logger.error('Falha ao inicializar Turso:', e.message);
  }
  logger.info(`Turso: ${tursoInitialized ? 'Conectado' : 'Não'}`);
  logger.info(`Push: ${pushConfigured ? 'Configurado' : 'Não configurado'}`);
  logger.info(`v2.21: Turso (libSQL) substitui Firestore.`);
  logger.info(`FIX #80 + #80b: Respiração mode-aware activa.`);
  logger.info(`FIX-PRONTIDAO: Bloqueio activo impede MATURE enganador.`);
  try {
    await loadStateFromTurso();
  } catch (e) {
    logger.error('loadStateFromTurso falhou:', e.message);
  }
});

const SELF_URL = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
setInterval(async () => {
  try { await fetch(`${SELF_URL}/health`); } catch (err) { logger.error('Erro no self-ping:', err.message); }
}, 4 * 60 * 1000);

process.on('SIGTERM', () => {
  logger.info('SIGTERM - encerrando...');
  process.exit(0);
});
