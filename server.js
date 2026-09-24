// ===================== server.js (Painel de Sinais) =====================
// Motor de análise + Web Push + histórico de sinais no Firestore.
// v2.7 — cooldown global por símbolo + limpar histórico + timeout inteligente

import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import admin from 'firebase-admin';
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

// Rotas PWA explícitas ANTES do static (headers corretos p/ iOS)
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

// ⭐ Política de Privacidade (obrigatório para Play Store)
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

// ========== FIREBASE ==========
let db = null;
let firebaseInitialized = false;
function initializeFirebase() {
  try {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || '{}');
    if (!serviceAccount.project_id) {
      logger.warn('Firebase não configurado.');
      return false;
    }
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    db = admin.firestore();
    firebaseInitialized = true;
    logger.info('Firebase inicializado!');
    return true;
  } catch (err) {
    logger.error('Erro ao inicializar Firebase:', err.message);
    return false;
  }
}
initializeFirebase();

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
    logger.error(`   VAPID_SUBJECT: "${VAPID_SUBJECT}"`);
    logger.error(`   VAPID_PUBLIC_KEY:  ${VAPID_PUBLIC_KEY.length} caracteres (esperado ~87)`);
    logger.error(`   VAPID_PRIVATE_KEY: ${VAPID_PRIVATE_KEY.length} caracteres (esperado ~43)`);
    logger.error('   Push desativado — servidor continua a arrancar normalmente.');
  }
} else {
  logger.warn('VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY não configurados. Push desativado.');
}

async function sendPushToWatchers(watchers, payload) {
  if (!pushConfigured || !firebaseInitialized || !watchers || watchers.length === 0) return;
  try {
    const body = JSON.stringify(payload);
    const deletions = [];
    const chunks = [];
    for (let i = 0; i < watchers.length; i += 30) chunks.push(watchers.slice(i, i + 30));

    for (const chunk of chunks) {
      const snap = await db.collection('push_subscriptions')
        .where('tokenHash', 'in', chunk)
        .get();

      await Promise.all(snap.docs.map(async (doc) => {
        const sub = doc.data();
        if (!sub.subscription) return;
        try {
          await webpush.sendNotification(sub.subscription, body);
        } catch (err) {
          if (err.statusCode === 404 || err.statusCode === 410) deletions.push(doc.ref.delete());
          else logger.error('Erro push:', err.message);
        }
      }));
    }
    if (deletions.length) await Promise.all(deletions);
  } catch (err) {
    logger.error('sendPushToWatchers erro:', err.message);
  }
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
    logger.error('[VALIDATE-PROXY] Erro ao contactar servidor de análise:', err.message);
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
      token,
      tokenHash,
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

// ⭐ NOVO — Cooldown GLOBAL por símbolo (evita spam do mesmo ativo em vários modos)
const PRONTIDAO_GLOBAL_COOLDOWN_MS = 10 * 60 * 1000; // 10 min

// ⭐ Timeout variável por modo — alinhado com a duração real de cada estratégia
const TRADE_TIMEOUT_POR_MODO_MS = {
  'SNIPER':   20 * 60 * 1000,            // 20 min
  'CAÇADOR':  90 * 60 * 1000,            // 1h30
  'PESCADOR': 12 * 60 * 60 * 1000,       // 12h
  'BALEEIRO': 72 * 60 * 60 * 1000        // 3 dias
};

const TRADE_TIMEOUT_EXTEND_POR_MODO_MS = {
  'SNIPER':   15 * 60 * 1000,            // +15 min
  'CAÇADOR':  45 * 60 * 1000,            // +45 min
  'PESCADOR': 6 * 60 * 60 * 1000,        // +6h
  'BALEEIRO': 48 * 60 * 60 * 1000        // +48h
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

// ========== WATCHLIST POR USER ==========
async function getUserWatchlist(tokenHash) {
  if (!firebaseInitialized) return null;
  const doc = await db.collection('user_watchlists').doc(tokenHash).get();
  const data = doc.exists ? doc.data() : {};
  return {
    tokenHash,
    email: data.email || null,
    engineActive: !!data.engineActive,
    SNIPER: data.SNIPER || [],
    'CAÇADOR': data['CAÇADOR'] || [],
    PESCADOR: data.PESCADOR || [],
    BALEEIRO: data.BALEEIRO || []
  };
}

async function saveUserWatchlist(tokenHash, patch) {
  await db.collection('user_watchlists').doc(tokenHash).set({
    ...patch,
    atualizadoEm: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
}

async function getAllUserWatchlists() {
  if (!firebaseInitialized) return [];
  const snap = await db.collection('user_watchlists').get();
  return snap.docs.map(d => {
    const data = d.data() || {};
    return {
      tokenHash: d.id,
      email: data.email || null,
      engineActive: !!data.engineActive,
      SNIPER: data.SNIPER || [],
      'CAÇADOR': data['CAÇADOR'] || [],
      PESCADOR: data.PESCADOR || [],
      BALEEIRO: data.BALEEIRO || []
    };
  });
}

function contarAtivosWatchlist(wl) {
  if (!wl) return 0;
  return (wl.SNIPER || []).length
       + (wl['CAÇADOR'] || []).length
       + (wl.PESCADOR || []).length
       + (wl.BALEEIRO || []).length;
}

// ========== ESTADO DE TRADES / PRONTIDÃO ==========
const tradesAbertos = new Map();
const cooldownPosTrade = new Map();
const prontidaoHistorico = new Map();
const prontidaoAtiva = new Set();
const prontidaoForaContagem = new Map();
const COOLDOWN_POS_TRADE_MS = 10 * 60 * 1000;

const prontidaoUltimoEnvio = new Map();
const arrefecimentoUltimoEnvio = new Map();
// ⭐ NOVO — Última PRONTIDAO enviada por SÍMBOLO (independente do modo)
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
  // ⭐ NOVO — Limpa o cooldown global de PRONTIDAO
  for (const [sym, ts] of prontidaoGlobalPorSymbol.entries()) {
    if (agora - ts > 30 * 60 * 1000) prontidaoGlobalPorSymbol.delete(sym);
  }
}, 60 * 1000);

// ========== PERSISTÊNCIA ==========
async function persistTradeOpen(tradeKey, trade) {
  if (!firebaseInitialized) return;
  try {
    await db.collection('open_trades').doc(tradeKey).set({
      ...trade,
      atualizadoEm: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
  } catch (err) { logger.error('Erro persistTradeOpen:', err.message); }
}

async function persistTradeUpdate(tradeKey, trade) {
  if (!firebaseInitialized) return;
  try {
    await db.collection('open_trades').doc(tradeKey).set({
      currentPrice: trade.currentPrice,
      avisoSeguindoEnviado: trade.avisoSeguindoEnviado,
      avisoZeroRiscoEnviado: trade.avisoZeroRiscoEnviado,
      avisoQuaseLaEnviado: trade.avisoQuaseLaEnviado,
      avisoAceleracaoEnviado: trade.avisoAceleracaoEnviado,
      avisoTempoEsgotadoEnviado: trade.avisoTempoEsgotadoEnviado,
      aviso5MinEnviado: trade.aviso5MinEnviado,
      timeoutAt: trade.timeoutAt,
      extensoes: trade.extensoes,
      percentualNoUltimoCheck: trade.percentualNoUltimoCheck,
      atualizadoEm: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
  } catch (err) { logger.error('Erro persistTradeUpdate:', err.message); }
}

async function removePersistedTrade(tradeKey) {
  if (!firebaseInitialized) return;
  try { await db.collection('open_trades').doc(tradeKey).delete(); }
  catch (err) { logger.error('Erro removePersistedTrade:', err.message); }
}

async function persistCooldown(tradeKey, expiresAt) {
  if (!firebaseInitialized) return;
  try {
    await db.collection('cooldowns').doc(tradeKey).set({
      expiresAt, atualizadoEm: admin.firestore.FieldValue.serverTimestamp()
    });
  } catch (err) { logger.error('Erro persistCooldown:', err.message); }
}

async function removePersistedCooldown(tradeKey) {
  if (!firebaseInitialized) return;
  try { await db.collection('cooldowns').doc(tradeKey).delete(); }
  catch (err) { logger.error('Erro removePersistedCooldown:', err.message); }
}

async function persistProntidao(tradeKey, historico, ativa) {
  if (!firebaseInitialized) return;
  try {
    await db.collection('prontidao_state').doc(tradeKey).set({
      historico: historico || [],
      ativa: !!ativa,
      atualizadoEm: admin.firestore.FieldValue.serverTimestamp()
    });
  } catch (err) { logger.error('Erro persistProntidao:', err.message); }
}

async function removePersistedProntidao(tradeKey) {
  if (!firebaseInitialized) return;
  try { await db.collection('prontidao_state').doc(tradeKey).delete(); }
  catch (err) { logger.error('Erro removePersistedProntidao:', err.message); }
}

async function loadStateFromFirestore() {
  if (!firebaseInitialized) {
    logger.warn('⏭️ loadStateFromFirestore: Firebase indisponível, a saltar.');
    return;
  }
  try {
    const tradesSnap = await db.collection('open_trades').get();
    const agora = Date.now();
    let tradesRestaurados = 0, tradesExpirados = 0;
     for (const doc of tradesSnap.docs) {
      const t = doc.data();
      const timestampTrade = t.timestamp || 0;
      const timeoutModo = getTimeoutModo(t.mode);
      const timeoutAt = t.timeoutAt || (timestampTrade + timeoutModo);

      if (agora > timeoutAt && !t.timeoutAt) {
        t.timeoutAt = agora + 60 * 1000;
      } else if (agora > timeoutAt) {
        t.timeoutAt = agora + 60 * 1000;
      }
      if (!t.timeoutAt) t.timeoutAt = timestampTrade + timeoutModo;
      tradesAbertos.set(doc.id, t);
      tradesRestaurados++;
    }

    const cdSnap = await db.collection('cooldowns').get();
    let cdRestaurados = 0, cdExpirados = 0;
    for (const doc of cdSnap.docs) {
      const c = doc.data();
      if (c.expiresAt && c.expiresAt > agora) { cooldownPosTrade.set(doc.id, c.expiresAt); cdRestaurados++; }
      else { await doc.ref.delete(); cdExpirados++; }
    }

    const prSnap = await db.collection('prontidao_state').get();
    let prRestaurados = 0;
    for (const doc of prSnap.docs) {
      const p = doc.data();
      if (Array.isArray(p.historico) && p.historico.length > 0) prontidaoHistorico.set(doc.id, p.historico);
      if (p.ativa) prontidaoAtiva.add(doc.id);
      prRestaurados++;
    }

    logger.info(`♻️ Estado restaurado: ${tradesRestaurados} trade(s), ${cdRestaurados} cooldown(s), ${prRestaurados} prontidão(ões) · ${tradesExpirados} trade(s) expirado(s), ${cdExpirados} cooldown(s) expirado(s)`);
  } catch (err) {
    logger.error('Erro ao carregar estado do Firestore:', err.message);
  }
}

function cleanSymbolName(symbol) {
  let nome = symbol.replace('frx', '').replace('cry', '').replace('OTC_', '');
  if (nome.length === 6) nome = nome.slice(0, 3) + '/' + nome.slice(3);
  if (symbol.includes('XAU')) nome = 'XAU/USD';
  if (symbol.includes('XAG')) nome = 'XAG/USD';
  return nome;
}

function extrairDirecaoPrep(dados) {
  const nota = dados.consolidated.primaryTrendNote || '';
  const matchNota = nota.match(/Tendência primária \([^)]+\):\s*(ALTA|BAIXA)/i);
  if (matchNota) return matchNota[1].toUpperCase() === 'ALTA' ? 'CALL' : 'PUT';

  const razaoTrend = (dados.consolidated.score_reasons || []).find(r => r.includes('🧭'));
  if (razaoTrend) {
    if (/Tendência de fundo:\s*ALTA|Tendência assumida:\s*(UP|ALTA)|reversão para UP/i.test(razaoTrend)) return 'CALL';
    if (/Tendência de fundo:\s*BAIXA|Tendência assumida:\s*(DOWN|BAIXA)|reversão para DOWN/i.test(razaoTrend)) return 'PUT';
  }

  const sinais = [];
  for (const tf of ['m1_timing', 'm5_timing', 'm15_timing', 'h1_timing']) {
    const s = dados.consolidated[tf]?.sinal;
    if (s === 'PUT' || s === 'CALL') sinais.push(s);
  }
  if (sinais.length > 0) {
    const puts = sinais.filter(s => s === 'PUT').length;
    if (puts !== sinais.length - puts) return puts > sinais.length - puts ? 'PUT' : 'CALL';
  }
  return null;
}

function diagnosticoProximidade(reasons) {
  const texto = (reasons || []).join(' ');
  if (/ADX muito fraco/i.test(texto)) return { nivel: 'LONGE', detalhe: 'macro sem força' };
  if (/CONFLITO|pullback em curso|aguarda histograma|aguarda alinhamento/i.test(texto)) return { nivel: 'PERTO', detalhe: 'gatilho em ajuste' };
  return { nivel: 'FORMACAO', detalhe: 'aguardando alinhamento' };
}

// ========== FORMATAÇÃO DE MENSAGENS ==========
function formatarMensagemPrep(symbol, direcao, dados, extras = {}) {
  const nomeAmigavel = fullAssets[symbol] || cleanSymbolName(symbol);
  const dirLabel = direcao === 'CALL' ? 'COMPRA (CALL)' : 'VENDA (PUT)';
  const score = dados.consolidated.score;
  const reasons = (dados.consolidated.score_reasons || []).join(' ');
  let proximidade, detalhe;
  if (/ADX muito fraco/i.test(reasons)) { proximidade = 'LONGE'; detalhe = 'tendência macro sem força'; }
  else if (/CONFLITO|pullback em curso|aguarda histograma|aguarda alinhamento/i.test(reasons)) { proximidade = 'PERTO'; detalhe = 'gatilho em ajuste'; }
  else { proximidade = 'EM FORMAÇÃO'; detalhe = 'aguardando alinhamento'; }

  return {
    titulo: `👀 Atenção: ${cleanSymbolName(symbol)}`,
    corpo: `${dirLabel} · ${nomeAmigavel}\n⚡ Score ${score}/100 · Zona B · ${proximidade}\n💡 ${detalhe}`,
    detalhes: {
      tipo: 'PRONTIDAO', nomeAmigavel, direcao, modo: extras.mode || null,
      score, zona: 'B', proximidade, detalhe,
      reasons: (dados.consolidated.score_reasons || []).slice(0, 8),
      price: dados.consolidated.price
    }
  };
}

function formatarMensagemArrefecimento(symbol, score, dados) {
  const nomeAmigavel = fullAssets[symbol] || cleanSymbolName(symbol);
  return {
    titulo: `😴 Prontidão encerrada: ${cleanSymbolName(symbol)}`,
    corpo: `${nomeAmigavel}\n📉 Score atual ${score}/100 · saiu da Zona B\n✅ A espera anterior foi cancelada`,
    detalhes: {
      tipo: 'ARREFECIMENTO', nomeAmigavel, score,
      motivo: 'Setup perdeu força e saiu da Zona B',
      reasons: (dados?.consolidated?.score_reasons || []).slice(0, 5)
    }
  };
}

function formatarMensagemSinal(dados, mode) {
  const { symbol, consolidated, suggestion } = dados;
  const emoji = consolidated.signal === 'CALL' ? '🟢' : '🔴';
  const dirLabel = consolidated.signal === 'CALL' ? 'COMPRA (CALL)' : 'VENDA (PUT)';
  const nomeAmigavel = fullAssets[symbol] || cleanSymbolName(symbol);
  const conf = (consolidated.confidence * 100).toFixed(1);

  return {
    titulo: `🚨 SINAL CONFIRMADO: ${cleanSymbolName(symbol)}`,
    corpo: `${emoji} ${dirLabel} · ${nomeAmigavel}\n💰 Entrada ${suggestion.entry} · 🎯 TP ${suggestion.takeProfit} · 🛑 SL ${suggestion.stopLoss}\n⚡ Score ${consolidated.score}/100 · Confiança ${conf}%`,
    detalhes: {
      tipo: 'SINAL_CONFIRMADO', nomeAmigavel, direcao: consolidated.signal, modo: mode,
      entry: suggestion.entry, takeProfit: suggestion.takeProfit, stopLoss: suggestion.stopLoss,
      score: consolidated.score, confidence: conf, price: consolidated.price,
      reasons: (consolidated.score_reasons || []).slice(0, 8),
      zona: 'A'
    }
  };
}

function formatarMensagem5Min(trade) {
  const nome = fullAssets[trade.symbol] || cleanSymbolName(trade.symbol);
  return {
    titulo: `⏱️ Atualização (5min): ${cleanSymbolName(trade.symbol)}`,
    corpo: `${nome} · ${trade.signal}\n💵 Preço atual ${trade.currentPrice} (entrada ${trade.entry})\n📈 Mantém a posição — trade em curso`,
    detalhes: {
      tipo: '5MIN', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode,
      entry: trade.entry, currentPrice: trade.currentPrice,
      takeProfit: trade.takeProfit, stopLoss: trade.stopLoss
    }
  };
}

function formatarMensagemAceleracao(trade) {
  const nome = fullAssets[trade.symbol] || cleanSymbolName(trade.symbol);
  return {
    titulo: `🚀 Mercado acelerando: ${cleanSymbolName(trade.symbol)}`,
    corpo: `${nome} · ${trade.signal}\n💵 Preço ${trade.currentPrice} — movimento forte\n🎯 Deixe correr até o TP ${trade.takeProfit}`,
    detalhes: {
      tipo: 'ACELERACAO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode,
      entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit
    }
  };
}

function formatarMensagemSeguindo(trade) {
  const nome = fullAssets[trade.symbol] || cleanSymbolName(trade.symbol);
  return {
    titulo: `✅ Seguindo o sinal: ${cleanSymbolName(trade.symbol)}`,
    corpo: `${nome} · ${trade.signal}\n💵 Preço ${trade.currentPrice} · tendência confirmada\n📊 +30% do alvo percorrido`,
    detalhes: {
      tipo: 'SEGUINDO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode,
      entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit
    }
  };
}

function formatarMensagemZeroRisco(trade) {
  const nome = fullAssets[trade.symbol] || cleanSymbolName(trade.symbol);
  return {
    titulo: `🛡️ Zero Risco: ${cleanSymbolName(trade.symbol)}`,
    corpo: `${nome} · ${trade.signal}\n✅ +50% do alvo — move SL para a entrada\n🎯 Entrada ${trade.entry} (proteção ativa)`,
    detalhes: {
      tipo: 'ZERO_RISCO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode,
      entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit
    }
  };
}

function formatarMensagemQuaseLa(trade) {
  const nome = fullAssets[trade.symbol] || cleanSymbolName(trade.symbol);
  return {
    titulo: `⏳ Quase no alvo: ${cleanSymbolName(trade.symbol)}`,
    corpo: `${nome} · ${trade.signal}\n💵 Preço ${trade.currentPrice} · 🎯 Alvo ${trade.takeProfit}\n📊 +80% percorrido — atenção máxima`,
    detalhes: {
      tipo: 'QUASE_LA', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode,
      entry: trade.entry, currentPrice: trade.currentPrice, takeProfit: trade.takeProfit
    }
  };
}

function formatarMensagemWin(trade) {
  const nome = fullAssets[trade.symbol] || cleanSymbolName(trade.symbol);
  const duracao = Math.floor((Date.now() - trade.timestamp) / 60000);
  return {
    titulo: `🎯 WIN: ${cleanSymbolName(trade.symbol)}`,
    corpo: `${nome} · ${trade.signal} ✅\n💰 Alvo ${trade.takeProfit} atingido!\n⏱️ Duração: ${duracao}min · fecha a posição`,
    detalhes: {
      tipo: 'WIN', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode,
      entry: trade.entry, takeProfit: trade.takeProfit, currentPrice: trade.currentPrice,
      duracao: duracao + 'min'
    }
  };
}

function formatarMensagemStop(trade) {
  const nome = fullAssets[trade.symbol] || cleanSymbolName(trade.symbol);
  const duracao = Math.floor((Date.now() - trade.timestamp) / 60000);
  return {
    titulo: `🛑 Stop Loss: ${cleanSymbolName(trade.symbol)}`,
    corpo: `${nome} · ${trade.signal}\n📉 Preço ${trade.currentPrice} bateu SL ${trade.stopLoss}\n⏱️ Duração: ${duracao}min · fecha a posição`,
    detalhes: {
      tipo: 'STOP', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode,
      entry: trade.entry, stopLoss: trade.stopLoss, currentPrice: trade.currentPrice,
      duracao: duracao + 'min'
    }
  };
}

function formatarMensagemTempoEsgotado(trade) {
  const nome = fullAssets[trade.symbol] || cleanSymbolName(trade.symbol);
  const duracao = Math.floor((Date.now() - trade.timestamp) / 60000);
  return {
    titulo: `⏱️ Tempo esgotado: ${cleanSymbolName(trade.symbol)}`,
    corpo: `${nome} · ${trade.signal}\n💵 Preço perto da entrada (${trade.currentPrice})\n✅ Considera fechar no breakeven`,
    detalhes: {
      tipo: 'TEMPO_ESGOTADO', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode,
      entry: trade.entry, currentPrice: trade.currentPrice,
      takeProfit: trade.takeProfit, stopLoss: trade.stopLoss, duracao: duracao + 'min'
    }
  };
}

function formatarMensagemTimeout(trade, currentPrice, tempoDecorridoMin, motivo) {
  const nome = fullAssets[trade.symbol] || cleanSymbolName(trade.symbol);
  const distanciaTotal = Math.abs(trade.takeProfit - trade.entry);
  const distanciaPercorrida = distanciaTotal > 0 ? (trade.signal === 'CALL' ? (currentPrice - trade.entry) : (trade.entry - currentPrice)) : 0;
  const pct = distanciaTotal > 0 ? ((distanciaPercorrida / distanciaTotal) * 100).toFixed(1) : '0.0';

  return {
    titulo: `🕐 Encerrado por timeout: ${cleanSymbolName(trade.symbol)}`,
    corpo: `${nome} · ${trade.signal}\n⏱️ ${tempoDecorridoMin}min sem avanço suficiente (${pct}% do alvo)\n❌ Motivo: ${motivo} · fecha a posição`,
    detalhes: {
      tipo: 'TIMEOUT', nomeAmigavel: nome, direcao: trade.signal, modo: trade.mode,
      entry: trade.entry, currentPrice, takeProfit: trade.takeProfit, stopLoss: trade.stopLoss,
      duracao: tempoDecorridoMin + 'min',
      motivo,
      percentualPercorrido: pct + '%',
      extensoes: trade.extensoes || 0
    }
  };
}

async function registrarEEnviarSinal(symbol, mode, tipo, msg, extra = {}, watchers = []) {
  const { titulo, corpo, detalhes } = msg;
  logger.info(`[SINAL] ${symbol} (${mode}) [${tipo}] ${titulo} — ${corpo} · ${watchers.length} watcher(s)`);
  if (firebaseInitialized) {
    try {
      await db.collection('signals').add({
        symbol, mode, tipo, titulo, corpo,
        detalhes: detalhes || null,
        ...extra,
        watchers,
        origem: 'motor',
        criadoEm: admin.firestore.FieldValue.serverTimestamp()
      });
    } catch (err) { logger.error('Erro ao gravar sinal:', err.message); }
  }
  await sendPushToWatchers(watchers, {
    title: titulo,
    body: corpo,
    tag: `${symbol}_${mode}_${tipo}`,
    data: { symbol, mode, tipo, url: '/' }
  });
}

// ========== ANÁLISE ==========
async function buscarSinalAnalise(symbol, mode) {
  const API_URL = process.env.ANALYSIS_API_URL || 'http://localhost:3001';
  const adminKey = process.env.ADMIN_SECRET;
  if (!adminKey) {
    logger.error('❌ ADMIN_SECRET não configurado!');
    return null;
  }
  try {
    const response = await fetch(`${API_URL}/analyze`, {
      method: 'POST',
      headers: { 'x-admin-key': adminKey, 'Content-Type': 'application/json' },
      body: JSON.stringify({ symbol, mode })
    });
    if (!response.ok) {
      logger.error(`❌ Erro HTTP ${response.status} ao buscar ${symbol}:`, await response.text());
      return null;
    }
    return await response.json();
  } catch (err) {
    logger.error(`❌ Erro de conexão ao buscar análise para ${symbol}:`, err.message);
    return null;
  }
}

// ⭐ Reenvia o SINAL_CONFIRMADO 30s depois para garantir entrega em caso de hibernação
async function _reenviarSinalConfirmado(symbol, mode, tradeKey, watchers) {
  setTimeout(async () => {
    try {
      const t = tradesAbertos.get(tradeKey);
      if (!t) return;
      if (Date.now() - t.timestamp > 2 * 60 * 1000) return;

      await sendPushToWatchers(watchers, {
        title: `🚨 Lembrete: ${cleanSymbolName(symbol)}`,
        body: `Sinal ${t.signal} ainda ativo — entrada ${t.entry} · TP ${t.takeProfit} · SL ${t.stopLoss}\n⚡ Se não recebeste o sinal anterior, entra agora`,
        tag: `${symbol}_${mode}_SINAL_CONFIRMADO_RETRY`,
        data: { symbol, mode, tipo: 'SINAL_CONFIRMADO_RETRY', url: '/' }
      });
      logger.info(`🔁 [SINAL_CONFIRMADO_RETRY] ${symbol} (${mode}) reenviado após 30s`);
    } catch (err) {
      logger.error('Erro no retry do SINAL_CONFIRMADO:', err.message);
    }
  }, 30 * 1000);
}

async function analisarEEnviarSinais(symbol, mode, watchers = []) {
  const dados = await buscarSinalAnalise(symbol, mode);
  if (!dados || !dados.success) return;

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

    const distanciaTotal = Math.abs(trade.takeProfit - trade.entry);
    const distanciaPercorrida = distanciaTotal > 0 ? (trade.signal === 'CALL' ? (currentPrice - trade.entry) : (trade.entry - currentPrice)) : 0;
    const percentualPercorrido = distanciaTotal > 0 ? (distanciaPercorrida / distanciaTotal) : 0;
    const tempoDecorridoMin = Math.floor((agora - trade.timestamp) / 60000);

    const timeoutModo = getTimeoutModo(trade.mode);
    const timeoutExtendModo = getTimeoutExtendModo(trade.mode);
    const timeoutAtual = trade.timeoutAt || (trade.timestamp + timeoutModo);
    if (agora > timeoutAtual) {
      const extensoes = trade.extensoes || 0;
      const progressoAnterior = trade.percentualNoUltimoCheck || 0;
      const progressoNovo = percentualPercorrido - progressoAnterior;
      const estaAvancando = progressoNovo >= PROGRESSO_MINIMO_EXTENSAO;

      if (estaAvancando && extensoes < EXTENSOES_MAX) {
        trade.timeoutAt = agora + timeoutExtendModo;
        trade.extensoes = extensoes + 1;
        trade.percentualNoUltimoCheck = percentualPercorrido;
        logger.info(`⏭️ Trade ${tradeKey} [${trade.mode}] estendido (${trade.extensoes}/${EXTENSOES_MAX}) — progresso ${(percentualPercorrido*100).toFixed(1)}% (+${(progressoNovo*100).toFixed(1)}%)`);
        persistTradeUpdate(tradeKey, trade);
      } else {
        const motivo = extensoes >= EXTENSOES_MAX
          ? `limite de ${EXTENSOES_MAX} extensões atingido`
          : `sem avanço suficiente nas últimas ${Math.floor(timeoutExtendModo/60000)}min`;

        await registrarEEnviarSinal(
          symbol, mode, 'TIMEOUT',
          formatarMensagemTimeout(trade, currentPrice, tempoDecorridoMin, motivo),
          { score: dados.consolidated.score },
          watchers
        );

        tradesAbertos.delete(tradeKey);
        removePersistedTrade(tradeKey);
        cooldownPosTrade.set(tradeKey, agora + COOLDOWN_POS_TRADE_MS);
        persistCooldown(tradeKey, agora + COOLDOWN_POS_TRADE_MS);
        return;
      }
    }

    let msgObj = null;
    let tipo = null;
    let fecharTrade = false;

    if (trade.signal === 'CALL' && currentPrice >= trade.takeProfit) { msgObj = formatarMensagemWin(trade); tipo = 'WIN'; fecharTrade = true; }
    else if (trade.signal === 'PUT' && currentPrice <= trade.takeProfit) { msgObj = formatarMensagemWin(trade); tipo = 'WIN'; fecharTrade = true; }
    else if (trade.signal === 'CALL' && currentPrice <= trade.stopLoss) { msgObj = formatarMensagemStop(trade); tipo = 'STOP'; fecharTrade = true; }
    else if (trade.signal === 'PUT' && currentPrice >= trade.stopLoss) { msgObj = formatarMensagemStop(trade); tipo = 'STOP'; fecharTrade = true; }
    else if (!trade.avisoTempoEsgotadoEnviado && tempoDecorridoMin >= 10 && percentualPercorrido < 0.15) { msgObj = formatarMensagemTempoEsgotado(trade); tipo = 'TEMPO_ESGOTADO'; trade.avisoTempoEsgotadoEnviado = true; fecharTrade = true; }
    else if (!trade.avisoAceleracaoEnviado && tempoDecorridoMin <= 2 && percentualPercorrido >= 0.40) { msgObj = formatarMensagemAceleracao(trade); tipo = 'ACELERACAO'; trade.avisoAceleracaoEnviado = true; trade.avisoSeguindoEnviado = true; }
    else if (!trade.avisoSeguindoEnviado && percentualPercorrido >= 0.30) { msgObj = formatarMensagemSeguindo(trade); tipo = 'SEGUINDO'; trade.avisoSeguindoEnviado = true; }
    else if (!trade.aviso5MinEnviado && tempoDecorridoMin >= 5 && percentualPercorrido > 0.10 && percentualPercorrido < 0.50) { msgObj = formatarMensagem5Min(trade); tipo = '5MIN'; trade.aviso5MinEnviado = true; }
    else if (!trade.avisoZeroRiscoEnviado && percentualPercorrido >= 0.50) { msgObj = formatarMensagemZeroRisco(trade); tipo = 'ZERO_RISCO'; trade.avisoZeroRiscoEnviado = true; }
    else if (!trade.avisoQuaseLaEnviado && percentualPercorrido >= 0.80) { msgObj = formatarMensagemQuaseLa(trade); tipo = 'QUASE_LA'; trade.avisoQuaseLaEnviado = true; }

    if (fecharTrade) {
      tradesAbertos.delete(tradeKey);
      removePersistedTrade(tradeKey);
      cooldownPosTrade.set(tradeKey, agora + COOLDOWN_POS_TRADE_MS);
      persistCooldown(tradeKey, agora + COOLDOWN_POS_TRADE_MS);
    } else {
      persistTradeUpdate(tradeKey, trade);
    }

    if (msgObj) {
      await registrarEEnviarSinal(symbol, mode, tipo, msgObj, { score: dados.consolidated.score }, watchers);
    }
    return;
  }

  // ⭐ Anti-duplicado
  if (firebaseInitialized) {
    try {
      const cincoMinAtras = new Date(Date.now() - 5 * 60 * 1000);
      const snap = await db.collection('signals')
        .where('symbol', '==', symbol)
        .where('mode', '==', mode)
        .where('tipo', '==', 'SINAL_CONFIRMADO')
        .where('criadoEm', '>=', cincoMinAtras)
        .limit(1)
        .get();
      if (!snap.empty) {
        logger.info(`⏭️ Anti-duplicado: sinal já emitido nos últimos 5min para ${symbol}/${mode} — a saltar`);
        return;
      }
    } catch (err) {
      logger.warn('Anti-duplicado indisponível (índice?):', err.message);
    }
  }

  if (dados.consolidated.signal !== 'HOLD' && dados.consolidated.zona === 'A') {
    if (dados.suggestion && dados.suggestion.action === 'ENTRADA' &&
        dados.suggestion.entry != null && dados.suggestion.takeProfit != null && dados.suggestion.stopLoss != null) {
      const novoTrade = {
        symbol,
        mode,
        signal: dados.consolidated.signal,
        entry: dados.suggestion.entry,
        takeProfit: dados.suggestion.takeProfit,
        stopLoss: dados.suggestion.stopLoss,
        watchers: [...watchers],
        timestamp: agora,
        avisoSeguindoEnviado: false,
        avisoZeroRiscoEnviado: false,
        avisoQuaseLaEnviado: false,
        avisoAceleracaoEnviado: false,
        avisoTempoEsgotadoEnviado: false,
        aviso5MinEnviado: false,
        timeoutAt: agora + getTimeoutModo(mode),
        extensoes: 0,
        percentualNoUltimoCheck: 0
      };
     
      tradesAbertos.set(tradeKey, novoTrade);
      persistTradeOpen(tradeKey, novoTrade);

          logger.info(`🚀 [ENTRAR AGORA] ${symbol} (${mode}) → ${dados.consolidated.signal} @ ${dados.suggestion.entry} | TP ${dados.suggestion.takeProfit} | SL ${dados.suggestion.stopLoss} | score ${dados.consolidated.score}`);

      await registrarEEnviarSinal(symbol, mode, 'SINAL_CONFIRMADO', formatarMensagemSinal(dados, mode), {
        score: dados.consolidated.score,
        confidence: dados.consolidated.confidence,
        entry: dados.suggestion.entry,
        takeProfit: dados.suggestion.takeProfit,
        stopLoss: dados.suggestion.stopLoss
      }, watchers);

      _reenviarSinalConfirmado(symbol, mode, tradeKey, watchers);   // ⭐ NOVO — retry após 30s

      prontidaoAtiva.delete(tradeKey);
      prontidaoHistorico.delete(tradeKey);
      prontidaoForaContagem.delete(tradeKey);
      prontidaoUltimoEnvio.delete(tradeKey);
      arrefecimentoUltimoEnvio.delete(tradeKey);
      removePersistedProntidao(tradeKey);
    }
  }
  else if (dados.consolidated.signal === 'HOLD' && dados.consolidated.zona === 'B') {
    prontidaoForaContagem.delete(tradeKey);
    const historico = prontidaoHistorico.get(tradeKey) || [];
    historico.push({ score: dados.consolidated.score, t: agora });
    if (historico.length > 5) historico.shift();
    prontidaoHistorico.set(tradeKey, historico);
    persistProntidao(tradeKey, historico, prontidaoAtiva.has(tradeKey));

    const cfg = getProntidaoConfig(mode);
    const ultimoEnvio = prontidaoUltimoEnvio.get(tradeKey) || 0;
    const podeEnviarAgora = (agora - ultimoEnvio) >= cfg.cooldownMs;

    // ⭐ NOVO — Verifica cooldown GLOBAL (mesmo símbolo noutros modos)
    const ultimoGlobal = prontidaoGlobalPorSymbol.get(symbol) || 0;
    const podeEnviarGlobal = (agora - ultimoGlobal) >= PRONTIDAO_GLOBAL_COOLDOWN_MS;

    // Só envia PRONTIDAO se: (a) nunca foi enviado OU (b) já passou o cooldown do modo
    // E também (c) já passou o cooldown global do símbolo
    if ((!prontidaoAtiva.has(tradeKey) || podeEnviarAgora) && podeEnviarGlobal) {
      const direcaoPrep = extrairDirecaoPrep(dados);
      if (direcaoPrep) {
        const subindo = historico.length >= 3 && historico[historico.length - 1].score > historico[0].score;
        await registrarEEnviarSinal(
          symbol, mode, 'PRONTIDAO',
          formatarMensagemPrep(symbol, direcaoPrep, dados, { subindo, historico, mode }),
          { score: dados.consolidated.score },
          watchers
        );
        prontidaoAtiva.add(tradeKey);
        prontidaoUltimoEnvio.set(tradeKey, agora);
        prontidaoGlobalPorSymbol.set(symbol, agora);   // ⭐ NOVO
        prontidaoForaContagem.set(tradeKey, 0);
        persistProntidao(tradeKey, prontidaoHistorico.get(tradeKey), true);
      }
    }
  }
  else if (dados.consolidated.signal === 'HOLD' && prontidaoAtiva.has(tradeKey)) {
    const cfg = getProntidaoConfig(mode);
    const fora = (prontidaoForaContagem.get(tradeKey) || 0) + 1;
    prontidaoForaContagem.set(tradeKey, fora);

    if (fora >= cfg.ciclosForaParaArrefecer) {
      const ultimoArref = arrefecimentoUltimoEnvio.get(tradeKey) || 0;
      const cooldownArrefMs = cfg.cooldownMs;
      if ((agora - ultimoArref) >= cooldownArrefMs) {
        await registrarEEnviarSinal(
          symbol, mode, 'ARREFECIMENTO',
          formatarMensagemArrefecimento(symbol, dados.consolidated.score, dados),
          { score: dados.consolidated.score },
          watchers
        );
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
          symbol: tradeKey.slice(0, idx),
          mode: tradeKey.slice(idx + 1)
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
        watchers = activeUsers
          .filter(u => (u[mode] || []).includes(symbol))
          .map(u => u.tokenHash);
      }

      await analisarEEnviarSinais(symbol, mode, watchers);
      await new Promise(r => setTimeout(r, 1200));
    }
  } catch (err) {
    logger.error('Erro no cron:', err.message);
  } finally {
    cronEmExecucao = false;
  }
}, { timezone: 'America/Sao_Paulo' });

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
  WLDAUD: 'Dólar Australiano (Cesta)', WLDEUR: 'Euro (Cesta)', WLDGBP: 'Libra Esterlina (Cesta)', WLDXAU: 'Ouro (Cesta)', WLDUSD: 'Dólar Americano (Cesta)',
  frxAUDCAD: 'AUD/CAD', frxAUDCHF: 'AUD/CHF', frxAUDJPY: 'AUD/JPY', frxAUDNZD: 'AUD/NZD', frxAUDUSD: 'AUD/USD', frxEURCAD: 'EUR/CAD', frxEURCHF: 'EUR/CHF', frxEURAUD: 'EUR/AUD', frxEURGBP: 'EUR/GBP', frxEURJPY: 'EUR/JPY', frxEURNZD: 'EUR/NZD', frxEURUSD: 'EUR/USD', frxGBPAUD: 'GBP/AUD', frxGBPCAD: 'GBP/CAD', frxGBPCHF: 'GBP/CHF', frxGBPJPY: 'GBP/JPY', frxGBPNOK: 'GBP/NOK', frxGBPNZD: 'GBP/NZD', frxGBPUSD: 'GBP/USD', frxNZDJPY: 'NZD/JPY', frxNZDUSD: 'NZD/USD', frxUSDCAD: 'USD/CAD', frxUSDCHF: 'USD/CHF', frxUSDJPY: 'USD/JPY', frxUSDMXN: 'USD/MXN', frxUSDNOK: 'USD/NOK', frxUSDPLN: 'USD/PLN', frxUSDSEK: 'USD/SEK', frxGBPPLN: 'GBP/PLN',
  frxXAUUSD: 'XAU/USD', frxXAGUSD: 'XAG/USD', frxXPDUSD: 'XPD/USD', frxXPTUSD: 'XPT/USD',
  RDBEAR: 'RD Bear', RDBULL: 'RD Bull', RB100: 'RB 100', RB200: 'RB 200', stpRNG: 'STP RNG', stpRNG2: 'STP RNG 2', stpRNG3: 'STP RNG 3', stpRNG4: 'STP RNG 4', stpRNG5: 'STP RNG 5', R_10: 'R_10', R_25: 'R_25', R_50: 'R_50', R_75: 'R_75', R_90: 'R_90', R_100: 'R_100', '1HZ10V': '1HZ 10V', '1HZ15V': '1HZ 15V', '1HZ25V': '1HZ 25V', '1HZ30V': '1HZ 30V', '1HZ50V': '1HZ 50V', '1HZ75V': '1HZ 75V', '1HZ90V': '1HZ 90V', '1HZ100V': '1HZ 100V', '1HZ150V': '1HZ 150V', '1HZ250V': '1HZ 250V',
  OTC_AS51: 'OTC AS51', OTC_SX5E: 'OTC SX5E', OTC_FCHI: 'OTC FCHI', OTC_GDAXI: 'OTC GDAXI', OTC_AEX: 'OTC AEX', OTC_FTSE: 'OTC FTSE', OTC_SPC: 'OTC SPC', OTC_NDX: 'OTC NDX', OTC_DJI: 'OTC DJI', OTC_HSI: 'OTC HSI', OTC_N225: 'OTC N225', OTC_SSMI: 'OTC SSMI',
  cryBTCUSD: 'BTC/USD', cryETHUSD: 'ETH/USD', cryLTCUSD: 'LTC/USD', cryBCHUSD: 'BCH/USD', cryBNBUSD: 'BNB/USD', cryDSHUSD: 'DSH/USD', cryIOTUSD: 'IOT/USD', cryNEOUSD: 'NEO/USD', cryTRXUSD: 'TRX/USD', cryXLMUSD: 'XLM/USD', cryXMRUSD: 'XMR/USD', cryXRPUSD: 'XRP/USD', cryZECUSD: 'ZEC/USD', cryBTCETH: 'BTC/ETH', cryBTCLTC: 'BTC/LTC'
};

// ========== API ENDPOINTS ==========

app.get('/health', (req, res) => res.json({
  status: 'ok',
  uptime: Math.floor(process.uptime()),
  tradesAbertos: tradesAbertos.size,
  cooldowns: cooldownPosTrade.size,
  prontidoes: prontidaoAtiva.size
}));

app.get('/api/vapid-public-key', (req, res) => {
  if (!pushConfigured) return res.status(503).json({ error: 'Push não configurado no servidor' });
  res.json({ publicKey: VAPID_PUBLIC_KEY });
});

app.post('/api/push/subscribe', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) return res.status(503).json({ error: 'Firestore indisponível' });
  const { subscription } = req.body;
  if (!subscription || !subscription.endpoint) return res.status(400).json({ error: 'Subscrição inválida' });
  try {
    const docId = Buffer.from(subscription.endpoint).toString('base64').replace(/[/+=]/g, '_').slice(0, 400);
    await db.collection('push_subscriptions').doc(docId).set({
      subscription,
      tokenHash: req.user.tokenHash,
      email: req.user.email || null,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/push/unsubscribe', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) return res.status(503).json({ error: 'Firestore indisponível' });
  const { endpoint } = req.body;
  if (!endpoint) return res.status(400).json({ error: 'endpoint obrigatório' });
  try {
    const docId = Buffer.from(endpoint).toString('base64').replace(/[/+=]/g, '_').slice(0, 400);
    await db.collection('push_subscriptions').doc(docId).delete();
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/push/test', authMiddleware, async (req, res) => {
  await sendPushToWatchers([req.user.tokenHash], {
    title: '🔔 Teste',
    body: 'Notificações a funcionar corretamente!',
    tag: 'teste_' + Date.now()
  });
  res.json({ success: true });
});

app.get('/api/user-me', authMiddleware, (req, res) => {
  res.json({
    tokenHash: req.user.tokenHash,
    email: req.user.email,
    name: req.user.name,
    periodDays: req.user.periodDays,
    plano: req.user.plano,
    maxAtivosPorModo: req.user.plano?.maxAtivosPorModo ?? 10
  });
});

app.get('/api/engine-config', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) {
    return res.json({ active: false, watchlist: { SNIPER: [], 'CAÇADOR': [], PESCADOR: [], BALEEIRO: [] } });
  }
  try {
    const wl = await getUserWatchlist(req.user.tokenHash);
    res.json({
      active: wl.engineActive,
      plano: req.user.plano,
      maxAtivosPorModo: req.user.plano?.maxAtivosPorModo ?? 10,
      watchlist: { SNIPER: wl.SNIPER, 'CAÇADOR': wl['CAÇADOR'], PESCADOR: wl.PESCADOR, BALEEIRO: wl.BALEEIRO }
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/engine-start', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) return res.status(503).json({ error: 'Firestore indisponível' });
  try {
    const wl = await getUserWatchlist(req.user.tokenHash);
    const totalAtivos = contarAtivosWatchlist(wl);

    if (totalAtivos === 0) {
      logger.warn(`⚠️ [ENGINE-START] user=${req.user.tokenHash} tentou ativar motor SEM ativos vigiados`);
      return res.status(400).json({
        success: false,
        error: 'Adiciona pelo menos 1 ativo à watchlist antes de ativar o motor.',
        code: 'WATCHLIST_EMPTY',
        totalAtivos: 0
      });
    }

    await saveUserWatchlist(req.user.tokenHash, { engineActive: true, email: req.user.email || null });
    logger.info(`✅ [ENGINE-START] user=${req.user.tokenHash} ativou motor com ${totalAtivos} ativo(s)`);
    res.json({ success: true, active: true, totalAtivos });
  } catch (err) {
    logger.error('Erro em /api/engine-start:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/engine-stop', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) return res.status(503).json({ error: 'Firestore indisponível' });
  try {
    await saveUserWatchlist(req.user.tokenHash, { engineActive: false });
    res.json({ success: true, active: false });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/engine-watchlist', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) return res.json({ watchlist: { SNIPER: [], 'CAÇADOR': [], PESCADOR: [], BALEEIRO: [] } });
  try {
    const wl = await getUserWatchlist(req.user.tokenHash);
    res.json({ watchlist: { SNIPER: wl.SNIPER, 'CAÇADOR': wl['CAÇADOR'], PESCADOR: wl.PESCADOR, BALEEIRO: wl.BALEEIRO } });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/engine-watchlist', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) return res.status(503).json({ error: 'Firestore indisponível' });
  const { watchlist } = req.body || {};
  if (!watchlist || typeof watchlist !== 'object') return res.status(400).json({ error: 'watchlist deve ser um objeto' });
  try {
    const maxAtivos = req.user.plano?.maxAtivosPorModo ?? 10;

    if (maxAtivos <= 0) {
      return res.status(403).json({
        error: 'A tua conta não tem um plano ativo. Contacta o suporte para ativares o acesso.'
      });
    }

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

    const houveCorte = Object.values(cortado).some(Boolean);
    res.json({
      success: true,
      watchlist: final,
      plano: req.user.plano,
      maxAtivosPorModo: maxAtivos,
      cortado: houveCorte ? cortado : null,
      mensagem: houveCorte
        ? `Plano ${req.user.plano?.nome || 'atual'} permite ${maxAtivos} por modo. Excesso removido.`
        : null
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/engine-watchlist', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) return res.status(503).json({ error: 'Firestore indisponível' });
  try {
    await saveUserWatchlist(req.user.tokenHash, { SNIPER: [], 'CAÇADOR': [], PESCADOR: [], BALEEIRO: [] });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/scan-group', authMiddleware, async (req, res) => {
  const { group, mode } = req.body;
  if (!group || !mode) return res.status(400).json({ error: 'Os campos "group" e "mode" são obrigatórios.' });
  const symbols = assetGroups[group];
  if (!symbols) return res.status(400).json({ error: `Grupo "${group}" não reconhecido.` });
  if (!MODOS_OK.includes(mode)) return res.status(400).json({ error: `Modo "${mode}" inválido. Use um de: ${MODOS_OK.join(', ')}` });

  try {
    const allResults = [];
    for (let i = 0; i < symbols.length; i += 5) {
      const batch = symbols.slice(i, i + 5);
      const batchResults = await Promise.allSettled(batch.map(async (symbol) => {
        const data = await buscarSinalAnalise(symbol, mode);
        if (!data || !data.success) {
          return { symbol, name: fullAssets[symbol] || symbol, signal: 'HOLD', zona: '?', score: 0, reasons: ['Erro ao obter análise'], error: true };
        }
        const consolidated = data.consolidated || {};
        return {
          symbol,
          name: fullAssets[symbol] || symbol,
          signal: consolidated.signal || 'HOLD',
          zona: consolidated.zona || '?',
          score: consolidated.score || 0,
          proximidade: diagnosticoProximidade(consolidated.score_reasons),
          reasons: (consolidated.score_reasons || []).slice(0, 3)
        };
      }));
      allResults.push(...batchResults);
      if (i + 5 < symbols.length) await new Promise(r => setTimeout(r, 800));
    }
    const scanResults = allResults.filter(r => r.status === 'fulfilled').map(r => r.value);
    res.json({ success: true, results: scanResults });
  } catch (err) {
    logger.error('Erro no /api/scan-group:', err.message);
    res.status(500).json({ error: 'Erro interno ao analisar grupo.' });
  }
});

app.get('/api/signals', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) return res.json({ signals: [] });
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    let q = db.collection('signals')
      .where('watchers', 'array-contains', req.user.tokenHash)
      .orderBy('criadoEm', 'desc')
      .limit(limit);
    if (req.query.mode) {
      q = db.collection('signals')
        .where('watchers', 'array-contains', req.user.tokenHash)
        .where('mode', '==', req.query.mode)
        .orderBy('criadoEm', 'desc')
        .limit(limit);
    }
    const snapshot = await q.get();
    const signals = snapshot.docs.map(doc => ({
      id: doc.id,
      ...doc.data(),
      criadoEm: doc.data().criadoEm?.toDate?.() || null
    }));
    res.json({ signals });
  } catch (err) {
    logger.error('Erro ao buscar sinais:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ⭐ NOVO — Limpar histórico de sinais do utilizador (remove tokenHash dos watchers)
app.delete('/api/signals', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) return res.status(503).json({ error: 'Firestore indisponível' });
  const mode = req.query.mode || null;
  try {
    let q = db.collection('signals')
      .where('watchers', 'array-contains', req.user.tokenHash);
    if (mode) q = q.where('mode', '==', mode);

    const snap = await q.get();
    if (snap.empty) {
      return res.json({ success: true, deletados: 0, atualizados: 0, total: 0, mensagem: 'Nada para apagar' });
    }

    let deletados = 0, atualizados = 0;
    let batch = db.batch();
    let ops = 0;
    const commits = [];

    for (const doc of snap.docs) {
      const data = doc.data();
      const watchers = Array.isArray(data.watchers) ? data.watchers : [];
      const restantes = watchers.filter(t => t !== req.user.tokenHash);

      if (restantes.length === 0) {
        batch.delete(doc.ref);
        deletados++;
      } else {
        batch.update(doc.ref, { watchers: restantes });
        atualizados++;
      }
      ops++;

      if (ops >= 450) {
        commits.push(batch.commit());
        batch = db.batch();
        ops = 0;
      }
    }
    if (ops > 0) commits.push(batch.commit());
    await Promise.all(commits);

    const total = deletados + atualizados;
    logger.info(`🗑️ [SIGNALS-CLEAR] user=${req.user.tokenHash}${mode?' mode='+mode:''} → ${deletados} apagado(s), ${atualizados} atualizado(s)`);
    res.json({
      success: true,
      deletados,
      atualizados,
      total,
      mensagem: `${total} sinal(is) removido(s) do teu histórico`
    });
  } catch (err) {
    logger.error('Erro em DELETE /api/signals:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/analysis-history', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) return res.status(503).json({ error: 'Firestore indisponível' });
  const { symbol, mode, result } = req.body || {};
  if (!symbol || !mode || !result) {
    return res.status(400).json({ error: 'symbol, mode e result são obrigatórios' });
  }
  try {
    const consolidated = result.consolidated || {};
    if (consolidated.signal === 'HOLD') return res.json({ success: true, skipped: true });

    await db.collection('analysis_history').add({
      tokenHash: req.user.tokenHash,
      userEmail: req.user.email || null,
      symbol,
      mode,
      signal: consolidated.signal,
      score: consolidated.score ?? null,
      confidence: consolidated.confidence ?? null,
      zona: consolidated.zona ?? null,
      price: consolidated.price ?? null,
      scoreReasons: (consolidated.score_reasons || []).slice(0, 5),
      analisadoEm: admin.firestore.FieldValue.serverTimestamp()
    });
    res.json({ success: true });
  } catch (err) {
    logger.error('Erro ao gravar analysis_history:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/analysis-history', authMiddleware, async (req, res) => {
  if (!firebaseInitialized) return res.json({ history: [] });
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    let q = db.collection('analysis_history')
      .where('tokenHash', '==', req.user.tokenHash)
      .orderBy('analisadoEm', 'desc')
      .limit(limit);
    if (req.query.mode) {
      q = db.collection('analysis_history')
        .where('tokenHash', '==', req.user.tokenHash)
        .where('mode', '==', req.query.mode)
        .orderBy('analisadoEm', 'desc')
        .limit(limit);
    }
    const snap = await q.get();
    const history = snap.docs.map(d => ({
      id: d.id,
      ...d.data(),
      analisadoEm: d.data().analisadoEm?.toDate?.() || null
    }));
    res.json({ history });
  } catch (err) {
    logger.error('Erro ao ler analysis_history:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/stats', authMiddleware, async (req, res) => {
  const wl = firebaseInitialized ? await getUserWatchlist(req.user.tokenHash).catch(() => null) : null;
  const stats = {
    engineActive: !!wl?.engineActive,
    watchlistCount: wl ? [wl.SNIPER, wl['CAÇADOR'], wl.PESCADOR, wl.BALEEIRO].reduce((a, arr) => a + arr.length, 0) : 0,
    openTrades: tradesAbertos.size,
    uptime: process.uptime(),
    firebase: firebaseInitialized,
    pushConfigured
  };
  if (firebaseInitialized) {
    try {
      const col = db.collection('signals').where('watchers', 'array-contains', req.user.tokenHash);
      stats.totalSignals = (await col.count().get()).data().count;
      stats.signalsToday = (await db.collection('signals')
        .where('watchers', 'array-contains', req.user.tokenHash)
        .where('criadoEm', '>=', new Date(new Date().setHours(0, 0, 0, 0)))
        .count().get()).data().count;
    } catch (err) { logger.error('Erro stats:', err.message); }
  }
  res.json(stats);
});

// ========== SERVE FRONTEND (PWA) ==========
app.get(/^\/(?!.*\.(png|jpg|jpeg|gif|svg|ico|json|js|css|woff|woff2|ttf|webp)).*$/, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', async () => {
  logger.info(`🚀 Servidor rodando na porta ${PORT}`);
  logger.info(`Firebase: ${firebaseInitialized ? 'Conectado' : 'Não'}`);
  logger.info(`Push: ${pushConfigured ? 'Configurado' : 'Não configurado'}`);
  await loadStateFromFirestore();
});

const SELF_URL = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
setInterval(async () => {
  try { await fetch(`${SELF_URL}/health`); } catch (err) { logger.error('Erro no self-ping:', err.message); }
}, 4 * 60 * 1000);

process.on('SIGTERM', () => {
  logger.info('SIGTERM - encerrando...');
  process.exit(0);
});
