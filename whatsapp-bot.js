// ===================== whatsapp-bot.js =====================
// Bot WhatsApp Profissional com Firebase, Redis, API REST e gestao completa
// v5.0.0 - Com múltiplos administradores, comandos via WhatsApp e frontend

import express from 'express';
import cors from 'cors';
import { Client, LocalAuth } from 'whatsapp-web.js';
import qrcode from 'qrcode-terminal';
import QRCode from 'qrcode';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import admin from 'firebase-admin';
import { Redis } from '@upstash/redis';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

const PORT = process.env.PORT || 3000;

// ========== ADMINISTRADORES (MÚLTIPLOS) ==========
const ADMIN_PRINCIPAL_LIST = process.env.ADMIN_PRINCIPAL
  ? process.env.ADMIN_PRINCIPAL.split(',').map(n => n.trim())
  : [];
const ADMIN_NOTIFICACAO_LIST = process.env.ADMIN_NOTIFICACAO
  ? process.env.ADMIN_NOTIFICACAO.split(',').map(n => n.trim())
  : [];
const ALL_NOTIFICATION_NUMBERS = [...new Set([...ADMIN_PRINCIPAL_LIST, ...ADMIN_NOTIFICACAO_LIST])];

// ========== CONFIGURACOES DE WEBHOOK/NOTIFICACOES ==========
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';

// ========== CONFIGURACAO DOS IDS DOS GRUPOS (VARIAVEL DE AMBIENTE OU JSON) ==========
let configuredGroupIds = {};

function loadGroupIdsFromEnvOrFile() {
  const envGroupIds = process.env.GROUP_IDS;
  if (envGroupIds) {
    try {
      configuredGroupIds = JSON.parse(envGroupIds);
      console.log('IDs dos grupos carregados da variável de ambiente:', Object.keys(configuredGroupIds).length);
      return;
    } catch (err) {
      console.error('Erro ao parsear GROUP_IDS:', err.message);
    }
  }
  try {
    const groupIdsPath = path.join(__dirname, 'group-ids.json');
    if (fs.existsSync(groupIdsPath)) {
      const data = fs.readFileSync(groupIdsPath, 'utf8');
      configuredGroupIds = JSON.parse(data);
      console.log('IDs dos grupos carregados do arquivo JSON:', Object.keys(configuredGroupIds).length);
    } else {
      console.warn('Arquivo group-ids.json não encontrado. Criando vazio.');
      fs.writeFileSync(groupIdsPath, '{}');
      configuredGroupIds = {};
    }
  } catch (err) {
    console.error('Erro ao carregar group-ids.json:', err.message);
    configuredGroupIds = {};
  }
}
loadGroupIdsFromEnvOrFile();

// ========== REDIS ==========
let redis = null;
let redisInitialized = false;
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!redisUrl || !redisToken) {
    console.warn('Redis (Upstash) não configurado. Usando fallback local.');
    return false;
  }
  redis = new Redis({ url: redisUrl, token: redisToken });
  redisInitialized = true;
  console.log('Redis (Upstash) inicializado!');
  return true;
}
initializeRedis();

// ========== FIREBASE ==========
let db = null;
let firebaseAuth = null;
let firebaseInitialized = false;
function initializeFirebase() {
  try {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || '{}');
    if (!serviceAccount.project_id) {
      console.warn('Firebase nao configurado. Usando armazenamento local.');
      return false;
    }
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL: `https://${serviceAccount.project_id}-default-rtdb.firebaseio.com`
    });
    db = admin.firestore();
    firebaseAuth = admin.auth();
    firebaseInitialized = true;
    console.log('Firebase inicializado!');
    return true;
  } catch (err) {
    console.error('Erro ao inicializar Firebase:', err.message);
    return false;
  }
}
initializeFirebase();

// ========== CONFIGURACOES GERAIS ==========
const KEEPALIVE_INTERVAL_MS = 30000;
const HEALTH_CHECK_INTERVAL_MS = 30000;
const MAX_RECONNECT_ATTEMPTS = 10;
const SESSION_DIR = './sessions';
if (!fs.existsSync(SESSION_DIR)) fs.mkdirSync(SESSION_DIR, { recursive: true });

let client = null;
let isReady = false;
let currentQR = null;
let reconnectAttempts = 0;
let keepaliveInterval = null;
let healthCheckInterval = null;
const activeTimeouts = new Map();
const inviteCodeCache = new Map();
let groupConfig = {};
let generatedCodesCache = {};
const GENERATED_CODES_FILE = path.join(__dirname, 'generated-codes.json');

// ========== FUNCOES DE PERSISTENCIA DE CODIGOS GERADOS ==========
async function saveGeneratedCode(groupId, duration, codeData) {
  const key = `${groupId}_${duration}`;
  const data = { ...codeData, groupId, duration, generatedAt: new Date().toISOString() };
  generatedCodesCache[key] = data;
  try {
    fs.writeFileSync(GENERATED_CODES_FILE, JSON.stringify(generatedCodesCache, null, 2));
  } catch (err) { console.error('Erro ao salvar JSON:', err.message); }
  if (redisInitialized && redis) {
    try {
      await redis.set(`generated_code:${key}`, JSON.stringify(data));
      const durationMs = parseDurationToMs(duration);
      if (durationMs > 0) await redis.expire(`generated_code:${key}`, Math.ceil(durationMs / 1000));
    } catch (err) { console.error('Erro Redis:', err.message); }
  }
  if (firebaseInitialized && db) {
    try {
      const docId = key.replace(/[/.@]/g, '_');
      await db.collection('generated_codes').doc(docId).set({ ...data, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    } catch (err) { console.error('Erro Firebase:', err.message); }
  }
  return data;
}

async function loadGeneratedCodesFromFirebase() {
  if (!firebaseInitialized || !db) {
    try {
      if (fs.existsSync(GENERATED_CODES_FILE)) {
        const data = fs.readFileSync(GENERATED_CODES_FILE, 'utf8');
        generatedCodesCache = JSON.parse(data);
        console.log('Códigos carregados do JSON:', Object.keys(generatedCodesCache).length);
      }
    } catch (err) { console.error('Erro JSON:', err.message); }
    return;
  }
  try {
    const snapshot = await db.collection('generated_codes').get();
    snapshot.forEach(doc => {
      const data = doc.data();
      const key = `${data.groupId}_${data.duration}`;
      generatedCodesCache[key] = data;
    });
    fs.writeFileSync(GENERATED_CODES_FILE, JSON.stringify(generatedCodesCache, null, 2));
    console.log('Códigos carregados do Firebase:', Object.keys(generatedCodesCache).length);
  } catch (err) { console.error('Erro Firebase:', err.message); }
}

function parseDurationToMs(duration) {
  const match = duration.match(/^(\d+)(d|h|m)?$/i);
  if (!match) return 0;
  const value = parseInt(match[1]);
  const unit = (match[2] || 'd').toLowerCase();
  switch (unit) {
    case 'd': return value * 24 * 60 * 60 * 1000;
    case 'h': return value * 60 * 60 * 1000;
    case 'm': return value * 60 * 1000;
    default: return value * 24 * 60 * 60 * 1000;
  }
}

function getDurationLabel(duration) {
  const match = duration.match(/^(\d+)(d|h|m)?$/i);
  if (!match) return duration;
  const value = parseInt(match[1]);
  const unit = (match[2] || 'd').toLowerCase();
  switch (unit) {
    case 'd': return value === 1 ? '1 dia' : `${value} dias`;
    case 'h': return value === 1 ? '1 hora' : `${value} horas`;
    case 'm': return value === 1 ? '1 minuto' : `${value} minutos`;
    default: return duration;
  }
}

// ========== FUNCOES DO FIREBASE ==========
async function loadGroupsFromFirebase() {
  if (!firebaseInitialized) {
    try {
      const data = fs.readFileSync('./groups.json', 'utf8');
      groupConfig = JSON.parse(data);
      console.log('Grupos carregados do arquivo local:', Object.keys(groupConfig).length);
    } catch { groupConfig = {}; }
    return;
  }
  try {
    // CORRIGIDO: whatsaap → whatsapp
    const snapshot = await db.collection('whatsapp').get();
    groupConfig = {};
    snapshot.forEach(doc => {
      const data = doc.data();
      groupConfig[data.groupId || doc.id] = { ...data, id: data.groupId || doc.id };
    });
    console.log('Grupos carregados do Firebase:', Object.keys(groupConfig).length);
  } catch (err) { console.error('Erro ao carregar grupos:', err.message); }
}

async function saveGroupToFirebase(groupId, data) {
  if (!firebaseInitialized) {
    groupConfig[groupId] = { ...data, id: groupId };
    fs.writeFileSync('./groups.json', JSON.stringify(groupConfig, null, 2));
    return true;
  }
  try {
    // CORRIGIDO: whatsaap → whatsapp
    const docRef = db.collection('whatsapp').doc(groupId.replace(/[/.]/g, '_'));
    await docRef.set({ groupId, ...data, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    groupConfig[groupId] = { ...data, id: groupId };
    return true;
  } catch (err) { console.error('Erro ao salvar grupo:', err.message); return false; }
}

async function deleteGroupFromFirebase(groupId) {
  if (!firebaseInitialized) {
    delete groupConfig[groupId];
    fs.writeFileSync('./groups.json', JSON.stringify(groupConfig, null, 2));
    return true;
  }
  try {
    // CORRIGIDO: whatsaap → whatsapp
    await db.collection('whatsapp').doc(groupId.replace(/[/.]/g, '_')).delete();
    delete groupConfig[groupId];
    inviteCodeCache.delete(groupId);
    return true;
  } catch (err) { console.error('Erro ao deletar grupo:', err.message); return false; }
}

// ========== PERSISTENCIA DE PENDING REMOVALS ==========
async function savePendingRemoval(groupId, memberId, data) {
  if (!firebaseInitialized) return;
  try {
    const docId = `${groupId.replace(/[/.]/g, '_')}_${memberId.replace(/[/.]/g, '_')}`;
    await db.collection('pending_removals').doc(docId).set({ groupId, memberId, ...data, createdAt: admin.firestore.FieldValue.serverTimestamp() });
  } catch (err) { console.error('Erro ao salvar pending removal:', err.message); }
}

async function deletePendingRemoval(groupId, memberId) {
  if (!firebaseInitialized) return;
  try {
    const docId = `${groupId.replace(/[/.]/g, '_')}_${memberId.replace(/[/.]/g, '_')}`;
    await db.collection('pending_removals').doc(docId).delete();
  } catch (err) { console.error('Erro ao deletar pending removal:', err.message); }
}

async function loadPendingRemovals() {
  if (!firebaseInitialized || !isReady) return;
  try {
    const now = new Date();
    const snapshot = await db.collection('pending_removals').get();
    console.log(`Carregando ${snapshot.size} remocoes pendentes...`);
    for (const doc of snapshot.docs) {
      const data = doc.data();
      const expiresAt = new Date(data.expiresAt);
      const remainingMs = expiresAt.getTime() - now.getTime();
      if (remainingMs <= 0) {
        await executeRemoval(data.groupId, data.memberId, data.userName, data.groupName);
        await deletePendingRemoval(data.groupId, data.memberId);
      } else {
        try {
          const chat = await client.getChatById(data.groupId);
          const groupInfo = groupConfig[data.groupId] || { name: data.groupName };
          scheduleRemovalInternal(chat, data.memberId, data.userName, remainingMs, groupInfo, false);
          console.log(`Remocao reprogramada: ${data.userName} em ${Math.round(remainingMs/3600000)}h`);
        } catch (err) { console.error(`Erro ao reprogramar: ${err.message}`); }
      }
    }
  } catch (err) { console.error('Erro ao carregar pending removals:', err.message); }
}

// ========== FUNCOES DE NOTIFICACAO ==========
async function sendAdminNotification(message) {
  for (const number of ALL_NOTIFICATION_NUMBERS) {
    if (number && isReady && client) {
      try {
        await client.sendMessage(`${number}@c.us`, message);
        console.log(`Notificação enviada para ${number}`);
      } catch (err) { console.error(`Erro ao notificar ${number}:`, err.message); }
    }
  }
  if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) {
    try {
      await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message, parse_mode: 'HTML' })
      });
    } catch (err) { console.error('Erro Telegram:', err.message); }
  }
}

async function logActivity(type, data) {
  const logMessage = `[${type}] ${JSON.stringify(data)}`;
  console.log(logMessage);
  if (!firebaseInitialized) return;
  try {
    await db.collection('activity_logs').add({ type, ...data, timestamp: admin.firestore.FieldValue.serverTimestamp() });
  } catch (err) { console.error('Erro ao salvar log:', err.message); }
}

async function saveMemberToFirebase(groupId, memberId, data) {
  if (!firebaseInitialized) return;
  try {
    const docId = `${groupId.replace(/[/.]/g, '_')}_${memberId.replace(/[/.]/g, '_')}`;
    await db.collection('members').doc(docId).set({ groupId, memberId, ...data, status: 'active', createdAt: admin.firestore.FieldValue.serverTimestamp() });
  } catch (err) { console.error('Erro ao salvar membro:', err.message); }
}

async function updateMemberStatus(groupId, memberId, status) {
  if (!firebaseInitialized) return;
  try {
    const docId = `${groupId.replace(/[/.]/g, '_')}_${memberId.replace(/[/.]/g, '_')}`;
    await db.collection('members').doc(docId).update({ status, updatedAt: admin.firestore.FieldValue.serverTimestamp() });
  } catch (err) { console.error('Erro ao atualizar membro:', err.message); }
}

// Carregar dados iniciais
await loadGroupsFromFirebase();
await loadGeneratedCodesFromFirebase();

// ========== KEEPALIVE E HEALTH CHECK ==========
function startKeepalive() {
  if (keepaliveInterval) clearInterval(keepaliveInterval);
  keepaliveInterval = setInterval(async () => {
    if (client && isReady) {
      try { await client.sendSeen('status@broadcast'); console.log('Keepalive enviado'); } catch (err) { console.error('Falha no keepalive:', err.message); }
    }
  }, KEEPALIVE_INTERVAL_MS);
}

function startHealthCheck() {
  if (healthCheckInterval) clearInterval(healthCheckInterval);
  healthCheckInterval = setInterval(async () => {
    if (client && isReady) {
      try {
        const page = client.pupPage;
        if (!page || page.isClosed()) { console.error('Health check: pupPage fechada'); isReady = false; await initializeBot(); }
      } catch (err) { console.error('Health check error:', err.message); }
    }
  }, HEALTH_CHECK_INTERVAL_MS);
}

// ========== EXECUCAO DE REMOCAO ==========
async function executeRemoval(groupId, userId, userName, groupName) {
  try {
    const chat = await client.getChatById(groupId);
    await chat.sendMessage(`Olá @${userId.split('@')[0]}, seu tempo no grupo "${groupName}" expirou. Obrigado!`);
    await chat.removeParticipants([userId]);
    console.log(`${userName} removido do grupo ${groupName}`);
    await updateMemberStatus(groupId, userId, 'removed');
    await logActivity('member_removed', { userId, userName, groupId, groupName });
    await sendAdminNotification(`🔹 Membro removido:\n- Nome: ${userName}\n- Grupo: ${groupName}`);
  } catch (err) {
    console.error(`Erro ao remover ${userId}:`, err.message);
    await logActivity('removal_error', { userId, groupId, error: err.message });
  }
}

// ========== AGENDAR REMOCAO ==========
function scheduleRemovalInternal(chat, userId, userName, durationMs, groupInfo, saveToDB = true) {
  const key = `${chat.id._serialized}_${userId}`;
  if (activeTimeouts.has(key)) {
    const existing = activeTimeouts.get(key);
    if (existing.main) clearTimeout(existing.main);
    if (existing.warning24h) clearTimeout(existing.warning24h);
    if (existing.warning1h) clearTimeout(existing.warning1h);
    activeTimeouts.delete(key);
  }
  const expiresAt = new Date(Date.now() + durationMs);
  const timeouts = {};
  if (durationMs > 86400000) {
    const warningTime24h = durationMs - 86400000;
    timeouts.warning24h = setTimeout(async () => {
      try { await chat.sendMessage(`Atenção @${userId.split('@')[0]}, seu acesso ao grupo "${groupInfo.name}" expira em 24 horas!`); await logActivity('warning_24h_sent', { userId, groupId: chat.id._serialized }); } catch (err) { console.error('Erro aviso 24h:', err.message); }
    }, warningTime24h);
  }
  if (durationMs > 3600000) {
    const warningTime1h = durationMs - 3600000;
    timeouts.warning1h = setTimeout(async () => {
      try { await chat.sendMessage(`Último aviso @${userId.split('@')[0]}! Seu acesso ao grupo "${groupInfo.name}" expira em 1 hora!`); await logActivity('warning_1h_sent', { userId, groupId: chat.id._serialized }); } catch (err) { console.error('Erro aviso 1h:', err.message); }
    }, warningTime1h);
  }
  timeouts.main = setTimeout(async () => {
    await executeRemoval(chat.id._serialized, userId, userName, groupInfo.name);
    activeTimeouts.delete(key);
    await deletePendingRemoval(chat.id._serialized, userId);
  }, durationMs);
  activeTimeouts.set(key, timeouts);
  if (saveToDB) {
    savePendingRemoval(chat.id._serialized, userId, { userName, groupName: groupInfo.name, durationMs, expiresAt: expiresAt.toISOString() });
  }
  console.log(`Remoção agendada: ${userName} em ${expiresAt.toLocaleString()}`);
}

function scheduleRemoval(chat, userId, userName, durationMs, groupInfo) {
  scheduleRemovalInternal(chat, userId, userName, durationMs, groupInfo, true);
}

// ========== COMANDOS ADMINISTRATIVOS VIA WHATSAPP ==========
async function handleAdminCommand(message) {
  const from = message.from.replace('@c.us', '');
  if (!ADMIN_PRINCIPAL_LIST.includes(from)) return;
  const body = message.body.trim();
  const parts = body.split(/\s+/);
  const cmd = parts[0].toLowerCase();
  const response = [];

  try {
    switch (cmd) {
      case '/ajuda':
        response.push('📋 *Comandos disponíveis:*');
        response.push('/status – status do bot');
        response.push('/grupos – lista grupos configurados');
        response.push('/membros – membros ativos (máx 30)');
        response.push('/membros <groupId> – membros de um grupo específico');
        response.push('/remover <groupId> <userId> – força remoção');
        response.push('/reiniciar – reconecta o WhatsApp');
        response.push('/limparCache – limpa cache de convites');
        response.push('/sair – encerra o bot (apenas admin principal)');
        break;
      case '/status':
        response.push(`🤖 *Status do Bot*`);
        response.push(`• Conectado: ${isReady ? '✅' : '❌'}`);
        response.push(`• Uptime: ${Math.floor(process.uptime() / 3600)}h ${Math.floor((process.uptime() % 3600) / 60)}m`);
        response.push(`• Grupos configurados (API): ${Object.keys(groupConfig).length}`);
        response.push(`• Grupos permitidos (GROUP_IDS): ${Object.keys(configuredGroupIds).length}`);
        response.push(`• Remoções ativas: ${activeTimeouts.size}`);
        response.push(`• Códigos gerados: ${Object.keys(generatedCodesCache).length}`);
        response.push(`• Firebase: ${firebaseInitialized ? '✅' : '❌'}  | Redis: ${redisInitialized ? '✅' : '❌'}`);
        break;
      case '/grupos':
        if (Object.keys(groupConfig).length === 0) response.push('Nenhum grupo configurado ainda.');
        else {
          response.push('📌 *Grupos configurados:*');
          for (const [groupId, info] of Object.entries(groupConfig)) {
            response.push(`• ${info.name || 'Sem nome'} → ${groupId.substring(0, 20)}... (${info.durationMs/86400000} dias)`);
          }
        }
        break;
      case '/membros': {
        let targetGroupId = parts[1];
        let membersList = [];
        if (targetGroupId) {
          if (!firebaseInitialized) {
            membersList = Array.from(activeTimeouts.entries())
              .filter(([key]) => key.startsWith(targetGroupId))
              .map(([key, timeouts]) => ({ userId: key.split('_')[1], expiresAt: new Date(Date.now() + (timeouts.main ? timeouts.main._idleTimeout : 0)) }));
          } else {
            const snapshot = await db.collection('members').where('groupId', '==', targetGroupId).where('status', '==', 'active').get();
            membersList = snapshot.docs.map(doc => doc.data());
          }
          if (membersList.length === 0) response.push(`Nenhum membro ativo no grupo ${targetGroupId.substring(0,15)}...`);
          else {
            response.push(`👥 *Membros ativos (${membersList.length})*:`);
            for (const m of membersList) {
              const expires = new Date(m.expirationDate || m.expiresAt);
              const horasRest = Math.floor((expires - Date.now()) / 3600000);
              response.push(`• ${m.userName || m.memberId} → ${horasRest > 0 ? horasRest + 'h restantes' : 'expirado em breve'}`);
            }
          }
        } else {
          if (!firebaseInitialized) membersList = Array.from(activeTimeouts.keys()).map(key => ({ userId: key.split('_')[1], groupId: key.split('_')[0] }));
          else {
            const snapshot = await db.collection('members').where('status', '==', 'active').orderBy('createdAt', 'desc').limit(30).get();
            membersList = snapshot.docs.map(doc => doc.data());
          }
          if (membersList.length === 0) response.push('Nenhum membro ativo no momento.');
          else {
            response.push(`👥 *Últimos membros ativos (max 30)*:`);
            for (const m of membersList) {
              const groupName = groupConfig[m.groupId]?.name || 'Grupo desconhecido';
              response.push(`• ${m.userName || m.memberId} → ${groupName}`);
            }
          }
        }
        break;
      }
      case '/remover': {
        const groupId = parts[1];
        const userId = parts[2];
        if (!groupId || !userId) { response.push('❌ Uso correto: `/remover <groupId> <userId>`'); break; }
        const groupInfo = groupConfig[groupId];
        if (!groupInfo) { response.push(`❌ Grupo ${groupId} não encontrado.`); break; }
        await executeRemoval(groupId, userId, 'Usuário removido manualmente', groupInfo.name);
        response.push(`✅ Remoção forçada de ${userId} do grupo ${groupInfo.name} solicitada.`);
        break;
      }
      case '/reiniciar':
        response.push('🔄 Reiniciando o bot... (pode levar alguns segundos)');
        await sendAdminNotification(`Bot reiniciado manualmente por ${from}`);
        reconnectAttempts = 0;
        await initializeBot();
        response.push('✅ Reinicialização concluída.');
        break;
      case '/limparCache':
        inviteCodeCache.clear();
        generatedCodesCache = {};
        if (fs.existsSync(GENERATED_CODES_FILE)) fs.unlinkSync(GENERATED_CODES_FILE);
        response.push('🧹 Cache de convites e códigos gerados limpo.');
        break;
      case '/sair':
        if (ADMIN_PRINCIPAL_LIST.includes(from)) {
          response.push('🛑 Encerrando o bot... Até logo.');
          await sendAdminNotification(`Bot foi encerrado por ${from}`);
          setTimeout(() => process.exit(0), 1000);
        } else { response.push('Apenas administradores principais podem usar /sair.'); }
        break;
      default:
        if (cmd.startsWith('/')) response.push('Comando não reconhecido. Digite `/ajuda` para ver os comandos disponíveis.');
        return;
    }
  } catch (err) {
    console.error('Erro ao processar comando:', err);
    response.push('❌ Ocorreu um erro ao processar seu comando.');
  }
  if (response.length > 0) await message.reply(response.join('\n'));
}

// ========== CLIENTE WHATSAPP ==========
function createClient() {
  const newClient = new Client({
    authStrategy: new LocalAuth({ dataPath: SESSION_DIR, clientId: 'render-wa-bot-v5' }),
    puppeteer: {
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-accelerated-2d-canvas', '--no-first-run', '--no-zygote', '--disable-gpu', '--single-process']
    }
  });

  newClient.on('qr', async (qr) => {
    console.log('ESCANEIE O QR CODE:');
    qrcode.generate(qr, { small: true });
    currentQR = qr;
    isReady = false;
    await logActivity('qr_generated', { timestamp: new Date().toISOString() });
  });

  newClient.on('ready', async () => {
    console.log('WhatsApp conectado!');
    isReady = true;
    currentQR = null;
    reconnectAttempts = 0;
    startKeepalive();
    startHealthCheck();
    await logActivity('bot_connected', { timestamp: new Date().toISOString() });
    setTimeout(async () => {
      await loadGroupsFromFirebase();
      await loadGeneratedCodesFromFirebase();
      await loadPendingRemovals();
    }, 5000);
    await sendAdminNotification('✅ Bot WhatsApp conectado e operacional!');
  });

  newClient.on('auth_failure', (msg) => {
    console.error('Falha autenticação:', msg);
    isReady = false;
    logActivity('auth_failure', { message: msg });
  });

  newClient.on('disconnected', async (reason) => {
    console.error(`Desconectado: ${reason}`);
    isReady = false;
    if (keepaliveInterval) clearInterval(keepaliveInterval);
    if (healthCheckInterval) clearInterval(healthCheckInterval);
    await logActivity('bot_disconnected', { reason });
    await sendAdminNotification(`⚠️ Bot desconectado: ${reason}`);
    if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
      const delay = Math.min(5000 * Math.pow(1.5, reconnectAttempts), 60000);
      reconnectAttempts++;
      console.log(`Reconectando em ${delay/1000}s (tentativa ${reconnectAttempts})...`);
      setTimeout(() => initializeBot(), delay);
    } else {
      console.error('Máximo de tentativas atingido.');
      await sendAdminNotification('ALERTA: Bot atingiu máximo de tentativas de reconexão!');
    }
  });

  newClient.on('group_join', async (notification) => {
    try {
      const chat = await notification.getChat();
      const groupId = chat.id._serialized;
      const groupInfo = groupConfig[groupId];
      if (!groupInfo) {
        console.log(`Grupo ${chat.name} (${groupId}) não configurado. Ignorando.`);
        return;
      }
      const recipientIds = notification.recipientIds || [];
      for (const recipientId of recipientIds) {
        const contact = await client.getContactById(recipientId);
        const userName = contact.pushname || contact.number || recipientId;
        console.log(`${userName} entrou no grupo "${chat.name}" (${groupInfo.level} - ${groupInfo.durationMs/86400000} dias)`);
        const daysText = groupInfo.durationMs / 86400000;
        const welcomeMsg = groupInfo.welcomeMessage || `Bem-vindo(a) ao grupo *${groupInfo.name}*!\n\nSeu acesso é válido por *${daysText} dia(s)*.\nVocê será notificado antes da expiração.\n\nAproveite!`;
        await chat.sendMessage(welcomeMsg);
        await saveMemberToFirebase(groupId, recipientId, { userName, groupName: groupInfo.name, durationMs: groupInfo.durationMs, expirationDate: new Date(Date.now() + groupInfo.durationMs).toISOString() });
        await logActivity('member_joined', { userId: recipientId, userName, groupId, groupName: groupInfo.name });
        await sendAdminNotification(`🆕 Nova entrada:\n- Nome: ${userName}\n- Grupo: ${groupInfo.name}\n- Duração: ${daysText} dias`);
        scheduleRemoval(chat, recipientId, userName, groupInfo.durationMs, groupInfo);
      }
    } catch (err) { console.error('Erro ao processar entrada:', err.message); }
  });

  // Listener para comandos privados
  newClient.on('message', async (message) => {
    if (message.from.includes('@g.us')) return;
    await handleAdminCommand(message);
  });

  return newClient;
}

// ========== INICIALIZACAO ==========
async function initializeBot() {
  if (client) { try { await client.destroy(); } catch(e) {} }
  client = createClient();
  await client.initialize();
}

// ========== MIDDLEWARE ==========
async function authMiddleware(req, res, next) {
  const authHeader = req.headers['authorization'];
  if (!authHeader || !authHeader.startsWith('Bearer ')) return res.status(401).json({ error: 'Token não fornecido' });
  const idToken = authHeader.split('Bearer ')[1];
  try {
    if (firebaseInitialized && firebaseAuth) await firebaseAuth.verifyIdToken(idToken);
    next();
  } catch (err) { return res.status(401).json({ error: 'Token inválido' }); }
}

// ========== API ENDPOINTS ==========
app.get('/api/health', (req, res) => {
  res.json({ status: isReady ? 'ready' : 'connecting', qrNeeded: !isReady && currentQR, reconnectAttempts, uptime: process.uptime(), firebase: firebaseInitialized, redis: redisInitialized, timestamp: new Date().toISOString() });
});

app.get('/api/qr', authMiddleware, async (req, res) => {
  if (!currentQR) return res.json({ available: false, message: 'Bot já conectado ou QR não disponível' });
  try {
    const qrDataUrl = await QRCode.toDataURL(currentQR, { width: 300 });
    res.json({ available: true, qrCode: qrDataUrl });
  } catch (err) { res.status(500).json({ error: 'Erro ao gerar QR code' }); }
});

app.get('/api/configured-groups', authMiddleware, async (req, res) => {
  loadGroupIdsFromEnvOrFile();
  const groups = Object.entries(configuredGroupIds).map(([groupId, duration]) => {
    const durationMs = parseDurationToMs(duration);
    const durationLabel = getDurationLabel(duration);
    const groupInfo = groupConfig[groupId];
    return { groupId, duration, durationMs, durationLabel, name: groupInfo?.name || `Grupo ${groupId.substring(0,10)}...`, level: groupInfo?.level || 'normal' };
  });
  res.json({ groups });
});

app.post('/api/generate-invite', authMiddleware, async (req, res) => {
  const { groupId, duration } = req.body;
  if (!groupId || !duration) return res.status(400).json({ error: 'groupId e duration são obrigatórios' });
  if (!configuredGroupIds[groupId]) return res.status(400).json({ error: 'Grupo não encontrado nas configurações' });
  if (!isReady) return res.status(503).json({ error: 'Bot não está conectado' });
  try {
    const chat = await client.getChatById(groupId);
    if (!chat.isGroup) return res.status(400).json({ error: 'ID não corresponde a um grupo' });
    const inviteCode = await chat.getInviteCode();
    const inviteLink = `https://chat.whatsapp.com/${inviteCode}`;
    const qrDataUrl = await QRCode.toDataURL(inviteLink, { width: 300 });
    const durationMs = parseDurationToMs(duration);
    const durationLabel = getDurationLabel(duration);
    const codeData = { inviteLink, qrCode: qrDataUrl, inviteCode, groupName: chat.name, durationMs, durationLabel };
    await saveGeneratedCode(groupId, duration, codeData);
    if (!groupConfig[groupId]) await saveGroupToFirebase(groupId, { name: chat.name, durationMs, level: 'normal', welcomeMessage: '', expiryMessage: '' });
    await logActivity('invite_generated', { groupId, groupName: chat.name, duration, durationLabel });
    res.json({ success: true, ...codeData });
  } catch (err) {
    console.error('Erro ao gerar convite:', err);
    res.status(500).json({ error: `Erro: ${err.message}. Verifique se o bot é admin do grupo.` });
  }
});

app.get('/api/groups', authMiddleware, async (req, res) => {
  const page = parseInt(req.query.page) || 1, limit = parseInt(req.query.limit) || 20, offset = (page-1)*limit;
  const allGroups = Object.entries(groupConfig).map(([id, info]) => ({ id, ...info, durationDays: info.durationMs/86400000 }));
  const paginatedGroups = allGroups.slice(offset, offset+limit);
  res.json({ groups: paginatedGroups, total: allGroups.length, page, totalPages: Math.ceil(allGroups.length/limit) });
});

app.post('/api/groups', authMiddleware, async (req, res) => {
  const { groupId, name, durationDays, level, welcomeMessage, expiryMessage } = req.body;
  if (!groupId || !name || !durationDays) return res.status(400).json({ error: 'Campos obrigatórios: groupId, name, durationDays' });
  const groupData = { name, durationMs: durationDays*86400000, level: level||'normal', welcomeMessage: welcomeMessage||'', expiryMessage: expiryMessage||'', updatedAt: new Date().toISOString() };
  const success = await saveGroupToFirebase(groupId, groupData);
  if (success) { await logActivity('group_created', { groupId, name }); res.json({ success: true, group: { id: groupId, ...groupData, durationDays } }); }
  else res.status(500).json({ error: 'Erro ao salvar grupo' });
});

app.delete('/api/groups/:groupId', authMiddleware, async (req, res) => {
  const groupId = decodeURIComponent(req.params.groupId);
  const success = await deleteGroupFromFirebase(groupId);
  if (success) { await logActivity('group_deleted', { groupId }); res.json({ success: true }); }
  else res.status(500).json({ error: 'Erro ao deletar grupo' });
});

app.get('/api/whatsapp-groups', authMiddleware, async (req, res) => {
  if (!isReady) return res.status(503).json({ error: 'Bot não está conectado' });
  try {
    const chats = await client.getChats();
    const groups = chats.filter(chat => chat.isGroup).map(chat => ({ id: chat.id._serialized, name: chat.name, participantsCount: chat.participants?.length || 0, isConfigured: !!groupConfig[chat.id._serialized] }));
    res.json({ groups });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/members', authMiddleware, async (req, res) => {
  const page = parseInt(req.query.page)||1, limit = parseInt(req.query.limit)||50, groupId = req.query.groupId;
  if (!firebaseInitialized) {
    const members = Array.from(activeTimeouts.keys()).map(key => { const parts = key.split('_'); return { groupId: parts[0], memberId: parts[1], active: true }; });
    return res.json({ members, total: members.length });
  }
  try {
    let query = db.collection('members').where('status', '==', 'active');
    if (groupId) query = query.where('groupId', '==', groupId);
    const snapshot = await query.orderBy('createdAt', 'desc').limit(limit).get();
    const members = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    res.json({ members, page, limit });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/stats', authMiddleware, async (req, res) => {
  const stats = { totalGroups: Object.keys(groupConfig).length, configuredGroups: Object.keys(configuredGroupIds).length, activeTimeouts: activeTimeouts.size, botReady: isReady, uptime: process.uptime(), redis: redisInitialized, firebase: firebaseInitialized };
  if (firebaseInitialized) {
    try {
      const activeMembersSnap = await db.collection('members').where('status', '==', 'active').get();
      stats.activeMembers = activeMembersSnap.size;
      const pendingRemovalsSnap = await db.collection('pending_removals').get();
      stats.pendingRemovals = pendingRemovalsSnap.size;
    } catch (err) { console.error('Erro stats:', err.message); }
  }
  res.json(stats);
});

app.post('/api/reconnect', authMiddleware, async (req, res) => {
  console.log('Reinicialização manual solicitada');
  reconnectAttempts = 0;
  await initializeBot();
  await logActivity('manual_reconnect', { timestamp: new Date().toISOString() });
  res.json({ message: 'Reconexão iniciada' });
});

app.post('/api/clear-invite-cache', authMiddleware, (req, res) => {
  const groupId = req.body.groupId;
  if (groupId) inviteCodeCache.delete(groupId);
  else inviteCodeCache.clear();
  res.json({ message: groupId ? `Cache limpo para ${groupId}` : 'Todo cache de convites limpo' });
});

app.get('/api/status', authMiddleware, (req, res) => {
  const sessionExists = fs.existsSync(path.join(SESSION_DIR, 'Default'));
  res.json({ ready: isReady, sessionExists, keepaliveActive: keepaliveInterval !== null, healthCheckActive: healthCheckInterval !== null, configuredGroups: Object.keys(groupConfig).length, envConfiguredGroups: Object.keys(configuredGroupIds).length, activeTimeouts: activeTimeouts.size, cachedInvites: inviteCodeCache.size, generatedCodes: Object.keys(generatedCodesCache).length, firebase: firebaseInitialized, redis: redisInitialized, puppeteerAlive: client?.pupPage ? !client.pupPage.isClosed() : false });
});

// Endpoint para comandos via frontend
app.post('/api/command', authMiddleware, async (req, res) => {
  const { command } = req.body;
  if (!command) return res.status(400).json({ error: 'Comando não fornecido' });
  const fakeMessage = {
    from: `${ADMIN_PRINCIPAL_LIST[0] || 'admin'}@c.us`,
    body: command,
    reply: async (text) => { fakeMessage.lastResponse = text; }
  };
  try {
    await handleAdminCommand(fakeMessage);
    res.json({ success: true, response: fakeMessage.lastResponse || 'Comando executado, mas sem resposta textual.' });
  } catch (err) {
    console.error('Erro ao executar comando:', err);
    res.status(500).json({ error: err.message });
  }
});

// Servir frontend
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ========== INICIALIZACAO DO SERVIDOR ==========
app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 Servidor rodando na porta ${PORT}`);
  console.log(`API: /api/health`);
  console.log(`Firebase: ${firebaseInitialized ? 'Conectado' : 'Não'}`);
  console.log(`Redis: ${redisInitialized ? 'Conectado' : 'Não'}`);
  console.log(`Admin principal: ${ADMIN_PRINCIPAL_LIST.join(', ') || 'nenhum'}`);
  console.log(`Admin notificação: ${ADMIN_NOTIFICACAO_LIST.join(', ') || 'nenhum'}`);
  console.log(`Grupos configurados: ${Object.keys(configuredGroupIds).length}`);
});

initializeBot().catch(err => {
  console.error('Falha fatal na inicialização:', err);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM recebido, encerrando...');
  if (keepaliveInterval) clearInterval(keepaliveInterval);
  if (healthCheckInterval) clearInterval(healthCheckInterval);
  for (const timeouts of activeTimeouts.values()) {
    if (timeouts.main) clearTimeout(timeouts.main);
    if (timeouts.warning24h) clearTimeout(timeouts.warning24h);
    if (timeouts.warning1h) clearTimeout(timeouts.warning1h);
  }
  if (client) await client.destroy();
  process.exit(0);
});