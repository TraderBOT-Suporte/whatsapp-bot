// ===================== whatsapp-bot.js =====================
// Bot WhatsApp Profissional com Firebase, Redis, API REST e gestao completa
// v6.2.0 – Completo: países, endpoints, afiliados, chatbot, página do cliente

import express from 'express';
import cors from 'cors';
import pkg from 'whatsapp-web.js';
const { Client, LocalAuth } = pkg;
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
const TELEGRAM_CHAT_ID   = process.env.TELEGRAM_CHAT_ID   || '';

// ========== CONFIGURACAO DOS IDS DOS GRUPOS ==========
let configuredGroupIds = {};
function loadGroupIdsFromEnvOrFile() {
  const envGroupIds = process.env.GROUP_IDS;
  if (envGroupIds) {
    try {
      configuredGroupIds = JSON.parse(envGroupIds);
      console.log('IDs dos grupos carregados da variável de ambiente:', Object.keys(configuredGroupIds).length);
      return;
    } catch (err) { console.error('Erro ao parsear GROUP_IDS:', err.message); }
  }
  try {
    const groupIdsPath = path.join(__dirname, 'group-ids.json');
    if (fs.existsSync(groupIdsPath)) {
      configuredGroupIds = JSON.parse(fs.readFileSync(groupIdsPath, 'utf8'));
      console.log('IDs dos grupos carregados do JSON:', Object.keys(configuredGroupIds).length);
    } else {
      fs.writeFileSync(groupIdsPath, '{}');
      configuredGroupIds = {};
    }
  } catch (err) { console.error('Erro group-ids.json:', err.message); configuredGroupIds = {}; }
}
loadGroupIdsFromEnvOrFile();

// ========== REDIS ==========
let redis = null;
let redisInitialized = false;
function initializeRedis() {
  const redisUrl   = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!redisUrl || !redisToken) { console.warn('Redis não configurado.'); return false; }
  redis = new Redis({ url: redisUrl, token: redisToken });
  redisInitialized = true;
  console.log('Redis inicializado!');
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
    if (!serviceAccount.project_id) { console.warn('Firebase não configurado.'); return false; }
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL: `https://${serviceAccount.project_id}-default-rtdb.firebaseio.com`
    });
    db = admin.firestore();
    firebaseAuth = admin.auth();
    firebaseInitialized = true;
    console.log('Firebase inicializado!');
    return true;
  } catch (err) { console.error('Erro Firebase:', err.message); return false; }
}
initializeFirebase();

// ========== CONFIGURACOES GERAIS ==========
const KEEPALIVE_INTERVAL_MS    = 30000;
const HEALTH_CHECK_INTERVAL_MS = 30000;
const MAX_RECONNECT_ATTEMPTS   = 10;
const SESSION_DIR       = './sessions';
const SESSION_CLIENT_ID = 'render-wa-bot-v5';
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
const bannedUsers = new Map(); // userId -> { groupId, bannedAt, reason }

// ========== BACKUP E RESTAURAÇÃO DA SESSÃO NO FIRESTORE ==========
const SESSION_BACKUP_DOC_ID    = 'whatsapp_bot';
const SESSION_BACKUP_COLLECTION = 'sessions';

async function backupSessionToFirestore() {
  if (!firebaseInitialized || !isReady) return;
  const sessionPath = path.join(SESSION_DIR, `session-${SESSION_CLIENT_ID}`);
  if (!fs.existsSync(sessionPath)) return;
  try {
    const files = {};
    const readRecursive = (dir, base = '') => {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const fullPath     = path.join(dir, entry.name);
        const relativePath = base + entry.name;
        if (entry.isDirectory()) readRecursive(fullPath, relativePath + '/');
        else files[relativePath] = fs.readFileSync(fullPath).toString('base64');
      }
    };
    readRecursive(sessionPath);
    await db.collection(SESSION_BACKUP_COLLECTION).doc(SESSION_BACKUP_DOC_ID).set({
      session: JSON.stringify(files), updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    console.log('Sessão salva no Firestore (backup).');
  } catch (err) { console.error('Erro backup sessão:', err.message); }
}

async function restoreSessionFromFirestore() {
  if (!firebaseInitialized || !db) return false;
  try {
    const doc = await db.collection(SESSION_BACKUP_COLLECTION).doc(SESSION_BACKUP_DOC_ID).get();
    if (!doc.exists) return false;
    const data = doc.data();
    if (!data.session) return false;
    const files       = JSON.parse(data.session);
    const sessionPath = path.join(SESSION_DIR, `session-${SESSION_CLIENT_ID}`);
    if (fs.existsSync(sessionPath)) fs.rmSync(sessionPath, { recursive: true, force: true });
    Object.entries(files).forEach(([relativePath, b64Content]) => {
      const fullPath = path.join(sessionPath, relativePath);
      const dir      = path.dirname(fullPath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(fullPath, Buffer.from(b64Content, 'base64'));
    });
    console.log('Sessão restaurada do Firestore.');
    return true;
  } catch (err) { console.error('Erro restaurar sessão:', err.message); return false; }
}

// ========== FUNÇÕES AUXILIARES ==========
function parseDurationToMs(duration) {
  const match = duration.match(/^(\d+)(d|h|m)?$/i);
  if (!match) return 0;
  const value = parseInt(match[1]);
  const unit  = (match[2] || 'd').toLowerCase();
  switch (unit) {
    case 'd': return value * 86400000;
    case 'h': return value * 3600000;
    case 'm': return value * 60000;
    default:  return value * 86400000;
  }
}

function getDurationLabel(duration) {
  const match = duration.match(/^(\d+)(d|h|m)?$/i);
  if (!match) return duration;
  const value = parseInt(match[1]);
  const unit  = (match[2] || 'd').toLowerCase();
  switch (unit) {
    case 'd': return value === 1 ? '1 dia'    : `${value} dias`;
    case 'h': return value === 1 ? '1 hora'   : `${value} horas`;
    case 'm': return value === 1 ? '1 minuto' : `${value} minutos`;
    default:  return duration;
  }
}

// ========== MAPEAMENTO DE PAÍSES / DETECÇÃO DE DDI ==========
const COUNTRY_CODES = {
  '1':   { name: 'EUA / Canadá',            flag: '🇺🇸', ddi: '+1'   },
  '7':   { name: 'Rússia',                  flag: '🇷🇺', ddi: '+7'   },
  '20':  { name: 'Egipto',                  flag: '🇪🇬', ddi: '+20'  },
  '27':  { name: 'África do Sul',           flag: '🇿🇦', ddi: '+27'  },
  '30':  { name: 'Grécia',                  flag: '🇬🇷', ddi: '+30'  },
  '31':  { name: 'Países Baixos',           flag: '🇳🇱', ddi: '+31'  },
  '32':  { name: 'Bélgica',                 flag: '🇧🇪', ddi: '+32'  },
  '33':  { name: 'França',                  flag: '🇫🇷', ddi: '+33'  },
  '34':  { name: 'Espanha',                 flag: '🇪🇸', ddi: '+34'  },
  '36':  { name: 'Hungria',                 flag: '🇭🇺', ddi: '+36'  },
  '39':  { name: 'Itália',                  flag: '🇮🇹', ddi: '+39'  },
  '40':  { name: 'Roménia',                 flag: '🇷🇴', ddi: '+40'  },
  '41':  { name: 'Suíça',                   flag: '🇨🇭', ddi: '+41'  },
  '43':  { name: 'Áustria',                 flag: '🇦🇹', ddi: '+43'  },
  '44':  { name: 'Reino Unido',             flag: '🇬🇧', ddi: '+44'  },
  '45':  { name: 'Dinamarca',               flag: '🇩🇰', ddi: '+45'  },
  '46':  { name: 'Suécia',                  flag: '🇸🇪', ddi: '+46'  },
  '47':  { name: 'Noruega',                 flag: '🇳🇴', ddi: '+47'  },
  '48':  { name: 'Polónia',                 flag: '🇵🇱', ddi: '+48'  },
  '49':  { name: 'Alemanha',                flag: '🇩🇪', ddi: '+49'  },
  '51':  { name: 'Peru',                    flag: '🇵🇪', ddi: '+51'  },
  '52':  { name: 'México',                  flag: '🇲🇽', ddi: '+52'  },
  '54':  { name: 'Argentina',               flag: '🇦🇷', ddi: '+54'  },
  '55':  { name: 'Brasil',                  flag: '🇧🇷', ddi: '+55'  },
  '56':  { name: 'Chile',                   flag: '🇨🇱', ddi: '+56'  },
  '57':  { name: 'Colômbia',                flag: '🇨🇴', ddi: '+57'  },
  '58':  { name: 'Venezuela',               flag: '🇻🇪', ddi: '+58'  },
  '60':  { name: 'Malásia',                 flag: '🇲🇾', ddi: '+60'  },
  '61':  { name: 'Austrália',               flag: '🇦🇺', ddi: '+61'  },
  '62':  { name: 'Indonésia',               flag: '🇮🇩', ddi: '+62'  },
  '63':  { name: 'Filipinas',               flag: '🇵🇭', ddi: '+63'  },
  '64':  { name: 'Nova Zelândia',           flag: '🇳🇿', ddi: '+64'  },
  '65':  { name: 'Singapura',               flag: '🇸🇬', ddi: '+65'  },
  '66':  { name: 'Tailândia',               flag: '🇹🇭', ddi: '+66'  },
  '81':  { name: 'Japão',                   flag: '🇯🇵', ddi: '+81'  },
  '82':  { name: 'Coreia do Sul',           flag: '🇰🇷', ddi: '+82'  },
  '84':  { name: 'Vietname',                flag: '🇻🇳', ddi: '+84'  },
  '86':  { name: 'China',                   flag: '🇨🇳', ddi: '+86'  },
  '90':  { name: 'Turquia',                 flag: '🇹🇷', ddi: '+90'  },
  '91':  { name: 'Índia',                   flag: '🇮🇳', ddi: '+91'  },
  '92':  { name: 'Paquistão',               flag: '🇵🇰', ddi: '+92'  },
  '94':  { name: 'Sri Lanka',               flag: '🇱🇰', ddi: '+94'  },
  '98':  { name: 'Irão',                    flag: '🇮🇷', ddi: '+98'  },
  '212': { name: 'Marrocos',                flag: '🇲🇦', ddi: '+212' },
  '213': { name: 'Argélia',                 flag: '🇩🇿', ddi: '+213' },
  '216': { name: 'Tunísia',                 flag: '🇹🇳', ddi: '+216' },
  '218': { name: 'Líbia',                   flag: '🇱🇾', ddi: '+218' },
  '220': { name: 'Gâmbia',                  flag: '🇬🇲', ddi: '+220' },
  '221': { name: 'Senegal',                 flag: '🇸🇳', ddi: '+221' },
  '222': { name: 'Mauritânia',              flag: '🇲🇷', ddi: '+222' },
  '223': { name: 'Mali',                    flag: '🇲🇱', ddi: '+223' },
  '224': { name: 'Guiné',                   flag: '🇬🇳', ddi: '+224' },
  '225': { name: 'Costa do Marfim',         flag: '🇨🇮', ddi: '+225' },
  '226': { name: 'Burkina Faso',            flag: '🇧🇫', ddi: '+226' },
  '227': { name: 'Níger',                   flag: '🇳🇪', ddi: '+227' },
  '228': { name: 'Togo',                    flag: '🇹🇬', ddi: '+228' },
  '229': { name: 'Benim',                   flag: '🇧🇯', ddi: '+229' },
  '230': { name: 'Maurícia',                flag: '🇲🇺', ddi: '+230' },
  '231': { name: 'Libéria',                 flag: '🇱🇷', ddi: '+231' },
  '232': { name: 'Serra Leoa',              flag: '🇸🇱', ddi: '+232' },
  '233': { name: 'Gana',                    flag: '🇬🇭', ddi: '+233' },
  '234': { name: 'Nigéria',                 flag: '🇳🇬', ddi: '+234' },
  '235': { name: 'Chade',                   flag: '🇹🇩', ddi: '+235' },
  '236': { name: 'Rep. Centro-Africana',    flag: '🇨🇫', ddi: '+236' },
  '237': { name: 'Camarões',                flag: '🇨🇲', ddi: '+237' },
  '238': { name: 'Cabo Verde',              flag: '🇨🇻', ddi: '+238' },
  '239': { name: 'São Tomé e Príncipe',     flag: '🇸🇹', ddi: '+239' },
  '240': { name: 'Guiné Equatorial',        flag: '🇬🇶', ddi: '+240' },
  '241': { name: 'Gabão',                   flag: '🇬🇦', ddi: '+241' },
  '242': { name: 'Congo',                   flag: '🇨🇬', ddi: '+242' },
  '243': { name: 'RD Congo',                flag: '🇨🇩', ddi: '+243' },
  '244': { name: 'Angola',                  flag: '🇦🇴', ddi: '+244' },
  '245': { name: 'Guiné-Bissau',            flag: '🇬🇼', ddi: '+245' },
  '248': { name: 'Seicheles',               flag: '🇸🇨', ddi: '+248' },
  '249': { name: 'Sudão',                   flag: '🇸🇩', ddi: '+249' },
  '250': { name: 'Ruanda',                  flag: '🇷🇼', ddi: '+250' },
  '251': { name: 'Etiópia',                 flag: '🇪🇹', ddi: '+251' },
  '252': { name: 'Somália',                 flag: '🇸🇴', ddi: '+252' },
  '253': { name: 'Djibouti',                flag: '🇩🇯', ddi: '+253' },
  '254': { name: 'Quénia',                  flag: '🇰🇪', ddi: '+254' },
  '255': { name: 'Tanzânia',                flag: '🇹🇿', ddi: '+255' },
  '256': { name: 'Uganda',                  flag: '🇺🇬', ddi: '+256' },
  '257': { name: 'Burundi',                 flag: '🇧🇮', ddi: '+257' },
  '258': { name: 'Moçambique',              flag: '🇲🇿', ddi: '+258' },
  '260': { name: 'Zâmbia',                  flag: '🇿🇲', ddi: '+260' },
  '261': { name: 'Madagáscar',              flag: '🇲🇬', ddi: '+261' },
  '262': { name: 'Reunião',                 flag: '🇷🇪', ddi: '+262' },
  '263': { name: 'Zimbabué',                flag: '🇿🇼', ddi: '+263' },
  '264': { name: 'Namíbia',                 flag: '🇳🇦', ddi: '+264' },
  '265': { name: 'Malawi',                  flag: '🇲🇼', ddi: '+265' },
  '266': { name: 'Lesoto',                  flag: '🇱🇸', ddi: '+266' },
  '267': { name: 'Botswana',                flag: '🇧🇼', ddi: '+267' },
  '268': { name: 'Essuatíni',               flag: '🇸🇿', ddi: '+268' },
  '269': { name: 'Comores',                 flag: '🇰🇲', ddi: '+269' },
  '291': { name: 'Eritreia',                flag: '🇪🇷', ddi: '+291' },
  '297': { name: 'Aruba',                   flag: '🇦🇼', ddi: '+297' },
  '298': { name: 'Ilhas Faroé',             flag: '🇫🇴', ddi: '+298' },
  '299': { name: 'Gronelândia',             flag: '🇬🇱', ddi: '+299' },
  '350': { name: 'Gibraltar',               flag: '🇬🇮', ddi: '+350' },
  '351': { name: 'Portugal',                flag: '🇵🇹', ddi: '+351' },
  '352': { name: 'Luxemburgo',              flag: '🇱🇺', ddi: '+352' },
  '353': { name: 'Irlanda',                 flag: '🇮🇪', ddi: '+353' },
  '354': { name: 'Islândia',                flag: '🇮🇸', ddi: '+354' },
  '355': { name: 'Albânia',                 flag: '🇦🇱', ddi: '+355' },
  '356': { name: 'Malta',                   flag: '🇲🇹', ddi: '+356' },
  '357': { name: 'Chipre',                  flag: '🇨🇾', ddi: '+357' },
  '358': { name: 'Finlândia',               flag: '🇫🇮', ddi: '+358' },
  '359': { name: 'Bulgária',                flag: '🇧🇬', ddi: '+359' },
  '370': { name: 'Lituânia',                flag: '🇱🇹', ddi: '+370' },
  '371': { name: 'Letónia',                 flag: '🇱🇻', ddi: '+371' },
  '372': { name: 'Estónia',                 flag: '🇪🇪', ddi: '+372' },
  '373': { name: 'Moldávia',                flag: '🇲🇩', ddi: '+373' },
  '374': { name: 'Arménia',                 flag: '🇦🇲', ddi: '+374' },
  '375': { name: 'Bielorrússia',            flag: '🇧🇾', ddi: '+375' },
  '376': { name: 'Andorra',                 flag: '🇦🇩', ddi: '+376' },
  '377': { name: 'Mónaco',                  flag: '🇲🇨', ddi: '+377' },
  '378': { name: 'São Marino',              flag: '🇸🇲', ddi: '+378' },
  '380': { name: 'Ucrânia',                 flag: '🇺🇦', ddi: '+380' },
  '381': { name: 'Sérvia',                  flag: '🇷🇸', ddi: '+381' },
  '382': { name: 'Montenegro',              flag: '🇲🇪', ddi: '+382' },
  '383': { name: 'Kosovo',                  flag: '🇽🇰', ddi: '+383' },
  '385': { name: 'Croácia',                 flag: '🇭🇷', ddi: '+385' },
  '386': { name: 'Eslovénia',               flag: '🇸🇮', ddi: '+386' },
  '387': { name: 'Bósnia e Herzegovina',    flag: '🇧🇦', ddi: '+387' },
  '389': { name: 'Macedónia do Norte',      flag: '🇲🇰', ddi: '+389' },
  '420': { name: 'República Checa',         flag: '🇨🇿', ddi: '+420' },
  '421': { name: 'Eslováquia',              flag: '🇸🇰', ddi: '+421' },
  '423': { name: 'Liechtenstein',           flag: '🇱🇮', ddi: '+423' },
  '500': { name: 'Malvinas',                flag: '🇫🇰', ddi: '+500' },
  '501': { name: 'Belize',                  flag: '🇧🇿', ddi: '+501' },
  '502': { name: 'Guatemala',               flag: '🇬🇹', ddi: '+502' },
  '503': { name: 'El Salvador',             flag: '🇸🇻', ddi: '+503' },
  '504': { name: 'Honduras',                flag: '🇭🇳', ddi: '+504' },
  '505': { name: 'Nicarágua',               flag: '🇳🇮', ddi: '+505' },
  '506': { name: 'Costa Rica',              flag: '🇨🇷', ddi: '+506' },
  '507': { name: 'Panamá',                  flag: '🇵🇦', ddi: '+507' },
  '509': { name: 'Haiti',                   flag: '🇭🇹', ddi: '+509' },
  '591': { name: 'Bolívia',                 flag: '🇧🇴', ddi: '+591' },
  '592': { name: 'Guiana',                  flag: '🇬🇾', ddi: '+592' },
  '593': { name: 'Equador',                 flag: '🇪🇨', ddi: '+593' },
  '594': { name: 'Guiana Francesa',         flag: '🇬🇫', ddi: '+594' },
  '595': { name: 'Paraguai',                flag: '🇵🇾', ddi: '+595' },
  '596': { name: 'Martinica',               flag: '🇲🇶', ddi: '+596' },
  '597': { name: 'Suriname',                flag: '🇸🇷', ddi: '+597' },
  '598': { name: 'Uruguai',                 flag: '🇺🇾', ddi: '+598' },
  '599': { name: 'Curaçao',                 flag: '🇨🇼', ddi: '+599' },
  '673': { name: 'Brunei',                  flag: '🇧🇳', ddi: '+673' },
  '674': { name: 'Nauru',                   flag: '🇳🇷', ddi: '+674' },
  '675': { name: 'Papua-Nova Guiné',        flag: '🇵🇬', ddi: '+675' },
  '676': { name: 'Tonga',                   flag: '🇹🇴', ddi: '+676' },
  '677': { name: 'Ilhas Salomão',           flag: '🇸🇧', ddi: '+677' },
  '678': { name: 'Vanuatu',                 flag: '🇻🇺', ddi: '+678' },
  '679': { name: 'Fiji',                    flag: '🇫🇯', ddi: '+679' },
  '680': { name: 'Palau',                   flag: '🇵🇼', ddi: '+680' },
  '681': { name: 'Wallis e Futuna',         flag: '🇼🇫', ddi: '+681' },
  '682': { name: 'Ilhas Cook',              flag: '🇨🇰', ddi: '+682' },
  '683': { name: 'Niue',                    flag: '🇳🇺', ddi: '+683' },
  '685': { name: 'Samoa',                   flag: '🇼🇸', ddi: '+685' },
  '686': { name: 'Kiribati',                flag: '🇰🇮', ddi: '+686' },
  '687': { name: 'Nova Caledónia',          flag: '🇳🇨', ddi: '+687' },
  '688': { name: 'Tuvalu',                  flag: '🇹🇻', ddi: '+688' },
  '689': { name: 'Polinésia Francesa',      flag: '🇵🇫', ddi: '+689' },
  '690': { name: 'Tokelau',                 flag: '🇹🇰', ddi: '+690' },
  '691': { name: 'Micronésia',              flag: '🇫🇲', ddi: '+691' },
  '692': { name: 'Ilhas Marshall',          flag: '🇲🇭', ddi: '+692' },
  '850': { name: 'Coreia do Norte',         flag: '🇰🇵', ddi: '+850' },
  '852': { name: 'Hong Kong',               flag: '🇭🇰', ddi: '+852' },
  '853': { name: 'Macau',                   flag: '🇲🇴', ddi: '+853' },
  '855': { name: 'Camboja',                 flag: '🇰🇭', ddi: '+855' },
  '856': { name: 'Laos',                    flag: '🇱🇦', ddi: '+856' },
  '880': { name: 'Bangladesh',              flag: '🇧🇩', ddi: '+880' },
  '886': { name: 'Taiwan',                  flag: '🇹🇼', ddi: '+886' },
  '960': { name: 'Maldivas',                flag: '🇲🇻', ddi: '+960' },
  '961': { name: 'Líbano',                  flag: '🇱🇧', ddi: '+961' },
  '962': { name: 'Jordânia',                flag: '🇯🇴', ddi: '+962' },
  '963': { name: 'Síria',                   flag: '🇸🇾', ddi: '+963' },
  '964': { name: 'Iraque',                  flag: '🇮🇶', ddi: '+964' },
  '965': { name: 'Kuwait',                  flag: '🇰🇼', ddi: '+965' },
  '966': { name: 'Arábia Saudita',          flag: '🇸🇦', ddi: '+966' },
  '967': { name: 'Iémen',                   flag: '🇾🇪', ddi: '+967' },
  '968': { name: 'Omã',                     flag: '🇴🇲', ddi: '+968' },
  '971': { name: 'Emirados Árabes Unidos',  flag: '🇦🇪', ddi: '+971' },
  '972': { name: 'Israel',                  flag: '🇮🇱', ddi: '+972' },
  '973': { name: 'Bahrein',                 flag: '🇧🇭', ddi: '+973' },
  '974': { name: 'Catar',                   flag: '🇶🇦', ddi: '+974' },
  '975': { name: 'Butão',                   flag: '🇧🇹', ddi: '+975' },
  '976': { name: 'Mongólia',                flag: '🇲🇳', ddi: '+976' },
  '977': { name: 'Nepal',                   flag: '🇳🇵', ddi: '+977' },
  '992': { name: 'Tajiquistão',             flag: '🇹🇯', ddi: '+992' },
  '993': { name: 'Turquemenistão',          flag: '🇹🇲', ddi: '+993' },
  '994': { name: 'Azerbaijão',              flag: '🇦🇿', ddi: '+994' },
  '995': { name: 'Geórgia',                 flag: '🇬🇪', ddi: '+995' },
  '996': { name: 'Quirguistão',             flag: '🇰🇬', ddi: '+996' },
  '998': { name: 'Uzbequistão',             flag: '🇺🇿', ddi: '+998' },
};

function parsePhoneNumber(userId) {
  const number      = userId.replace('@c.us', '');
  const sortedCodes = Object.keys(COUNTRY_CODES).sort((a, b) => b.length - a.length);
  let country = null;
  let code    = '';
  for (const prefix of sortedCodes) {
    if (number.startsWith(prefix)) {
      country = COUNTRY_CODES[prefix];
      code    = prefix;
      break;
    }
  }
  const localNumber = number.slice(code.length);
  return {
    raw: number,
    code,
    localNumber,
    country:     country || { name: `+${code || '??'}`, flag: '🌐', ddi: `+${code}` },
    fullNumber:  number,
    formatted:   country
      ? `${country.flag} ${country.name} · ${localNumber}`
      : `🌐 +${code} ${localNumber}`
  };
}

// ========== PERSISTENCIA DE CODIGOS GERADOS ==========
async function saveGeneratedCode(groupId, duration, codeData) {
  const key  = `${groupId}_${duration}`;
  const data = { ...codeData, groupId, duration, generatedAt: new Date().toISOString() };
  generatedCodesCache[key] = data;
  try { fs.writeFileSync(GENERATED_CODES_FILE, JSON.stringify(generatedCodesCache, null, 2)); } catch (err) { console.error('Erro JSON:', err.message); }
  if (redisInitialized && redis) {
    try {
      await redis.set(`generated_code:${key}`, JSON.stringify(data));
      const dur = parseDurationToMs(duration);
      if (dur > 0) await redis.expire(`generated_code:${key}`, Math.ceil(dur / 1000));
    } catch (err) { console.error('Erro Redis:', err.message); }
  }
  if (firebaseInitialized && db) {
    try { await db.collection('generated_codes').doc(key.replace(/[/.@]/g, '_')).set({ ...data, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }); } catch (err) { console.error('Erro Firebase:', err.message); }
  }
  return data;
}

async function loadGeneratedCodesFromFirebase() {
  if (!firebaseInitialized || !db) {
    try { if (fs.existsSync(GENERATED_CODES_FILE)) { generatedCodesCache = JSON.parse(fs.readFileSync(GENERATED_CODES_FILE, 'utf8')); console.log('Códigos carregados do JSON:', Object.keys(generatedCodesCache).length); } } catch (err) { console.error('Erro JSON:', err.message); }
    return;
  }
  try {
    const snapshot = await db.collection('generated_codes').get();
    snapshot.forEach(doc => { const d = doc.data(); generatedCodesCache[`${d.groupId}_${d.duration}`] = d; });
    fs.writeFileSync(GENERATED_CODES_FILE, JSON.stringify(generatedCodesCache, null, 2));
    console.log('Códigos carregados do Firebase:', Object.keys(generatedCodesCache).length);
  } catch (err) { console.error('Erro Firebase:', err.message); }
}

// ========== FUNÇÕES FIREBASE ==========
async function loadGroupsFromFirebase() {
  if (!firebaseInitialized) {
    try { groupConfig = JSON.parse(fs.readFileSync('./groups.json', 'utf8')); console.log('Grupos carregados local:', Object.keys(groupConfig).length); } catch { groupConfig = {}; }
    return;
  }
  try {
    const snapshot = await db.collection('whatsapp').get();
    groupConfig = {};
    snapshot.forEach(doc => { const d = doc.data(); groupConfig[d.groupId || doc.id] = { ...d, id: d.groupId || doc.id }; });
    console.log('Grupos carregados Firebase:', Object.keys(groupConfig).length);
  } catch (err) { console.error('Erro grupos:', err.message); }
}

async function saveGroupToFirebase(groupId, data) {
  if (!firebaseInitialized) { groupConfig[groupId] = { ...data, id: groupId }; fs.writeFileSync('./groups.json', JSON.stringify(groupConfig, null, 2)); return true; }
  try {
    const docRef = db.collection('whatsapp').doc(groupId.replace(/[/.]/g, '_'));
    await docRef.set({ groupId, ...data, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    groupConfig[groupId] = { ...data, id: groupId };
    return true;
  } catch (err) { console.error('Erro salvar grupo:', err.message); return false; }
}

async function deleteGroupFromFirebase(groupId) {
  if (!firebaseInitialized) { delete groupConfig[groupId]; fs.writeFileSync('./groups.json', JSON.stringify(groupConfig, null, 2)); return true; }
  try { await db.collection('whatsapp').doc(groupId.replace(/[/.]/g, '_')).delete(); delete groupConfig[groupId]; inviteCodeCache.delete(groupId); return true; } catch (err) { console.error('Erro deletar grupo:', err.message); return false; }
}

// ========== PENDENTES ==========
async function savePendingRemoval(groupId, memberId, data) {
  if (!firebaseInitialized) return;
  try { await db.collection('pending_removals').doc(`${groupId.replace(/[/.]/g, '_')}_${memberId.replace(/[/.]/g, '_')}`).set({ groupId, memberId, ...data, createdAt: admin.firestore.FieldValue.serverTimestamp() }); } catch (err) { console.error('Erro pending:', err.message); }
}

async function deletePendingRemoval(groupId, memberId) {
  if (!firebaseInitialized) return;
  try { await db.collection('pending_removals').doc(`${groupId.replace(/[/.]/g, '_')}_${memberId.replace(/[/.]/g, '_')}`).delete(); } catch (err) { console.error('Erro delete pending:', err.message); }
}

async function loadPendingRemovals() {
  if (!firebaseInitialized || !isReady) return;
  try {
    const now      = new Date();
    const snapshot = await db.collection('pending_removals').get();
    console.log(`Carregando ${snapshot.size} remocoes pendentes...`);
    for (const doc of snapshot.docs) {
      const data        = doc.data();
      const remainingMs = new Date(data.expiresAt).getTime() - now.getTime();
      if (remainingMs <= 0) {
        await executeRemoval(data.groupId, data.memberId, data.userName, data.groupName);
        await deletePendingRemoval(data.groupId, data.memberId);
      } else {
        try {
          const chat = await client.getChatById(data.groupId);
          const info = groupConfig[data.groupId] || { name: data.groupName };
          scheduleRemovalInternal(chat, data.memberId, data.userName, remainingMs, info, false);
          console.log(`Remocao reprogramada: ${data.userName} em ${Math.round(remainingMs / 3600000)}h`);
        } catch (err) { console.error(`Erro reprogramar: ${err.message}`); }
      }
    }
  } catch (err) { console.error('Erro pending removals:', err.message); }
}

// ========== NOTIFICAÇÕES E LOGS ==========
async function sendAdminNotification(message) {
  for (const number of ALL_NOTIFICATION_NUMBERS) {
    if (number && isReady && client) {
      try { await client.sendMessage(`${number}@c.us`, message); } catch (err) { console.error(`Erro notificar ${number}:`, err.message); }
    }
  }
  if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) {
    try {
      await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message, parse_mode: 'HTML' })
      });
    } catch (err) { console.error('Erro Telegram:', err.message); }
  }
}

async function logActivity(type, data) {
  console.log(`[${type}] ${JSON.stringify(data)}`);
  if (!firebaseInitialized) return;
  try { await db.collection('activity_logs').add({ type, ...data, timestamp: admin.firestore.FieldValue.serverTimestamp() }); } catch (err) { console.error('Erro log:', err.message); }
}

async function saveMemberToFirebase(groupId, memberId, data) {
  if (!firebaseInitialized) return;
  try { await db.collection('members').doc(`${groupId.replace(/[/.]/g, '_')}_${memberId.replace(/[/.]/g, '_')}`).set({ groupId, memberId, ...data, status: 'active', createdAt: admin.firestore.FieldValue.serverTimestamp() }); } catch (err) { console.error('Erro salvar membro:', err.message); }
}

async function updateMemberStatus(groupId, memberId, status) {
  if (!firebaseInitialized) return;
  try { await db.collection('members').doc(`${groupId.replace(/[/.]/g, '_')}_${memberId.replace(/[/.]/g, '_')}`).update({ status, updatedAt: admin.firestore.FieldValue.serverTimestamp() }); } catch (err) { console.error('Erro atualizar membro:', err.message); }
}

// Carregar dados iniciais
await loadGroupsFromFirebase();
await loadGeneratedCodesFromFirebase();

// ========== KEEPALIVE E HEALTH CHECK ==========
function startKeepalive() {
  if (keepaliveInterval) clearInterval(keepaliveInterval);
  keepaliveInterval = setInterval(async () => {
    if (client && isReady) { try { await client.sendSeen('status@broadcast'); } catch (err) {} }
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

// ========== EXECUÇÃO DE REMOÇÃO ==========
async function executeRemoval(groupId, userId, userName, groupName) {
  try {
    const chat = await client.getChatById(groupId);
    await chat.sendMessage(`Olá @${userId.split('@')[0]}, seu tempo no grupo "${groupName}" expirou. Obrigado!`);
    await chat.removeParticipants([userId]);
    console.log(`${userName} removido do grupo ${groupName}`);
    await updateMemberStatus(groupId, userId, 'removed');
    await logActivity('member_removed', { userId, userName, groupId, groupName });
    await sendAdminNotification(`🔹 Membro removido:\n- Nome: ${userName}\n- Grupo: ${groupName}`);
  } catch (err) { console.error(`Erro ao remover ${userId}:`, err.message); await logActivity('removal_error', { userId, groupId, error: err.message }); }
}

// ========== AGENDAR REMOÇÃO ==========
function scheduleRemovalInternal(chat, userId, userName, durationMs, groupInfo, saveToDB = true) {
  const key = `${chat.id._serialized}_${userId}`;
  if (activeTimeouts.has(key)) {
    const existing = activeTimeouts.get(key);
    if (existing.main)       clearTimeout(existing.main);
    if (existing.warning24h) clearTimeout(existing.warning24h);
    if (existing.warning1h)  clearTimeout(existing.warning1h);
    activeTimeouts.delete(key);
  }
  const expiresAt = new Date(Date.now() + durationMs);
  const timeouts  = {};
  if (durationMs > 86400000) {
    timeouts.warning24h = setTimeout(async () => {
      try { await chat.sendMessage(`⚠️ @${userId.split('@')[0]}, seu acesso ao grupo "${groupInfo.name}" expira em 24 horas!`); } catch (err) {}
    }, durationMs - 86400000);
  }
  if (durationMs > 3600000) {
    timeouts.warning1h = setTimeout(async () => {
      try { await chat.sendMessage(`🚨 @${userId.split('@')[0]}, seu acesso ao grupo "${groupInfo.name}" expira em 1 hora!`); } catch (err) {}
    }, durationMs - 3600000);
  }
  timeouts.main = setTimeout(async () => {
    await executeRemoval(chat.id._serialized, userId, userName, groupInfo.name);
    activeTimeouts.delete(key);
    await deletePendingRemoval(chat.id._serialized, userId);
  }, durationMs);
  activeTimeouts.set(key, timeouts);
  if (saveToDB) savePendingRemoval(chat.id._serialized, userId, { userName, groupName: groupInfo.name, durationMs, expiresAt: expiresAt.toISOString() });
  console.log(`Remoção agendada: ${userName} em ${expiresAt.toLocaleString()}`);
}

function scheduleRemoval(chat, userId, userName, durationMs, groupInfo) {
  scheduleRemovalInternal(chat, userId, userName, durationMs, groupInfo, true);
}

// ========== COMANDOS ADMIN ==========
async function handleAdminCommand(message) {
  const from = message.from.replace('@c.us', '');
  if (!ADMIN_PRINCIPAL_LIST.includes(from)) return;
  const body = message.body.trim();
  if (!body.startsWith('/')) {
    const menu =
      `🤖 *TRADER BOT – Menu de Comandos*\n\n` +
      `📊 *Status e Informações*\n` +
      `/status – Status do bot\n/meunumero – Número do bot\n/grupos – Grupos configurados\n/grupoinfo <id> – Info detalhada\n\n` +
      `👥 *Membros*\n` +
      `/membros – Últimos membros\n/membros <id> – Membros de um grupo\n/membroinfo <userId> – Info do membro\n\n` +
      `⏰ *Acesso e Remoções*\n` +
      `/pendentes – Remoções agendadas\n/cancelar <g> <u> – Cancela remoção\n/adiar <g> <u> <dias> – Adia remoção\n/prolongar <g> <u> <dias> – Prolonga\n/remover <g> <u> – Remove agora\n\n` +
      `🔗 *Convites*\n` +
      `/convite <g> <dur> – Gerar convite\n/afiliado <g> <dur> <cod> – Convite afiliado\n\n` +
      `⚙️ *Configurações*\n` +
      `/editarmsg <g> welcome|expiry <msg>\n/nivel <g> normal|vip\n/desativar <g> – Pausar grupo\n/ativar <g> – Reativar grupo\n\n` +
      `🛡️ *Banimentos*\n` +
      `/banir <g> <u> – Banir usuário\n/banidos – Lista banidos\n/desbanir <u> – Remover ban\n\n` +
      `🔧 *Sistema*\n` +
      `/reiniciar – Reconectar\n/limparCache – Limpar cache\n/sair – Encerrar bot\n\n` +
      `📝 *Exemplos:*\n/convite 123456@g.us 30d\n/membroinfo 5511999999999@c.us`;
    await message.reply(menu);
    return;
  }

  const parts    = body.split(/\s+/);
  const cmd      = parts[0].toLowerCase();
  const response = [];

  try {
    switch (cmd) {
      case '/convite': {
        const gid = parts[1], dur = parts[2];
        if (!gid || !dur) { response.push('❌ Uso: /convite <groupId> <duracao>\nEx: /convite 123456@g.us 30d'); break; }
        if (!isReady) { response.push('❌ Bot não conectado'); break; }
        try {
          const chat = await client.getChatById(gid);
          if (!chat.isGroup) { response.push('❌ ID não é grupo'); break; }
          const code = await chat.getInviteCode();
          response.push('✅ *Convite Gerado!*');
          response.push(`🔗 https://chat.whatsapp.com/${code}`);
          response.push(`📌 Grupo: ${chat.name}`);
          response.push(`⏳ Duração: ${getDurationLabel(dur)}`);
        } catch (e) { response.push(`❌ Erro: ${e.message}`); }
        break;
      }
      case '/afiliado': {
        const gid = parts[1], dur = parts[2], affCode = parts[3];
        if (!gid || !dur || !affCode) { response.push('❌ Uso: /afiliado <groupId> <duracao> <codigo>'); break; }
        if (!isReady) { response.push('❌ Bot não conectado'); break; }
        try {
          const chat = await client.getChatById(gid);
          if (!chat.isGroup) { response.push('❌ ID não é grupo'); break; }
          const code = await chat.getInviteCode();
          if (firebaseInitialized) {
            await db.collection('affiliates').add({ affiliateCode: affCode, groupId: gid, duration: dur, inviteLink: `https://chat.whatsapp.com/${code}`, generatedAt: admin.firestore.FieldValue.serverTimestamp(), used: false });
          }
          response.push('✅ *Convite Afiliado Gerado!*');
          response.push(`🔗 https://chat.whatsapp.com/${code}`);
          response.push(`👤 Afiliado: ${affCode}`);
          response.push(`📌 ${chat.name} – ${getDurationLabel(dur)}`);
        } catch (e) { response.push(`❌ Erro: ${e.message}`); }
        break;
      }
      case '/pendentes': {
        const pendingList = [];
        for (const [key, timeouts] of activeTimeouts.entries()) {
          const [gid, uid] = key.split('_');
          const exp        = new Date(Date.now() + (timeouts.main ? timeouts.main._idleTimeout : 0));
          const name       = groupConfig[gid]?.name || gid.substring(0, 15);
          const parsed     = parsePhoneNumber(uid);
          pendingList.push(`• ${parsed.formatted} → ${name} (${Math.round((exp - Date.now()) / 3600000)}h restantes)`);
        }
        if (pendingList.length === 0) response.push('📭 Nenhuma remoção agendada.');
        else response.push(`⏰ *Remoções Agendadas (${pendingList.length})*\n${pendingList.join('\n')}`);
        break;
      }
      case '/cancelar': {
        const gid = parts[1], uid = parts[2];
        if (!gid || !uid) { response.push('❌ Uso: /cancelar <groupId> <userId>'); break; }
        const key = `${gid}_${uid}`;
        if (activeTimeouts.has(key)) {
          const t = activeTimeouts.get(key);
          if (t.main) clearTimeout(t.main); if (t.warning24h) clearTimeout(t.warning24h); if (t.warning1h) clearTimeout(t.warning1h);
          activeTimeouts.delete(key);
          await deletePendingRemoval(gid, uid);
          response.push(`✅ Remoção de ${uid.split('@')[0]} cancelada.`);
        } else response.push('❌ Remoção não encontrada.');
        break;
      }
      case '/adiar': {
        const gid = parts[1], uid = parts[2], dias = parseInt(parts[3]);
        if (!gid || !uid || !dias) { response.push('❌ Uso: /adiar <groupId> <userId> <dias>'); break; }
        const key = `${gid}_${uid}`;
        if (activeTimeouts.has(key)) {
          const t = activeTimeouts.get(key);
          if (t.main) clearTimeout(t.main); if (t.warning24h) clearTimeout(t.warning24h); if (t.warning1h) clearTimeout(t.warning1h);
          activeTimeouts.delete(key);
          await deletePendingRemoval(gid, uid);
          try {
            const chat = await client.getChatById(gid);
            const info = groupConfig[gid] || { name: 'Grupo' };
            scheduleRemovalInternal(chat, uid, uid.split('@')[0], dias * 86400000, info, true);
            response.push(`✅ Remoção de ${uid.split('@')[0]} adiada em ${dias} dias.`);
          } catch (e) { response.push(`❌ Erro: ${e.message}`); }
        } else response.push('❌ Remoção não encontrada.');
        break;
      }
      case '/grupoinfo': {
        const gid  = parts[1];
        if (!gid) { response.push('❌ Uso: /grupoinfo <groupId>'); break; }
        const info = groupConfig[gid];
        if (!info) { response.push('❌ Grupo não configurado.'); break; }
        response.push(`📌 *${info.name}*`);
        response.push(`🆔 ${gid}`);
        response.push(`⭐ Nível: ${info.level || 'normal'}`);
        response.push(`⏳ Duração padrão: ${(info.durationMs || 0) / 86400000} dias`);
        response.push(`🚫 Desativado: ${info.disabled ? 'Sim' : 'Não'}`);
        response.push(`💬 Welcome: ${info.welcomeMessage || 'Padrão'}`);
        response.push(`⏰ Expiry: ${info.expiryMessage || 'Padrão'}`);
        break;
      }
      case '/editarmsg': {
        const gid = parts[1], tipo = parts[2], msg = parts.slice(3).join(' ');
        if (!gid || !tipo || !msg) { response.push('❌ Uso: /editarmsg <groupId> welcome|expiry <mensagem>'); break; }
        if (!groupConfig[gid]) { response.push('❌ Grupo não configurado.'); break; }
        if (tipo === 'welcome')      { groupConfig[gid].welcomeMessage = msg; response.push('✅ Mensagem de boas-vindas atualizada!'); }
        else if (tipo === 'expiry') { groupConfig[gid].expiryMessage  = msg; response.push('✅ Mensagem de expiração atualizada!'); }
        else { response.push('❌ Tipo inválido. Use welcome ou expiry.'); break; }
        await saveGroupToFirebase(gid, groupConfig[gid]);
        break;
      }
      case '/nivel': {
        const gid = parts[1], nivel = parts[2];
        if (!gid || !nivel) { response.push('❌ Uso: /nivel <groupId> normal|vip'); break; }
        if (!groupConfig[gid]) { response.push('❌ Grupo não configurado.'); break; }
        if (!['normal', 'vip'].includes(nivel)) { response.push('❌ Nível inválido. Use normal ou vip.'); break; }
        groupConfig[gid].level = nivel;
        await saveGroupToFirebase(gid, groupConfig[gid]);
        response.push(`✅ Nível do grupo "${groupConfig[gid].name}" atualizado para ${nivel.toUpperCase()}!`);
        break;
      }
      case '/desativar': {
        const gid = parts[1];
        if (!gid) { response.push('❌ Uso: /desativar <groupId>'); break; }
        if (!groupConfig[gid]) { response.push('❌ Grupo não configurado.'); break; }
        groupConfig[gid].disabled = true;
        await saveGroupToFirebase(gid, groupConfig[gid]);
        response.push(`🚫 Grupo "${groupConfig[gid].name}" desativado.`);
        break;
      }
      case '/ativar': {
        const gid = parts[1];
        if (!gid) { response.push('❌ Uso: /ativar <groupId>'); break; }
        if (!groupConfig[gid]) { response.push('❌ Grupo não configurado.'); break; }
        delete groupConfig[gid].disabled;
        await saveGroupToFirebase(gid, groupConfig[gid]);
        response.push(`✅ Grupo "${groupConfig[gid].name}" reativado!`);
        break;
      }
      case '/prolongar': {
        const gid = parts[1], uid = parts[2], dias = parseInt(parts[3]);
        if (!gid || !uid || !dias) { response.push('❌ Uso: /prolongar <groupId> <userId> <dias>'); break; }
        const key = `${gid}_${uid}`;
        if (activeTimeouts.has(key)) {
          const t = activeTimeouts.get(key);
          if (t.main) clearTimeout(t.main); if (t.warning24h) clearTimeout(t.warning24h); if (t.warning1h) clearTimeout(t.warning1h);
          activeTimeouts.delete(key);
          await deletePendingRemoval(gid, uid);
        }
        try {
          const chat = await client.getChatById(gid);
          const info = groupConfig[gid] || { name: 'Grupo' };
          scheduleRemovalInternal(chat, uid, uid.split('@')[0], dias * 86400000, info, true);
          await updateMemberStatus(gid, uid, 'active');
          response.push(`✅ Acesso de ${uid.split('@')[0]} prolongado em ${dias} dias.`);
        } catch (e) { response.push(`❌ Erro: ${e.message}`); }
        break;
      }
      case '/membroinfo': {
        const uid = parts[1];
        if (!uid) { response.push('❌ Uso: /membroinfo <userId>'); break; }
        let found = null;
        for (const [key, timeouts] of activeTimeouts.entries()) {
          const [gid, memberId] = key.split('_');
          if (memberId === uid) { found = { groupId: gid, remaining: Math.round((Date.now() + (timeouts.main ? timeouts.main._idleTimeout : 0) - Date.now()) / 3600000) }; break; }
        }
        if (found) {
          const parsed = parsePhoneNumber(uid);
          response.push(`👤 *${parsed.formatted}*`);
          response.push(`📌 Grupo: ${groupConfig[found.groupId]?.name || found.groupId}`);
          response.push(`⏳ Tempo restante: ${found.remaining}h`);
        } else response.push('❌ Membro não encontrado nos agendamentos.');
        break;
      }
      case '/banir': {
        const gid = parts[1], uid = parts[2];
        if (!gid || !uid) { response.push('❌ Uso: /banir <groupId> <userId>'); break; }
        bannedUsers.set(uid, { groupId: gid, bannedAt: new Date().toISOString() });
        try { const chat = await client.getChatById(gid); await chat.removeParticipants([uid]); response.push(`🚫 Usuário ${uid.split('@')[0]} banido e removido.`); } catch (e) { response.push(`✅ Banido localmente. Erro ao remover: ${e.message}`); }
        break;
      }
      case '/banidos': {
        if (bannedUsers.size === 0) response.push('✅ Nenhum usuário banido.');
        else { response.push(`🚫 *Usuários Banidos (${bannedUsers.size})*`); for (const [uid, data] of bannedUsers.entries()) { response.push(`• ${uid.split('@')[0]} – Grupo: ${groupConfig[data.groupId]?.name || data.groupId}`); } }
        break;
      }
      case '/desbanir': {
        const uid = parts[1];
        if (!uid) { response.push('❌ Uso: /desbanir <userId>'); break; }
        if (bannedUsers.has(uid)) { bannedUsers.delete(uid); response.push(`✅ ${uid.split('@')[0]} foi desbanido.`); } else response.push('❌ Usuário não está na lista de banidos.');
        break;
      }
      case '/notificar': {
        const msg = parts.slice(1).join(' ');
        if (!msg) { response.push('❌ Uso: /notificar <mensagem>'); break; }
        await sendAdminNotification(`📢 *Notificação de teste:*\n${msg}`);
        response.push('✅ Notificação enviada para todos os admins.');
        break;
      }
      case '/meunumero': {
        if (client && isReady) { const info = client.info; response.push(`📱 *Número do Bot:* ${info?.wid?.user || 'Desconhecido'}`); }
        else response.push('❌ Bot não conectado.');
        break;
      }
      case '/status':
        response.push(`🤖 *Status do Bot*`);
        response.push(`• Conectado: ${isReady ? '✅' : '❌'}`);
        response.push(`• Uptime: ${Math.floor(process.uptime() / 3600)}h ${Math.floor((process.uptime() % 3600) / 60)}m`);
        response.push(`• Grupos: ${Object.keys(groupConfig).length} (${Object.keys(configuredGroupIds).length} config)`);
        response.push(`• Remoções ativas: ${activeTimeouts.size}`);
        response.push(`• Banidos: ${bannedUsers.size}`);
        response.push(`• Firebase: ${firebaseInitialized ? '✅' : '❌'} | Redis: ${redisInitialized ? '✅' : '❌'}`);
        break;
      case '/grupos':
        if (Object.keys(groupConfig).length === 0) response.push('Nenhum grupo configurado.');
        else { response.push('📌 *Grupos:*'); for (const [id, info] of Object.entries(groupConfig)) { response.push(`• ${info.name || 'Sem nome'} → ${id.substring(0, 20)}... (${(info.durationMs || 0) / 86400000}d) ${info.disabled ? '🚫' : ''}`); } }
        break;
      case '/membros':
        response.push('👥 Funcionalidade acessível via /membros <groupId>');
        break;
      case '/remover': {
        const gid = parts[1], uid = parts[2];
        if (!gid || !uid) { response.push('❌ Uso: /remover <groupId> <userId>'); break; }
        const info = groupConfig[gid]; if (!info) { response.push('❌ Grupo não encontrado.'); break; }
        await executeRemoval(gid, uid, 'Removido manualmente', info.name);
        response.push(`✅ Remoção de ${uid.split('@')[0]} solicitada.`);
        break;
      }
      case '/reiniciar':
        response.push('🔄 Reiniciando...');
        await sendAdminNotification(`Bot reiniciado por ${from}`);
        reconnectAttempts = 0;
        await initializeBot();
        response.push('✅ Reconexão concluída.');
        break;
      case '/limparCache':
        inviteCodeCache.clear();
        generatedCodesCache = {};
        if (fs.existsSync(GENERATED_CODES_FILE)) fs.unlinkSync(GENERATED_CODES_FILE);
        response.push('🧹 Cache limpo.');
        break;
      case '/sair':
        if (ADMIN_PRINCIPAL_LIST.includes(from)) { response.push('🛑 Encerrando...'); await sendAdminNotification(`Bot encerrado por ${from}`); setTimeout(() => process.exit(0), 1000); }
        else response.push('❌ Apenas admin principal.');
        break;
      default:
        if (cmd.startsWith('/')) response.push('❓ Comando não reconhecido. Envie qualquer mensagem para ver o menu.');
    }
  } catch (err) { console.error('Erro comando:', err); response.push('❌ Erro ao processar.'); }
  if (response.length > 0) await message.reply(response.join('\n'));
}

// ========== CLIENTE WHATSAPP ==========
function createClient() {
  const newClient = new Client({
    authStrategy: new LocalAuth({ dataPath: SESSION_DIR, clientId: SESSION_CLIENT_ID }),
    puppeteer: {
      headless: true,
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || path.join(__dirname, '.cache', 'puppeteer', 'chrome', 'linux-146.0.7680.31', 'chrome-linux64', 'chrome'),
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-accelerated-2d-canvas', '--no-first-run', '--no-zygote', '--disable-gpu', '--single-process']
    }
  });

  newClient.on('qr', async (qr) => {
    console.log('ESCANEIE O QR CODE:');
    qrcode.generate(qr, { small: true });
    currentQR = qr; isReady = false;
    await logActivity('qr_generated', { timestamp: new Date().toISOString() });
  });

  newClient.on('ready', async () => {
    console.log('WhatsApp conectado!'); isReady = true; currentQR = null; reconnectAttempts = 0;
    startKeepalive(); startHealthCheck();
    await logActivity('bot_connected', { timestamp: new Date().toISOString() });
    setTimeout(async () => { await loadGroupsFromFirebase(); await loadGeneratedCodesFromFirebase(); await loadPendingRemovals(); }, 5000);
    await sendAdminNotification('✅ Bot WhatsApp conectado e operacional!');
    setTimeout(() => backupSessionToFirestore(), 10000);
  });

  newClient.on('auth_failure', (msg) => { console.error('Falha autenticação:', msg); isReady = false; logActivity('auth_failure', { message: msg }); });

  newClient.on('disconnected', async (reason) => {
    console.error(`Desconectado: ${reason}`); isReady = false;
    if (keepaliveInterval) clearInterval(keepaliveInterval);
    if (healthCheckInterval) clearInterval(healthCheckInterval);
    await logActivity('bot_disconnected', { reason });
    await sendAdminNotification(`⚠️ Bot desconectado: ${reason}`);
    await backupSessionToFirestore();
    if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
      const delay = Math.min(5000 * Math.pow(1.5, reconnectAttempts), 60000); reconnectAttempts++;
      console.log(`Reconectando em ${delay / 1000}s...`); setTimeout(() => initializeBot(), delay);
    } else { console.error('Máx tentativas.'); await sendAdminNotification('ALERTA: Máx tentativas de reconexão!'); }
  });

  newClient.on('group_join', async (notification) => {
    try {
      const chat      = await notification.getChat();
      const groupId   = chat.id._serialized;
      const groupInfo = groupConfig[groupId];
      if (!groupInfo || groupInfo.disabled) { console.log(`Grupo ${chat.name} não configurado/desativado.`); return; }
      for (const recipientId of (notification.recipientIds || [])) {
        if (bannedUsers.has(recipientId)) { try { await chat.removeParticipants([recipientId]); console.log(`${recipientId} banido – removido.`); } catch (e) {} continue; }
        const contact  = await client.getContactById(recipientId);
        const userName = contact.pushname || contact.number || recipientId;
        const parsed   = parsePhoneNumber(recipientId);
        console.log(`${userName} entrou: ${chat.name} (${groupInfo.level} - ${groupInfo.durationMs / 86400000}d)`);
        const welcomeMsg = groupInfo.welcomeMessage || `Bem-vindo(a) ao *${groupInfo.name}*!\nAcesso: *${groupInfo.durationMs / 86400000} dia(s)*.`;
        await chat.sendMessage(welcomeMsg);
        await saveMemberToFirebase(groupId, recipientId, { userName, groupName: groupInfo.name, durationMs: groupInfo.durationMs, expirationDate: new Date(Date.now() + groupInfo.durationMs).toISOString() });
        await logActivity('member_joined', { userId: recipientId, userName, groupId, groupName: groupInfo.name });
        await sendAdminNotification(`🆕 ${parsed.formatted} → ${groupInfo.name} (${groupInfo.durationMs / 86400000}d)`);
        scheduleRemoval(chat, recipientId, userName, groupInfo.durationMs, groupInfo);
      }
    } catch (err) { console.error('Erro group_join:', err.message); }
  });

  newClient.on('message', async (message) => { if (message.from.includes('@g.us')) return; await handleAdminCommand(message); });
  return newClient;
}

// ========== INICIALIZAÇÃO ==========
async function initializeBot() {
  if (client) { try { await client.destroy(); } catch (e) {} }
  if (firebaseInitialized) await restoreSessionFromFirestore();
  client = createClient();
  await client.initialize();
}

// ========== MIDDLEWARE DE AUTENTICAÇÃO ==========
async function authMiddleware(req, res, next) {
  const authHeader = req.headers['authorization'];
  if (!authHeader || !authHeader.startsWith('Bearer ')) return res.status(401).json({ error: 'Token não fornecido' });
  try {
    if (firebaseInitialized && firebaseAuth) await firebaseAuth.verifyIdToken(authHeader.split('Bearer ')[1]);
    next();
  } catch (err) { res.status(401).json({ error: 'Token inválido' }); }
}

// ========== ROTAS ==========

// Página inicial / ping
app.get('/', (req, res) => res.status(200).send('OK'));

// Saúde básica
app.get('/health', (req, res) => res.json({ status: 'ok', ready: isReady, uptime: process.uptime() }));

// ── /api/health ─────────────────────────────────────────────────────────────
app.get('/api/health', (req, res) => {
  res.json({
    status:      isReady ? 'connected' : 'disconnected',
    ready:       isReady,
    uptime:      Math.floor(process.uptime()),
    groups:      Object.keys(groupConfig).length,
    activeTimers: activeTimeouts.size,
    firebase:    firebaseInitialized,
    redis:       redisInitialized,
    timestamp:   new Date().toISOString()
  });
});

// ── /api/status ──────────────────────────────────────────────────────────────
app.get('/api/status', (req, res) => {
  res.json({
    connected:    isReady,
    hasQR:        !!currentQR,
    reconnects:   reconnectAttempts,
    groups:       Object.keys(groupConfig).length,
    configuredGroups: Object.keys(configuredGroupIds).length,
    pendingRemovals:  activeTimeouts.size,
    bannedUsers:  bannedUsers.size,
    firebase:     firebaseInitialized,
    redis:        redisInitialized,
    uptime:       Math.floor(process.uptime())
  });
});

// ── /api/qr ──────────────────────────────────────────────────────────────────
app.get('/api/qr', async (req, res) => {
  if (!currentQR) return res.status(404).json({ error: isReady ? 'Bot já conectado' : 'QR não disponível ainda' });
  try {
    const format = req.query.format || 'png';
    if (format === 'json') return res.json({ qr: currentQR });
    const qrImage = await QRCode.toDataURL(currentQR);
    res.json({ qr: currentQR, image: qrImage });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── /api/configured-groups ───────────────────────────────────────────────────
app.get('/api/configured-groups', (req, res) => {
  res.json({ groups: configuredGroupIds, total: Object.keys(configuredGroupIds).length });
});

// ── /api/groups ──────────────────────────────────────────────────────────────
// Listar todos os grupos configurados no Firebase/local
app.get('/api/groups', authMiddleware, (req, res) => {
  const list = Object.entries(groupConfig).map(([id, info]) => ({
    id,
    name:        info.name        || 'Sem nome',
    level:       info.level       || 'normal',
    durationMs:  info.durationMs  || 0,
    durationDays: (info.durationMs || 0) / 86400000,
    disabled:    !!info.disabled,
    welcomeMessage: info.welcomeMessage || null,
    expiryMessage:  info.expiryMessage  || null,
  }));
  res.json({ groups: list, total: list.length });
});

// Criar/atualizar grupo
app.post('/api/groups', authMiddleware, async (req, res) => {
  const { groupId, name, durationDays, level, welcomeMessage, expiryMessage } = req.body;
  if (!groupId || !name || !durationDays) return res.status(400).json({ error: 'groupId, name e durationDays são obrigatórios' });
  const data = { name, durationMs: durationDays * 86400000, level: level || 'normal', welcomeMessage: welcomeMessage || null, expiryMessage: expiryMessage || null };
  const ok = await saveGroupToFirebase(groupId, data);
  if (ok) res.json({ success: true, group: { id: groupId, ...data } });
  else res.status(500).json({ error: 'Erro ao salvar grupo' });
});

// Remover grupo
app.delete('/api/groups/:groupId', authMiddleware, async (req, res) => {
  const groupId = decodeURIComponent(req.params.groupId);
  if (!groupConfig[groupId]) return res.status(404).json({ error: 'Grupo não encontrado' });
  const ok = await deleteGroupFromFirebase(groupId);
  if (ok) res.json({ success: true });
  else res.status(500).json({ error: 'Erro ao remover grupo' });
});

// ── /api/members ─────────────────────────────────────────────────────────────
// Listar membros ativos (via activeTimeouts + Firebase)
app.get('/api/members', authMiddleware, async (req, res) => {
  const { groupId, limit = 50 } = req.query;
  const activeList = [];
  for (const [key, timeouts] of activeTimeouts.entries()) {
    const [gid, uid]     = key.split('_');
    if (groupId && gid !== groupId) continue;
    const remainingMs    = timeouts.main ? timeouts.main._idleTimeout : 0;
    const parsed         = parsePhoneNumber(uid);
    activeList.push({
      userId:      uid.split('@')[0],
      groupId:     gid,
      groupName:   groupConfig[gid]?.name || gid,
      country:     parsed.country.name,
      flag:        parsed.country.flag,
      formatted:   parsed.formatted,
      daysLeft:    Math.max(0, Math.ceil(remainingMs / 86400000)),
      hoursLeft:   Math.max(0, Math.ceil(remainingMs / 3600000)),
      expiration:  new Date(Date.now() + remainingMs).toISOString(),
      source:      'memory'
    });
  }

  // Se tem Firebase e não filtrou o suficiente, complementa
  if (firebaseInitialized && db && activeList.length < parseInt(limit)) {
    try {
      let query = db.collection('members').where('status', '==', 'active').limit(parseInt(limit));
      if (groupId) query = query.where('groupId', '==', groupId);
      const snapshot = await query.get();
      snapshot.forEach(doc => {
        const d = doc.data();
        if (!activeList.find(m => m.userId === d.memberId?.split('@')[0])) {
          const exp    = new Date(d.expirationDate || Date.now());
          const parsed = parsePhoneNumber(d.memberId || '');
          activeList.push({
            userId:     d.memberId?.split('@')[0],
            groupId:    d.groupId,
            groupName:  d.groupName,
            country:    parsed.country.name,
            flag:       parsed.country.flag,
            formatted:  parsed.formatted,
            daysLeft:   Math.max(0, Math.ceil((exp - new Date()) / 86400000)),
            hoursLeft:  Math.max(0, Math.ceil((exp - new Date()) / 3600000)),
            expiration: exp.toISOString(),
            source:     'firebase'
          });
        }
      });
    } catch (err) { console.error('Erro listar membros Firebase:', err.message); }
  }

  res.json({ members: activeList.slice(0, parseInt(limit)), total: activeList.length });
});

// ── /api/stats ───────────────────────────────────────────────────────────────
app.get('/api/stats', authMiddleware, async (req, res) => {
  const stats = {
    groups:          Object.keys(groupConfig).length,
    activeMembers:   activeTimeouts.size,
    bannedUsers:     bannedUsers.size,
    cachedCodes:     Object.keys(generatedCodesCache).length,
    botReady:        isReady,
    uptime:          Math.floor(process.uptime()),
    firebase:        firebaseInitialized,
    redis:           redisInitialized,
    reconnectAttempts,
  };
  if (firebaseInitialized && db) {
    try {
      const [membersSnap, logsSnap] = await Promise.all([
        db.collection('members').where('status', '==', 'active').count().get(),
        db.collection('activity_logs').orderBy('timestamp', 'desc').limit(1).get(),
      ]);
      stats.totalActiveFirebase = membersSnap.data().count;
      stats.lastActivity        = logsSnap.docs[0]?.data()?.timestamp?.toDate()?.toISOString() || null;
    } catch (err) { console.error('Erro stats Firebase:', err.message); }
  }
  res.json(stats);
});

// ── /api/generate-invite ─────────────────────────────────────────────────────
app.post('/api/generate-invite', authMiddleware, async (req, res) => {
  const { groupId, duration, affiliateCode } = req.body;
  if (!groupId || !duration) return res.status(400).json({ error: 'groupId e duration são obrigatórios' });
  if (!isReady || !client) return res.status(503).json({ error: 'Bot não conectado' });
  try {
    const chat = await client.getChatById(groupId);
    if (!chat.isGroup) return res.status(400).json({ error: 'ID não é um grupo' });
    const code       = await chat.getInviteCode();
    const inviteLink = `https://chat.whatsapp.com/${code}`;
    const codeData   = await saveGeneratedCode(groupId, duration, { inviteLink, groupName: chat.name, affiliateCode: affiliateCode || null });
    if (affiliateCode && firebaseInitialized && db) {
      await db.collection('affiliates').add({ affiliateCode, groupId, duration, inviteLink, groupName: chat.name, generatedAt: admin.firestore.FieldValue.serverTimestamp(), used: false });
    }
    res.json({ success: true, inviteLink, groupName: chat.name, duration, affiliateCode: affiliateCode || null, generatedAt: codeData.generatedAt });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── /api/reconnect ───────────────────────────────────────────────────────────
app.post('/api/reconnect', authMiddleware, async (req, res) => {
  try {
    reconnectAttempts = 0;
    res.json({ success: true, message: 'Reconexão iniciada' });
    await initializeBot(); // após resposta
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── /api/clear-invite-cache ───────────────────────────────────────────────────
app.post('/api/clear-invite-cache', authMiddleware, (req, res) => {
  inviteCodeCache.clear();
  generatedCodesCache = {};
  try { if (fs.existsSync(GENERATED_CODES_FILE)) fs.unlinkSync(GENERATED_CODES_FILE); } catch (e) {}
  res.json({ success: true, message: 'Cache de convites limpo' });
});

// ── /api/command ─────────────────────────────────────────────────────────────
// Executar acções administrativas via API REST
app.post('/api/command', authMiddleware, async (req, res) => {
  const { action, groupId, userId, days, message, level } = req.body;
  if (!action) return res.status(400).json({ error: 'action é obrigatória' });
  if (!isReady || !client) return res.status(503).json({ error: 'Bot não conectado' });

  try {
    switch (action) {

      case 'remove': {
        if (!groupId || !userId) return res.status(400).json({ error: 'groupId e userId obrigatórios' });
        const info = groupConfig[groupId];
        if (!info) return res.status(404).json({ error: 'Grupo não configurado' });
        await executeRemoval(groupId, userId + '@c.us', 'Remoção via API', info.name);
        return res.json({ success: true });
      }

      case 'extend': {
        if (!groupId || !userId || !days) return res.status(400).json({ error: 'groupId, userId e days obrigatórios' });
        const uid = userId.includes('@') ? userId : userId + '@c.us';
        const key = `${groupId}_${uid}`;
        if (activeTimeouts.has(key)) {
          const t = activeTimeouts.get(key);
          if (t.main) clearTimeout(t.main); if (t.warning24h) clearTimeout(t.warning24h); if (t.warning1h) clearTimeout(t.warning1h);
          activeTimeouts.delete(key);
          await deletePendingRemoval(groupId, uid);
        }
        const chat = await client.getChatById(groupId);
        const info = groupConfig[groupId] || { name: 'Grupo' };
        scheduleRemovalInternal(chat, uid, uid.split('@')[0], parseInt(days) * 86400000, info, true);
        await updateMemberStatus(groupId, uid, 'active');
        return res.json({ success: true, message: `Acesso prolongado em ${days} dias` });
      }

      case 'cancel_removal': {
        if (!groupId || !userId) return res.status(400).json({ error: 'groupId e userId obrigatórios' });
        const uid = userId.includes('@') ? userId : userId + '@c.us';
        const key = `${groupId}_${uid}`;
        if (!activeTimeouts.has(key)) return res.status(404).json({ error: 'Remoção não encontrada' });
        const t = activeTimeouts.get(key);
        if (t.main) clearTimeout(t.main); if (t.warning24h) clearTimeout(t.warning24h); if (t.warning1h) clearTimeout(t.warning1h);
        activeTimeouts.delete(key);
        await deletePendingRemoval(groupId, uid);
        return res.json({ success: true });
      }

      case 'ban': {
        if (!groupId || !userId) return res.status(400).json({ error: 'groupId e userId obrigatórios' });
        const uid = userId.includes('@') ? userId : userId + '@c.us';
        bannedUsers.set(uid, { groupId, bannedAt: new Date().toISOString() });
        try { const chat = await client.getChatById(groupId); await chat.removeParticipants([uid]); } catch (e) {}
        return res.json({ success: true });
      }

      case 'unban': {
        if (!userId) return res.status(400).json({ error: 'userId obrigatório' });
        const uid = userId.includes('@') ? userId : userId + '@c.us';
        bannedUsers.delete(uid);
        return res.json({ success: true });
      }

      case 'send_message': {
        if (!groupId || !message) return res.status(400).json({ error: 'groupId e message obrigatórios' });
        const chat = await client.getChatById(groupId);
        await chat.sendMessage(message);
        return res.json({ success: true });
      }

      case 'set_level': {
        if (!groupId || !level) return res.status(400).json({ error: 'groupId e level obrigatórios' });
        if (!['normal', 'vip'].includes(level)) return res.status(400).json({ error: 'level inválido. Use normal ou vip' });
        if (!groupConfig[groupId]) return res.status(404).json({ error: 'Grupo não configurado' });
        groupConfig[groupId].level = level;
        await saveGroupToFirebase(groupId, groupConfig[groupId]);
        return res.json({ success: true });
      }

      case 'toggle_group': {
        if (!groupId) return res.status(400).json({ error: 'groupId obrigatório' });
        if (!groupConfig[groupId]) return res.status(404).json({ error: 'Grupo não configurado' });
        const isDisabled = !!groupConfig[groupId].disabled;
        if (isDisabled) delete groupConfig[groupId].disabled;
        else groupConfig[groupId].disabled = true;
        await saveGroupToFirebase(groupId, groupConfig[groupId]);
        return res.json({ success: true, disabled: !isDisabled });
      }

      default:
        return res.status(400).json({ error: `Acção desconhecida: ${action}` });
    }
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── /api/cliente/:userId ─────────────────────────────────────────────────────
// Página pública de consulta do cliente (sem auth)
app.get('/api/cliente/:userId', async (req, res) => {
  const userId = req.params.userId + '@c.us';
  let memberInfo = null;

  // 1. Procura nos timeouts activos (memória)
  for (const [key, timeouts] of activeTimeouts.entries()) {
    const [gid, uid] = key.split('_');
    if (uid === userId) {
      const remainingMs = timeouts.main ? timeouts.main._idleTimeout : 0;
      const exp         = new Date(Date.now() + remainingMs);
      memberInfo = {
        userId:    userId.split('@')[0],
        groupId:   gid,
        groupName: groupConfig[gid]?.name || 'Grupo',
        level:     groupConfig[gid]?.level || 'normal',
        expiration: exp.toISOString(),
        daysLeft:  Math.max(0, Math.ceil(remainingMs / 86400000)),
        hoursLeft: Math.max(0, Math.ceil(remainingMs / 3600000))
      };
      break;
    }
  }

  // 2. Fallback: procura no Firestore
  if (!memberInfo && firebaseInitialized && db) {
    try {
      const snapshot = await db.collection('members')
        .where('memberId', '==', userId)
        .where('status',   '==', 'active')
        .limit(1).get();
      if (!snapshot.empty) {
        const data = snapshot.docs[0].data();
        const exp  = new Date(data.expirationDate);
        memberInfo = {
          userId:    userId.split('@')[0],
          groupId:   data.groupId,
          groupName: data.groupName || groupConfig[data.groupId]?.name || 'Grupo',
          level:     groupConfig[data.groupId]?.level || 'normal',
          expiration: exp.toISOString(),
          daysLeft:  Math.max(0, Math.ceil((exp - new Date()) / 86400000)),
          hoursLeft: Math.max(0, Math.ceil((exp - new Date()) / 3600000))
        };
      }
    } catch (e) { console.error('Erro /api/cliente Firebase:', e.message); }
  }

  // 3. Enriquecer com dados do país
  if (memberInfo) {
    const parsed       = parsePhoneNumber(memberInfo.userId);
    memberInfo.country  = parsed.country.name;
    memberInfo.flag     = parsed.country.flag;
    memberInfo.ddi      = parsed.country.ddi;
    memberInfo.formatted = parsed.formatted;
  }

  res.json(memberInfo || { notFound: true, message: 'Membro não encontrado ou acesso expirado.' });
});

// ── /api/banned ───────────────────────────────────────────────────────────────
app.get('/api/banned', authMiddleware, (req, res) => {
  const list = [];
  for (const [uid, data] of bannedUsers.entries()) {
    const parsed = parsePhoneNumber(uid);
    list.push({ userId: uid.split('@')[0], formatted: parsed.formatted, country: parsed.country.name, flag: parsed.country.flag, groupId: data.groupId, groupName: groupConfig[data.groupId]?.name || data.groupId, bannedAt: data.bannedAt });
  }
  res.json({ banned: list, total: list.length });
});

// ── /api/pending-removals ────────────────────────────────────────────────────
app.get('/api/pending-removals', authMiddleware, (req, res) => {
  const list = [];
  for (const [key, timeouts] of activeTimeouts.entries()) {
    const [gid, uid]   = key.split('_');
    const remainingMs  = timeouts.main ? timeouts.main._idleTimeout : 0;
    const parsed       = parsePhoneNumber(uid);
    list.push({
      userId:    uid.split('@')[0],
      formatted: parsed.formatted,
      country:   parsed.country.name,
      flag:      parsed.country.flag,
      groupId:   gid,
      groupName: groupConfig[gid]?.name || gid,
      daysLeft:  Math.max(0, Math.ceil(remainingMs / 86400000)),
      hoursLeft: Math.max(0, Math.ceil(remainingMs / 3600000)),
      expiration: new Date(Date.now() + remainingMs).toISOString()
    });
  }
  list.sort((a, b) => a.hoursLeft - b.hoursLeft);
  res.json({ removals: list, total: list.length });
});

// ── /api/affiliates ───────────────────────────────────────────────────────────
app.get('/api/affiliates', authMiddleware, async (req, res) => {
  if (!firebaseInitialized || !db) return res.status(503).json({ error: 'Firebase não configurado' });
  try {
    const snapshot = await db.collection('affiliates').orderBy('generatedAt', 'desc').limit(100).get();
    const list     = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data(), generatedAt: doc.data().generatedAt?.toDate()?.toISOString() }));
    res.json({ affiliates: list, total: list.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── /api/logs ─────────────────────────────────────────────────────────────────
app.get('/api/logs', authMiddleware, async (req, res) => {
  const limitVal = parseInt(req.query.limit) || 50;
  const typeVal  = req.query.type || null;
  if (!firebaseInitialized || !db) return res.status(503).json({ error: 'Firebase não configurado' });
  try {
    let query = db.collection('activity_logs').orderBy('timestamp', 'desc').limit(limitVal);
    if (typeVal) query = query.where('type', '==', typeVal);
    const snapshot = await query.get();
    const logs     = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data(), timestamp: doc.data().timestamp?.toDate()?.toISOString() }));
    res.json({ logs, total: logs.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── /api/country-codes ────────────────────────────────────────────────────────
app.get('/api/country-codes', (req, res) => {
  res.json({ countryCodes: COUNTRY_CODES, total: Object.keys(COUNTRY_CODES).length });
});

// ========== SERVIDOR ==========
app.listen(PORT, '0.0.0.0', () => { console.log(`\n🚀 Servidor rodando na porta ${PORT}`); });
initializeBot().catch(err => { console.error('Falha fatal:', err); process.exit(1); });

process.on('SIGTERM', async () => {
  if (keepaliveInterval)   clearInterval(keepaliveInterval);
  if (healthCheckInterval) clearInterval(healthCheckInterval);
  for (const t of activeTimeouts.values()) {
    if (t.main)       clearTimeout(t.main);
    if (t.warning24h) clearTimeout(t.warning24h);
    if (t.warning1h)  clearTimeout(t.warning1h);
  }
  await backupSessionToFirestore();
  if (client) await client.destroy();
  process.exit(0);
});
