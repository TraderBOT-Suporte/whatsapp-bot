# README.md corrigido — alinhado com o código real

```markdown
# Painel de Sinais (sem WhatsApp)

App web (PWA) que analisa ativos nos modos SNIPER / CAÇADOR / PESCADOR / BALEEIRO,
mantém um motor de sinais contínuo no servidor, grava um histórico permanente
no Firestore e envia notificações **Web Push** segmentadas por utilizador —
mesmo com o telemóvel bloqueado ou a app fechada.

Tudo o que era WhatsApp (Baileys, QR code, pairing code, grupos, membros,
comandos admin, envio de mensagens) foi removido por completo.
O **Firebase Authentication também foi removido**: o login agora é por token,
validado contra a tua API de análise.

---

## Índice
1. [O que já está pronto](#o-que-já-está-pronto)
2. [Arquitetura — como tudo se liga](#arquitetura)
3. [Autenticação por token](#autenticação-por-token)
4. [Setup do Firebase](#setup-do-firebase)
5. [Gerar chaves VAPID (Web Push)](#gerar-chaves-vapid-web-push)
6. [Ícones da PWA](#ícones-da-pwa)
7. [Regras do Firestore](#regras-do-firestore)
8. [Índices compostos do Firestore](#índices-compostos-do-firestore)
9. [Variáveis de ambiente](#variáveis-de-ambiente)
10. [Correr localmente](#correr-localmente)
11. [Deploy no Render](#deploy-no-render)
12. [HTTPS obrigatório](#https-é-obrigatório)
13. [Testar o push](#testar-o-push)
14. [Referência completa da API](#referência-completa-da-api)
15. [Fluxo de utilização, passo a passo](#fluxo-de-utilização-passo-a-passo)
16. [Estrutura de dados no Firestore](#estrutura-de-dados-no-firestore)
17. [Troubleshooting](#troubleshooting)
18. [Limitações conhecidas](#limitações-conhecidas)

---

## O que já está pronto

| Ficheiro | Função |
|---|---|
| `server.js` | Motor de análise, cron de 1 em 1 minuto, endpoints REST, Web Push segmentado, 4 coleções Firestore |
| `public/index.html` | Painel (login por token, Scanner, Motor de Sinais, Painel de Sinais, Menu, Manual) |
| `public/service-worker.js` | Recebe os pushes em background, mesmo com a app fechada |
| `public/manifest.json` | Torna a app instalável (PWA) no telemóvel |
| `package.json` | Dependências (Express, Firebase Admin, web-push, node-cron, pino, cors) |
| `.env.example` | Modelo das variáveis de ambiente necessárias |

Nada disto depende de WhatsApp, Baileys, QR code, grupos, nem de Firebase Authentication.

---

## Arquitetura

```
┌───────────────────────┐        ┌──────────────────────────┐        ┌──────────────────────┐
│  A tua API de          │◄──────│  server.js (este projeto) │──────►│  Firestore            │
│  indicadores/análise   │  HTTP  │  · cron 1x/min            │  R/W  │  · signals            │
│  (ANALYSIS_API_URL)    │        │  · scan manual            │        │  · push_subscriptions │
│  · /api/analyze        │        │  · auth por token         │        │  · user_watchlists    │
│  · /validate-token     │        │  · watchlist por user     │        │  · analysis_history   │
│  já existente          │        │  · Web Push segmentado    │        └──────────────────────┘
└───────────────────────┘        └───────────┬───────────────┘
                                               │ Web Push (VAPID)
                                               ▼
                                   ┌──────────────────────────┐
                                   │  Telemóvel/PC do utilizador│
                                   │  · Service Worker           │
                                   │  · Notificação nativa       │
                                   │  · PWA instalada (opcional) │
                                   └──────────────────────────┘
```

- O **motor de sinais corre sempre no servidor**, independente de qualquer app estar aberta. É o `cron.schedule('* * * * *', ...)` dentro de `server.js` (timezone `America/Sao_Paulo`).
- O **Firestore é a fonte de verdade** de: histórico de sinais (`signals`), subscrições push (`push_subscriptions`), watchlists por utilizador (`user_watchlists`) e histórico de análises (`analysis_history`).
- O **telemóvel só recebe notificações e mostra o histórico** — não faz processamento pesado. Funciona mesmo desligando e ligando o telemóvel: ao abrir a app, sincroniza com o servidor.

---

## Autenticação por token

<!-- ⭐ CORRIGIDO: secção nova que substitui o antigo "Setup do Firebase → Authentication" -->

O login **não usa Firebase Authentication**. O fluxo é:

1. O utilizador insere um **token** no ecrã de login da app.
2. O frontend (`index.html`) chama `POST {ANALYSIS_API_URL}/validate-token` com `{ token }`.
3. Se a tua API responder `{ valid: true, email, name }`, o token é guardado em `localStorage.authToken` e passa a ser enviado em todas as chamadas como `Authorization: Bearer <token>`.
4. Em cada pedido ao `server.js`, o middleware `authMiddleware` faz o seguinte:
   - Se o token for igual ao `ADMIN_SECRET` → **login automático como admin** (`email: 'admin@local'`, `isAdmin: true`).
   - Caso contrário, verifica cache (5 min) e, se não estiver em cache, chama `POST {ANALYSIS_API_URL}/validate-token`.
   - Deriva um `tokenHash` (`sha256(token).hex.slice(0,16)`) — é este hash que identifica o utilizador em todo o Firestore (nunca o token cru).

**Consequências práticas:**
- **Não** precisas de ativar Firebase Authentication.
- **Não** precisas de criar utilizadores no Firebase Console.
- Os utilizadores são geridos na tua API de análise (quem gera e valida os tokens).
- Se quiseres um login admin direto, usa o próprio `ADMIN_SECRET` como token.

---

## Setup do Firebase

<!-- ⭐ CORRIGIDO: removida toda a parte de Authentication e de firebaseConfig no frontend -->

1. Vai a [console.firebase.google.com](https://console.firebase.google.com) → **Adicionar projeto** (ou reutiliza o teu).
2. Ativa **Firestore Database** → **Criar base de dados** → modo produção (as regras ficam na secção 7).
3. **Não** é preciso ativar Authentication.
4. Vai a **Definições do projeto → Contas de serviço → Gerar nova chave privada**. Isto descarrega um ficheiro `.json` — é o valor que vai para `FIREBASE_SERVICE_ACCOUNT` (colar o JSON inteiro numa única linha, com `\n` escapados).
5. **Não** é preciso adicionar "app Web" nem colar `firebaseConfig` em nenhum sítio. O `index.html` atual não usa `firebaseConfig`.

---

## Gerar chaves VAPID (Web Push)

As chaves VAPID identificam o teu servidor perante os navegadores (Chrome, Firefox, Safari) para poder enviar notificações push.

```bash
npm install
npx web-push generate-vapid-keys
```

Vai aparecer algo como:
```
=======================================

Public Key:
BN4Gv... (uma string longa)

Private Key:
xY9k... (outra string longa)

=======================================
```

Copia:
- `Public Key` → `VAPID_PUBLIC_KEY`
- `Private Key` → `VAPID_PRIVATE_KEY`
- `VAPID_SUBJECT` → um `mailto:teu-email@exemplo.com` (formato tem de ser válido; pode ser fictício)

Guarda a `Private Key` como segredo — nunca a coloques no frontend/HTML.

---

## Ícones da PWA

Cria dois ficheiros PNG e coloca-os em `public/`:
- `icon-192.png` — 192x192 pixels
- `icon-512.png` — 512x512 pixels

Recomendado: fundo sólido, sem transparência total (iOS trata melhor). Sem eles a app continua a funcionar, mas a instalação como PWA fica com ícone genérico do navegador.

---

## Regras do Firestore

<!-- ⭐ CORRIGIDO: adicionadas regras para user_watchlists e analysis_history -->

Vai a **Firestore Database → Regras**:

```
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {

    // Histórico de sinais — leitura só pelo servidor (Admin SDK ignora isto).
    match /signals/{doc} {
      allow read, write: if false;
    }

    // Subscrições de push — nunca lidas/escritas diretamente pelo cliente.
    match /push_subscriptions/{doc} {
      allow read, write: if false;
    }

    // Watchlists por utilizador — geridas exclusivamente pelo servidor.
    match /user_watchlists/{doc} {
      allow read, write: if false;
    }

    // Histórico de análises por utilizador.
    match /analysis_history/{doc} {
      allow read, write: if false;
    }
  }
}
```

**Nota:** o **Firebase Admin SDK usado no `server.js` ignora estas regras** (acesso total via conta de serviço). Estas regras existem apenas para bloquear acesso direto vindo do navegador. Como o frontend **não** acede ao Firestore diretamente (tudo passa pelo `server.js`), o mais seguro é negar tudo.

---

## Índices compostos do Firestore

<!-- ⭐ CORRIGIDO: índices anteriores estavam errados; agora refletem as queries reais -->

O código faz queries compostas em **duas coleções**. O Firestore exige índices explícitos para cada combinação.

### `signals`
| # | Campos | Tipo do 1º campo |
|---|---|---|
| 1 | `watchers` + `criadoEm` (desc) | Array contains |
| 2 | `watchers` + `mode` (asc) + `criadoEm` (desc) | Array contains |

### `analysis_history`
| # | Campos | Tipo do 1º campo |
|---|---|---|
| 3 | `tokenHash` + `analisadoEm` (desc) | Ascending |
| 4 | `tokenHash` + `mode` (asc) + `analisadoEm` (desc) | Ascending |

**Como criar (opção fácil):** ao usar o filtro pela primeira vez, o Firestore devolve um erro na consola do browser com um link direto `https://console.firebase.google.com/project/.../firestore/indexes?create_composite=...`. Clica, confirma, espera 1-2 min.

**Como criar (manual):** Firestore → Índices → **Criar índice composto** → escolhe a coleção e adiciona os campos pela ordem da tabela acima.

---

## Variáveis de ambiente

Cria um `.env` (local) ou configura no painel do Render. Ver `.env.example`.

| Variável | Obrigatória | Descrição |
|---|---|---|
| `PORT` | Não | Porta do servidor. Render define automaticamente. |
| `FIREBASE_SERVICE_ACCOUNT` | **Sim** | JSON da conta de serviço, numa única linha. |
| `VAPID_PUBLIC_KEY` | Sim (para push) | Chave pública gerada na secção anterior. |
| `VAPID_PRIVATE_KEY` | Sim (para push) | Chave privada. Nunca expor no frontend. |
| `VAPID_SUBJECT` | Sim (para push) | `mailto:...` de contacto. |
| `ANALYSIS_API_URL` | **Sim** | URL da tua API (`/api/analyze` e `/validate-token`). Sem barra no fim. |
| `ADMIN_SECRET` | **Sim** | 1) Chave enviada como `x-admin-key` para `/api/analyze`. 2) Também serve como **token de login admin** quando usado no campo de token. |
| `RENDER_EXTERNAL_URL` | Não | Preenchido pelo Render. Usado no self-ping a cada 4 min. |
| `LOG_LEVEL` | Não | `trace|debug|info|warn|error|fatal` (padrão: `info`). |

<!-- ⭐ CORRIGIDO: CORS_ORIGINS consta no .env.example mas NÃO é lido pelo server.js (usa cors() sem opções). Não documentar como funcional. -->

Se `VAPID_*` não estiverem definidas, o servidor arranca com `Push: Não configurado` e `/api/vapid-public-key` devolve 503. Se `ANALYSIS_API_URL`/`ADMIN_SECRET` estiverem errados, o scan e o motor falham com `Erro HTTP` ou `Erro de conexão` nos logs.

---

## Correr localmente

```bash
cd painel-sinais
npm install
cp .env.example .env
# edita o .env
node server.js
```

Abre `http://localhost:3000`. Em `localhost`, o Web Push funciona mesmo sem HTTPS (exceção dos navegadores para desenvolvimento).

---

## Deploy no Render

1. **Web Service** novo, aponta para o repositório.
2. Build command: `npm install`
3. Start command: `npm start`
4. Adiciona todas as env vars da secção anterior.
5. O Render atribui HTTPS e `RENDER_EXTERNAL_URL` automaticamente.
6. Confirma nos logs:
   ```
   🚀 Servidor rodando na porta 3000
   Firebase: Conectado
   Push: Configurado
   ```

---

## HTTPS é obrigatório

Web Push e Service Workers só funcionam em `https://` (ou `localhost`). Em produção sem HTTPS, o navegador recusa registar o Service Worker. O Render já serve em HTTPS por padrão.

---

## Testar o push

<!-- ⭐ CORRIGIDO: /api/push/test NÃO envia para todos — envia só para o token do próprio requisitante -->

1. Faz login com o teu token.
2. Menu → **Notificações → Ativar Push** → aceita a permissão.
3. Para confirmar:
   ```
   POST /api/push/test
   Authorization: Bearer <o-teu-token-de-acesso>
   ```
   Isto envia uma notificação de teste **apenas para os dispositivos subscritos do próprio utilizador autenticado** — não é um broadcast global.

Se não chegar nada, ver [Troubleshooting](#troubleshooting).

---

## Referência completa da API

Todos os endpoints exceto `/health` e `/api/vapid-public-key` exigem:
```
Authorization: Bearer <TOKEN_DE_ACESSO>
```

### Sistema
| Método | Rota | Descrição |
|---|---|---|
| GET | `/health` | Health check (usado no self-ping). Público. |
| GET | `/api/stats` | Estatísticas do utilizador atual: `engineActive`, `watchlistCount`, `openTrades`, `uptime`, `firebase`, `pushConfigured`, `totalSignals`, `signalsToday`. |
| GET | `/api/user-me` | Devolve `{ tokenHash, email, name }` do utilizador autenticado. |

### Push
| Método | Rota | Body | Descrição |
|---|---|---|---|
| GET | `/api/vapid-public-key` | — | Chave pública VAPID. Público. |
| POST | `/api/push/subscribe` | `{ subscription }` | Guarda/atualiza a subscrição do dispositivo atual. |
| POST | `/api/push/unsubscribe` | `{ endpoint }` | Remove uma subscrição. |
| POST | `/api/push/test` | — | Envia notificação de teste **apenas para o próprio utilizador**. |

### Motor de Sinais (por utilizador)
| Método | Rota | Body | Descrição |
|---|---|---|---|
| GET | `/api/engine-config` | — | Devolve `{ active, watchlist }` do utilizador. |
| POST | `/api/engine-start` | — | Ativa o motor do utilizador. |
| POST | `/api/engine-stop` | — | Desativa o motor do utilizador. |
| GET | `/api/engine-watchlist` | — | Watchlist do utilizador por modo. |
| POST | `/api/engine-watchlist` | `{ watchlist: { SNIPER: [...], 'CAÇADOR': [...], PESCADOR: [...], BALEEIRO: [...] } }` | Substitui a watchlist. Máx. **10 ativos por modo por utilizador** — o excesso é cortado silenciosamente. Confirma sempre pela resposta. |
| DELETE | `/api/engine-watchlist` | — | Limpa a watchlist do utilizador (todos os modos). |

### Análise / Scan
| Método | Rota | Body | Descrição |
|---|---|---|---|
| POST | `/api/scan-group` | `{ group, mode }` | Analisa todos os ativos do grupo (`Cestas de Moedas`, `Forex`, `Metais`, `Índices Sintéticos`, `Índices OTC`, `Criptomoedas`). Devolve por ativo: `symbol`, `name`, `signal`, `zona`, `score`, `proximidade`, `reasons`. |

### Sinais (histórico)
| Método | Rota | Query | Descrição |
|---|---|---|---|
| GET | `/api/signals` | `?mode=SNIPER` (opcional), `?limit=50` (opcional, máx. 200) | Sinais em que o utilizador é watcher, ordem decrescente de `criadoEm`. |

### Histórico de análises
<!-- ⭐ CORRIGIDO: endpoints existiam no servidor mas não estavam documentados -->
| Método | Rota | Body | Descrição |
|---|---|---|---|
| POST | `/api/analysis-history` | `{ symbol, mode, result }` | Grava uma análise. **Ignora silenciosamente** se `result.consolidated.signal === 'HOLD'`. |
| GET | `/api/analysis-history` | `?mode=...` (opcional), `?limit=50` (opcional, máx. 200) | Histórico pessoal de análises (ordem desc. de `analisadoEm`). |

> **Nota:** o `index.html` atual **não chama** estes endpoints — são uma capacidade do servidor disponível para integrações futuras (ex.: gravar automaticamente o resultado de cada `runScan`).

---

## Fluxo de utilização, passo a passo

1. **Login**: insere o token gerado pela tua API de análise (ou o próprio `ADMIN_SECRET` para entrar como admin).
2. **Análise**: escolhe um grupo e um modo, clica em "Analisar Grupo". Resultados ordenados por zona (A → B → C) e score.
3. **Selecionar e monitorizar**: marca checkboxes (ou "Selecionar Zona B") e clica em "Monitorar". Isto adiciona à watchlist do modo escolhido, **do teu utilizador** (máx. 10 por modo por utilizador).
4. **Motor de Sinais**: na aba correspondente, ativa o motor. O servidor reanalisa continuamente com cadências diferentes: SNIPER 1 min, CAÇADOR 3 min, PESCADOR 10 min, BALEEIRO 30 min.
5. **Quando surge um sinal** (prontidão em Zona B, sinal confirmado em Zona A, atualizações de trade — seguindo, aceleração, zero risco, quase lá, WIN, STOP, tempo esgotado, arrefecimento):
   - É gravado em `signals` com o array `watchers` (quem deve receber);
   - É enviado como push **apenas para os watchers desse par símbolo+modo** — não é broadcast global.
6. **Depois de desligar e voltar a ligar o telemóvel**: ao abrir a app, o histórico, o estado do motor e a watchlist estão sincronizados — tudo vive no servidor/Firestore.

---

## Estrutura de dados no Firestore

<!-- ⭐ CORRIGIDO: adicionadas user_watchlists e analysis_history; corrigido push_subscriptions (tokenHash, não uid); documentado o campo watchers -->

### `signals`
```json
{
  "symbol": "frxEURUSD",
  "mode": "SNIPER",
  "tipo": "SINAL_CONFIRMADO",
  "titulo": "🚨 SINAL CONFIRMADO: EUR/USD",
  "corpo": "🟢 CALL · Confiança 82.4% · Score 91/100 · Entrada 1.0842 · TP 1.0860 · SL 1.0830",
  "score": 91,
  "confidence": 0.824,
  "entry": 1.0842,
  "takeProfit": 1.086,
  "stopLoss": 1.083,
  "watchers": ["<tokenHash1>", "<tokenHash2>"],
  "origem": "motor",
  "criadoEm": "Timestamp"
}
```
`score`, `confidence`, `entry`, `takeProfit`, `stopLoss` só aparecem conforme o tipo. `watchers` é o array de `tokenHash` que devem receber o push.

`tipo` possíveis: `PRONTIDAO`, `SINAL_CONFIRMADO`, `SEGUINDO`, `ACELERACAO`, `ZERO_RISCO`, `QUASE_LA`, `5MIN`, `WIN`, `STOP`, `TEMPO_ESGOTADO`, `ARREFECIMENTO`.

### `push_subscriptions`
```json
{
  "subscription": { "endpoint": "...", "keys": { "p256dh": "...", "auth": "..." } },
  "tokenHash": "<sha256(token).slice(0,16)>",
  "email": "utilizador@exemplo.com",
  "updatedAt": "Timestamp"
}
```
O `id` do documento é `base64(endpoint)` sanitizado — um dispositivo = um documento.

### `user_watchlists`
```json
{
  "SNIPER": ["frxEURUSD", "frxGBPUSD"],
  "CAÇADOR": [],
  "PESCADOR": [],
  "BALEEIRO": [],
  "engineActive": true,
  "email": "utilizador@exemplo.com",
  "atualizadoEm": "Timestamp"
}
```
`id` do documento = `tokenHash`. Máx. 10 ativos por modo.

### `analysis_history`
```json
{
  "tokenHash": "<sha256(token).slice(0,16)>",
  "userEmail": "utilizador@exemplo.com",
  "symbol": "frxEURUSD",
  "mode": "SNIPER",
  "signal": "CALL",
  "score": 91,
  "confidence": 0.824,
  "zona": "A",
  "price": 1.0842,
  "scoreReasons": ["...", "..."],
  "analisadoEm": "Timestamp"
}
```
Só é gravado quando `signal !== 'HOLD'`.

---

## Troubleshooting

**Não recebo nenhuma notificação:**
1. Confirma `Push: Configurado` nos logs do servidor.
2. Confirma que aceitaste a permissão de notificações no navegador.
3. Confirma que estás em HTTPS (ou localhost).
4. Testa com `POST /api/push/test` — envia **só para ti**. Se não chegar, é problema de subscrição/VAPID, não do motor.
5. Verifica `Erro ao enviar push:` nos logs — normalmente subscrição expirada (o servidor remove-a automaticamente em 404/410).

**O filtro por modo no Painel de Sinais dá erro:**
Falta um dos índices da secção 8. Ver o link que o Firestore devolve na consola.

**O scan/motor não traz resultados, erro "Erro de conexão":**
`ANALYSIS_API_URL` incorreto ou a tua API offline. Confirma que responde em `POST {ANALYSIS_API_URL}/api/analyze` (header `x-admin-key`) e em `POST {ANALYSIS_API_URL}/validate-token`.

**Login falha sempre:**
<!-- ⭐ CORRIGIDO: já não é Firebase Auth -->
A tua API de análise em `ANALYSIS_API_URL/validate-token` não está a devolver `{ valid: true }` para esse token. Verifica na tua API se o token é válido, se não expirou e se o endpoint está acessível. **Não** tem nada a ver com Firebase Authentication.

**A app não aparece como instalável no telemóvel:**
Faltam `icon-192.png`/`icon-512.png` em `public/`, ou não estás em HTTPS.

**As notificações chegam atrasadas (minutos depois) no Android:**
Fabricantes (Xiaomi/MIUI, Huawei, Samsung, Oppo) adormecem o Chrome em background:
1. **Definições → Apps → Chrome → Bateria → Sem restrições**.
2. Xiaomi/MIUI tem opções extra: **Definições → Apps → Gerir apps → Chrome → Poupança de bateria → Sem restrições** e **Segurança → Otimização de bateria → Chrome → Sem restrições**.
3. Se instalaste a PWA, aplica o mesmo ao ícone "Painel de Sinais".

**No iPhone não recebo nada:**
<!-- ⭐ CORRIGIDO: adicionada secção iOS -->
O iOS só entrega Web Push se a PWA estiver **instalada no ecrã inicial**:
1. Abre o site no **Safari** (não Chrome/Edge).
2. **Partilhar → Adicionar ao Ecrã Principal**.
3. Abre pelo **ícone novo** (não pelo Safari).
4. Aí sim: Menu → Notificações → Ativar Push.
5. Requer **iOS 16.4 ou superior**.

---

## Limitações conhecidas

<!-- ⭐ CORRIGIDO: push é segmentado por watchers, não broadcast global -->

- Máximo de **10 ativos por modo por utilizador** (protege o motor e a tua API de análise).
- O push é **segmentado**: um utilizador só recebe sinais dos pares símbolo+modo que tem na sua watchlist. Não há broadcast global.
- O Web Push depende do navegador/SO manterem o Service Worker vivo. Android costuma ser fiável; iOS (16.4+) só funciona com a PWA **instalada no ecrã inicial**.
- No iOS, a subscrição push expira após ~3 meses sem a app ser aberta.
- As notificações são enviadas **apenas aos watchers no momento em que o sinal é gerado**. Se adicionares um ativo à watchlist depois de um sinal já ter sido emitido, não recebes esse retroativamente.
- `tradesAbertos` (trades em curso) vive **em memória** — reiniciar o servidor perde o seguimento de trades abertos (os sinais já emitidos ficam no Firestore normalmente).

## Não esquecer
- O `whatsapp-bot.js` antigo e a UI antiga com QR/Baileys podem ser arquivados.
- A tua API de indicadores (`ANALYSIS_API_URL`) **não mudou**: o `server.js` chama `/api/analyze` (com `x-admin-key`) e `/validate-token` (para autenticação), exatamente como o bot antigo fazia.
- O menu lateral inclui links para **Canal de WhatsApp, Email de Suporte e YouTube** — são apenas links sociais, não têm relação com o antigo sistema de bot.
- O ficheiro `service-worker.js` faz cache de `['/', '/index.html', '/manifest.json']`. Se alterares assets estáticos, muda o `CACHE_NAME` (`'painel-sinais-v1'`) para forçar atualização.
```

---

## Resumo das correções aplicadas

| # | Secção | O que estava errado | O que ficou |
|---|---|---|---|
| 1 | Setup Firebase | Mandava ativar Auth + criar users + colar `firebaseConfig` | Só Firestore + service account |
| 2 | Autenticação | Inexistia — dava a entender que era Firebase Auth | Nova secção "Autenticação por token" |
| 3 | Regras Firestore | Só `signals` e `push_subscriptions` | + `user_watchlists` e `analysis_history`, com `allow read, write: if false` |
| 4 | Índices | `mode + criadoEm` | `watchers + criadoEm`, `watchers + mode + criadoEm`, `tokenHash + analisadoEm`, `tokenHash + mode + analisadoEm` |
| 5 | Env vars | Sem `LOG_LEVEL`, `CORS_ORIGINS` documentado como funcional | `LOG_LEVEL` adicionado; `CORS_ORIGINS` removido (não usado) |
| 6 | `POST /api/push/test` | "para todos os subscritos" | "apenas para o próprio utilizador" |
| 7 | API | Faltavam `/api/user-me`, `/api/analysis-history` (POST e GET) | Adicionados |
| 8 | `push_subscriptions` | Campo `uid` | Campo `tokenHash` |
| 9 | `signals` | Sem campo `watchers` nem `origem` | Adicionados |
| 10 | Watchlist | Documentada como global | Documentada como **por utilizador** |
| 11 | Troubleshooting | "Login falha → Firebase Auth" | "Login falha → `/validate-token` da tua API" |
| 12 | iOS | Só mencionado de passagem | Secção dedicada no Troubleshooting |
| 13 | Limitações | "todos recebem todos os sinais" | Push segmentado por watchers; trades em memória |

