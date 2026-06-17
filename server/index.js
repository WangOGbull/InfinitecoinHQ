/**
 * Infinite Coin HQ — WebSocket Command Center Backend (FINAL)
 * 
 * Features:
 * - DexScreener API price polling (real price)
 * - REAL transactions from Helius API history + Helius WebSocket
 * - AI FAQ bot engine
 * - Live visitor counter
 * - Price alerts
 * - Scrolling ticker with real transactions
 */

require('dotenv').config();
const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const fetch = require('node-fetch');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ═══════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════
const PORT = process.env.PORT || 3001;
const TOKEN_CA = process.env.TOKEN_CA || 'C8KsvkMBuqmvX416MWTJGKW9S9MpKiUjmpnj1fhzpump';
const DEXSCREENER_API = 'https://api.dexscreener.com/latest/dex/tokens';
const HELIUS_API_KEY = process.env.HELIUS_API_KEY || 'de2fb44b-73e1-4ee5-aa9d-b1134825a8b0';
const PRICE_INTERVAL = parseInt(process.env.PRICE_INTERVAL) || 10000;
const ALERT_THRESHOLD = parseFloat(process.env.ALERT_THRESHOLD) || 5;
const WS_PING_INTERVAL = parseInt(process.env.WS_PING_INTERVAL) || 30000;
const RATE_LIMIT = parseInt(process.env.RATE_LIMIT) || 20;

// State
let lastPrice = 0;
let lastAlertPrice = 0;
let lastMcap = 0;
let lastVol = '890K';
let lastLiq = '1.1M';
let lastPriceChange = 0;
let lastHolders = 18420;
const visitors = new Set();
const rateLimits = new Map();
const clients = new Set();
let heliusWs = null;
let lastRealTxTime = Date.now();
let recentTransactions = [];

// ═══════════════════════════════════════════
// EXPRESS MIDDLEWARE
// ═══════════════════════════════════════════
app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    connections: clients.size,
    visitors: visitors.size,
    heliusConnected: heliusWs && heliusWs.readyState === WebSocket.OPEN,
    recentTxCount: recentTransactions.length,
    timestamp: new Date().toISOString()
  });
});

app.get('/', (req, res) => {
  res.json({
    name: 'Infinite Coin HQ WebSocket Server',
    version: '1.4.0',
    endpoints: ['/health', '/ws'],
    status: 'running'
  });
});

// ═══════════════════════════════════════════
// RATE LIMITING
// ═══════════════════════════════════════════
function checkRateLimit(ip) {
  const now = Date.now();
  const limit = rateLimits.get(ip);
  if (!limit || now > limit.resetTime) {
    rateLimits.set(ip, { count: 1, resetTime: now + 60000 });
    return true;
  }
  if (limit.count >= RATE_LIMIT) return false;
  limit.count++;
  return true;
}

// ═══════════════════════════════════════════
// BROADCAST HELPERS
// ═══════════════════════════════════════════
function broadcast(data) {
  const msg = JSON.stringify(data);
  clients.forEach(ws => {
    if (ws.readyState === 1) ws.send(msg);
  });
}

function send(ws, data) {
  if (ws.readyState === 1) ws.send(JSON.stringify(data));
}

// ═══════════════════════════════════════════
// DEXSCREENER PRICE POLLING (REAL PRICE)
// ═══════════════════════════════════════════
async function fetchPrice() {
  try {
    const url = `${DEXSCREENER_API}/${TOKEN_CA}`;
    const res = await fetch(url, { timeout: 8000 });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    
    if (!data.pairs || data.pairs.length === 0) {
      throw new Error('No pairs found');
    }
    
    const pair = data.pairs[0];
    const price = parseFloat(pair.priceUsd);
    const mcap = parseFloat(pair.fdv || pair.marketCap || 0);
    const vol24h = pair.volume?.h24 || 0;
    const liquidity = pair.liquidity?.usd || 0;
    const priceChange = pair.priceChange?.h24 || 0;
    
    const volFormatted = vol24h > 1000000 
      ? `$${(vol24h / 1000000).toFixed(1)}M`
      : vol24h > 1000 
        ? `$${(vol24h / 1000).toFixed(0)}K`
        : `$${vol24h.toFixed(0)}`;
    
    const liqFormatted = liquidity > 1000000 
      ? `$${(liquidity / 1000000).toFixed(1)}M`
      : `$${(liquidity / 1000).toFixed(0)}K`;
    
    const mcapFormatted = mcap > 1000000 
      ? `$${(mcap / 1000000).toFixed(1)}M`
      : `$${(mcap / 1000).toFixed(0)}K`;
    
    let change = priceChange;
    if (lastPrice > 0 && price !== lastPrice) {
      change = ((price - lastPrice) / lastPrice) * 100;
    }
    
    if (lastAlertPrice > 0) {
      const pctChange = Math.abs((price - lastAlertPrice) / lastAlertPrice * 100);
      if (pctChange >= ALERT_THRESHOLD) {
        broadcast({
          type: 'priceAlert',
          direction: price >= lastAlertPrice ? 'up' : 'down',
          percent: pctChange.toFixed(1),
          price: price.toFixed(8)
        });
        lastAlertPrice = price;
      }
    } else {
      lastAlertPrice = price;
    }
    
    lastPrice = price;
    lastMcap = mcap;
    lastVol = volFormatted;
    lastLiq = liqFormatted;
    lastPriceChange = change;
    
    broadcast({
      type: 'price',
      price: price.toFixed(8),
      change: change.toFixed(2),
      mcap: mcapFormatted,
      vol: volFormatted,
      liq: liqFormatted,
      holders: lastHolders
    });
    
    console.log(`[Price] $${price.toFixed(8)} | Change: ${change.toFixed(2)}% | MCap: ${mcapFormatted}`);
    
  } catch (err) {
    console.error('[Price] DexScreener error:', err.message);
    if (lastPrice > 0) {
      broadcast({
        type: 'price',
        price: lastPrice.toFixed(8),
        change: lastPriceChange.toFixed(2),
        mcap: lastMcap > 0 ? `$${(lastMcap / 1000000).toFixed(1)}M` : '$18.4M',
        vol: lastVol,
        liq: lastLiq,
        holders: lastHolders
      });
    }
  }
}

setInterval(fetchPrice, PRICE_INTERVAL);
fetchPrice();

// ═══════════════════════════════════════════
// FETCH TRANSACTION HISTORY FROM HELIUS API
// ═══════════════════════════════════════════
async function fetchTransactionHistory() {
  if (!HELIUS_API_KEY) {
    console.log('[Helius] No API key. Cannot fetch transaction history.');
    return;
  }

  try {
    // Helius API endpoint for transaction history
    const url = `https://api.helius.xyz/v0/addresses/${TOKEN_CA}/transactions?api-key=${HELIUS_API_KEY}&limit=20`;
    
    const res = await fetch(url, { timeout: 10000 });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    
    if (!data || data.length === 0) {
      console.log('[Helius] No transaction history found');
      return;
    }

    // Process transactions
    const txs = [];
    let txCount = 0;
    
    for (const tx of data) {
      // Only process confirmed transactions
      if (!tx.confirmed) continue;
      
      // Try to find token transfers
      const transfers = tx.tokenTransfers || [];
      let found = false;
      
      for (const transfer of transfers) {
        // Check if this transfer involves our token
        if (transfer.mint === TOKEN_CA) {
          const amount = parseFloat(transfer.tokenAmount) || 0;
          if (amount <= 0) continue;
          
          // Determine if buy or sell (simplified)
          const type = tx.feePayer === transfer.toUserAccount ? 'buy' : 'sell';
          const value = (amount * (lastPrice || 0.00000418)).toFixed(2);
          const time = tx.timestamp ? Math.floor((Date.now() / 1000 - tx.timestamp) / 60) + 'm ago' : 'recent';
          
          txs.push({
            type: type,
            amount: amount.toLocaleString() + ' INF',
            value: '$' + value,
            time: time
          });
          
          found = true;
          txCount++;
          break;
        }
      }
      
      if (txCount >= 15) break;
    }

    if (txs.length > 0) {
      recentTransactions = txs;
      console.log(`[Helius] Loaded ${txs.length} historical transactions`);
      
      // Broadcast to all connected clients
      txs.forEach(tx => {
        broadcast({ type: 'tx', tx: tx });
      });
    } else {
      console.log('[Helius] No transactions found for this token');
      // Try fallback - DexScreener
      await fetchRecentTransactionsFallback();
    }
    
  } catch (err) {
    console.error('[Helius] Error fetching transaction history:', err.message);
    // Fallback to DexScreener
    await fetchRecentTransactionsFallback();
  }
}

// ═══════════════════════════════════════════
// FALLBACK: FETCH FROM DEXSCREENER
// ═══════════════════════════════════════════
async function fetchRecentTransactionsFallback() {
  try {
    const url = `${DEXSCREENER_API}/${TOKEN_CA}`;
    const res = await fetch(url, { timeout: 8000 });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    
    if (!data.pairs || data.pairs.length === 0) return;
    
    const pair = data.pairs[0];
    const trades = pair.trades || [];
    
    if (trades.length === 0) {
      console.log('[DexScreener] No trades found (fallback)');
      // No transactions available - send placeholder
      return;
    }
    
    const txs = [];
    trades.slice(0, 15).forEach(trade => {
      const type = trade.side === 'buy' ? 'buy' : 'sell';
      const amount = parseFloat(trade.amount).toLocaleString() + ' INF';
      const value = '$' + parseFloat(trade.priceUsd * trade.amount).toFixed(2);
      const time = trade.timeAgo || 'recent';
      
      txs.push({ type, amount, value, time });
    });
    
    recentTransactions = txs;
    console.log(`[DexScreener] Loaded ${txs.length} transactions (fallback)`);
    
    txs.forEach(tx => {
      broadcast({ type: 'tx', tx: tx });
    });
    
  } catch (err) {
    console.error('[DexScreener] Fallback failed:', err.message);
  }
}

// ═══════════════════════════════════════════
// INITIALIZE TRANSACTIONS
// ═══════════════════════════════════════════
async function initTransactions() {
  console.log('[Init] Fetching transaction history...');
  
  // Try Helius first
  await fetchTransactionHistory();
  
  // If no transactions found, try fallback
  if (recentTransactions.length === 0) {
    console.log('[Init] No transactions from Helius, trying DexScreener...');
    await fetchRecentTransactionsFallback();
  }
  
  if (recentTransactions.length > 0) {
    console.log(`[Init] Successfully loaded ${recentTransactions.length} transactions`);
  } else {
    console.log('[Init] No transactions available. Will retry in 30s...');
    setTimeout(initTransactions, 30000);
  }
}

// Start transaction loading
initTransactions();

// Refresh transactions every 60 seconds
setInterval(async () => {
  await fetchTransactionHistory();
}, 60000);

// ═══════════════════════════════════════════
// HELIUS - FETCH REAL HOLDER COUNT
// ═══════════════════════════════════════════
async function fetchHolders() {
  if (!HELIUS_API_KEY) return;
  
  try {
    const url = `https://api.helius.xyz/v0/token-metadata?api-key=${HELIUS_API_KEY}`;
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mint: TOKEN_CA })
    });
    
    if (res.ok) {
      const data = await res.json();
      if (data && data[0] && data[0].holder_count) {
        lastHolders = data[0].holder_count;
        console.log(`[Holders] Updated: ${lastHolders}`);
      }
    }
  } catch (err) {
    console.error('[Holders] Error:', err.message);
  }
}

setInterval(fetchHolders, 300000);
fetchHolders();

// ═══════════════════════════════════════════
// HELIUS WEB SOCKET FOR REAL-TIME TRANSACTIONS
// ═══════════════════════════════════════════
function connectHelius() {
  if (!HELIUS_API_KEY) {
    console.log('[Helius] No API key provided. Real-time transactions disabled.');
    return;
  }
  
  const heliusUrl = `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
  
  try {
    if (heliusWs && heliusWs.readyState === WebSocket.OPEN) {
      heliusWs.close();
    }
    
    heliusWs = new WebSocket(heliusUrl);
    
    heliusWs.on('open', () => {
      console.log('[Helius] ✅ Connected. Listening for real-time transactions...');
      
      const subscribeMsg = {
        jsonrpc: "2.0",
        id: 1,
        method: "logsSubscribe",
        params: [
          { mentions: [TOKEN_CA] },
          { commitment: "confirmed" }
        ]
      };
      heliusWs.send(JSON.stringify(subscribeMsg));
      console.log(`[Helius] Subscribed to token: ${TOKEN_CA}`);
    });
    
    heliusWs.on('message', (data) => {
      try {
        const parsed = JSON.parse(data.toString());
        
        if (parsed.id === 1 && parsed.result) {
          console.log('[Helius] Subscription confirmed');
          return;
        }
        
        if (parsed.params && parsed.params.result) {
          const log = parsed.params.result;
          if (log.value && log.value.logs) {
            const logs = log.value.logs.join(' ');
            let type = null;
            let amount = null;
            
            const logsLower = logs.toLowerCase();
            if (logsLower.includes('buy') || logsLower.includes('swap')) {
              type = 'buy';
            } else if (logsLower.includes('sell')) {
              type = 'sell';
            }
            
            let amountMatch = logs.match(/(\d+(?:,\d+)?)\s+(?:INF|INFINITE)/i);
            if (!amountMatch) amountMatch = logs.match(/(\d+(?:,\d+)?)\s+tokens?/i);
            if (!amountMatch) amountMatch = logs.match(/amount[:\s]*(\d+(?:,\d+)?)/i);
            
            if (amountMatch) {
              amount = amountMatch[1].replace(/,/g, '');
            }
            
            if (type && amount && parseInt(amount) > 100) {
              const value = (parseInt(amount) * (lastPrice || 0.00000418)).toFixed(2);
              lastRealTxTime = Date.now();
              
              const txData = {
                type: type,
                amount: parseInt(amount).toLocaleString() + ' INF',
                value: '$' + value,
                time: 'just now'
              };
              
              // Add to recent transactions
              recentTransactions.unshift(txData);
              if (recentTransactions.length > 20) recentTransactions.pop();
              
              broadcast({ type: 'tx', tx: txData });
              console.log(`[Helius] Real-time ${type.toUpperCase()}: ${parseInt(amount).toLocaleString()} INF - $${value}`);
            }
          }
        }
      } catch (err) {}
    });
    
    heliusWs.on('error', (err) => {
      console.error('[Helius] Error:', err.message);
    });
    
    heliusWs.on('close', () => {
      console.log('[Helius] Disconnected. Reconnecting in 5s...');
      setTimeout(connectHelius, 5000);
    });
    
  } catch (err) {
    console.error('[Helius] Connection failed:', err.message);
    setTimeout(connectHelius, 5000);
  }
}

connectHelius();

setInterval(() => { lastRealTxTime = Date.now(); }, 1000);

// ═══════════════════════════════════════════
// AI FAQ BOT ENGINE
// ═══════════════════════════════════════════
const FAQ_BOT = {
  'what is infinite coin': 'Infinite Coin is a community-driven meme coin built on trust, transparency, and long-term momentum.',
  'what is infinite': 'Infinite Coin is a community-driven meme coin.',
  'contract': `CA: ${TOKEN_CA}`,
  'ca': `CA: ${TOKEN_CA}`,
  'address': `CA: ${TOKEN_CA}`,
  'how to buy': 'Buy on Jupiter: jup.ag. Connect Phantom, swap SOL for $INFINITE.',
  'buy': 'Buy on Jupiter DEX.',
  'staking': '32% APY staking coming Q3 2026.',
  'stake': '32% APY staking coming Q3 2026.',
  'telegram': 'Join Telegram: t.me/InfiniteCoinHQ',
  'twitter': 'Follow X: x.com/infinitecoinhq',
  'x': 'Follow X: x.com/infinitecoinhq',
  'tiktok': 'Follow TikTok: tiktok.com/@infinitecoinhq',
  'youtube': 'Subscribe YouTube: youtube.com/@infinitecoinhq',
  'instagram': 'Follow Instagram: instagram.com/infinitecoinhq',
  'help': 'Ask about: contract, buying, staking, NFTs, roadmap.',
  'hello': 'Welcome to Infinite Coin HQ! ♾️',
  'hi': 'Hey there! Ready to go infinite?',
  'lfg': 'LFG!!! ♾️🚀',
  'moon': 'To the moon! 🌕',
  '_default': 'Ask about contract, buying, staking, or say "help". ♾️'
};

function getBotReply(input) {
  const l = input.toLowerCase().trim();
  if (FAQ_BOT[l]) return FAQ_BOT[l];
  for (const [k, v] of Object.entries(FAQ_BOT)) {
    if (k === '_default') continue;
    if (l.includes(k)) return v;
  }
  return FAQ_BOT._default;
}

// ═══════════════════════════════════════════
// WEBSOCKET CONNECTION HANDLER
// ═══════════════════════════════════════════
wss.on('connection', (ws, req) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  clients.add(ws);
  visitors.add(ip);

  console.log(`[WS] Client connected. Total: ${clients.size}, Visitors: ${visitors.size}`);
  broadcast({ type: 'visitorCount', count: visitors.size });

  if (lastPrice > 0) {
    send(ws, {
      type: 'price',
      price: lastPrice.toFixed(8),
      change: lastPriceChange.toFixed(2),
      mcap: lastMcap > 0 ? `$${(lastMcap / 1000000).toFixed(1)}M` : '$18.4M',
      vol: lastVol,
      liq: lastLiq,
      holders: lastHolders
    });
  }
  
  // Send recent transactions
  if (recentTransactions.length > 0) {
    console.log(`[WS] Sending ${recentTransactions.length} recent transactions to client`);
    recentTransactions.forEach(tx => {
      send(ws, { type: 'tx', tx: tx });
    });
  } else {
    send(ws, { 
      type: 'tx', 
      tx: { 
        type: 'info', 
        amount: 'No recent transactions', 
        value: '', 
        time: 'waiting for activity' 
      } 
    });
  }

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (!checkRateLimit(ip)) {
        send(ws, { type: 'error', message: 'Rate limit exceeded. Max 20 msg/min.' });
        return;
      }

      switch (msg.type) {
        case 'subscribe':
          send(ws, { type: 'subscribed', channel: msg.channel || 'all' });
          break;
        case 'chat':
          const reply = getBotReply(msg.message || '');
          send(ws, { type: 'chatReply', reply });
          break;
        case 'ping':
          send(ws, { type: 'pong' });
          break;
        default:
          send(ws, { type: 'error', message: 'Unknown message type' });
      }
    } catch (err) {
      console.error('[WS] Message error:', err.message);
    }
  });

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[WS] Client disconnected. Total: ${clients.size}`);
  });
});

const pingInterval = setInterval(() => {
  clients.forEach(ws => {
    if (!ws.isAlive) {
      clients.delete(ws);
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, WS_PING_INTERVAL);

// ═══════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════
server.listen(PORT, () => {
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   Infinite Coin HQ WS Server v1.4.0     ║');
  console.log('╠══════════════════════════════════════════╣');
  console.log(`║  Port:        ${PORT.toString().padEnd(27)} ║`);
  console.log(`║  Price Int:   ${PRICE_INTERVAL}ms${''.padEnd(18)} ║`);
  console.log(`║  Alert Thresh: ${ALERT_THRESHOLD}%${''.padEnd(20)} ║`);
  console.log(`║  Rate Limit:  ${RATE_LIMIT}/min${''.padEnd(17)} ║`);
  console.log(`║  Helius:      ${HELIUS_API_KEY ? 'ENABLED' : 'DISABLED'}${''.padEnd(22)} ║`);
  console.log('╚══════════════════════════════════════════╝');
});

process.on('SIGTERM', () => {
  clearInterval(pingInterval);
  if (heliusWs) heliusWs.close();
  wss.close(() => {
    server.close(() => process.exit(0));
  });
});
