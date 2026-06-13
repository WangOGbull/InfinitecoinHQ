/**
 * Infinite Coin HQ — WebSocket Command Center Backend
 * Node.js + Express + ws (WebSocket library)
 * 
 * Features:
 * - Jupiter API price polling (every 10s)
 * - Simulated realistic TX feed
 * - AI FAQ bot engine (50+ responses)
 * - Live visitor counter
 * - Price alert detection (5%+ threshold)
 * - Rate limiting (20 msg/min per IP)
 * - Health check endpoint
 * - Railway-ready deploy config
 */

require('dotenv').config();
const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const fetch = require('node-fetch');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ═══════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════
const PORT = process.env.PORT || 3001;
const TOKEN_CA = process.env.TOKEN_CA || 'C8KsvkMBuqmvX416MWTJGKW9S9MpKiUjmpnj1fhzpump';
const JUPITER_API = process.env.JUPITER_API || 'https://price.jup.ag/v6/price';
const PRICE_INTERVAL = parseInt(process.env.PRICE_INTERVAL) || 10000;
const ALERT_THRESHOLD = parseFloat(process.env.ALERT_THRESHOLD) || 5;
const WS_PING_INTERVAL = parseInt(process.env.WS_PING_INTERVAL) || 30000;
const RATE_LIMIT = parseInt(process.env.RATE_LIMIT) || 20;

// State
let lastPrice = 0;
let lastAlertPrice = 0;
const visitors = new Set();        // unique IPs
const rateLimits = new Map();      // ip -> {count, resetTime}
const clients = new Set();

// ═══════════════════════════════════════════
// EXPRESS MIDDLEWARE
// ═══════════════════════════════════════════
app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

// Health check (Railway uses this)
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    connections: clients.size,
    visitors: visitors.size,
    timestamp: new Date().toISOString()
  });
});

// Root
app.get('/', (req, res) => {
  res.json({
    name: 'Infinite Coin HQ WebSocket Server',
    version: '1.0.0',
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
// JUPITER PRICE POLLING
// ═══════════════════════════════════════════
async function fetchPrice() {
  try {
    const url = `${JUPITER_API}?ids=${TOKEN_CA}`;
    const res = await fetch(url, { timeout: 8000 });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const token = data.data?.[TOKEN_CA];
    if (!token?.price) throw new Error('No price data');

    const price = parseFloat(token.price);
    const mcap = price * 1000000000;
    const vol = (890000 + Math.random() * 100000).toFixed(0);
    const liq = (1100000 + Math.random() * 50000).toFixed(0);
    const change = lastPrice > 0
      ? (((price - lastPrice) / lastPrice) * 100).toFixed(2)
      : (Math.random() * 4 - 1).toFixed(2);

    // Check price alert
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

    broadcast({
      type: 'price',
      price: price.toFixed(8),
      change: change,
      mcap: mcap.toFixed(0),
      vol: `$${(vol/1000).toFixed(0)}K`,
      liq: `$${(liq/1000000).toFixed(1)}M`
    });
  } catch (err) {
    console.error('[Price] Error:', err.message);
  }
}

// Start price polling
setInterval(fetchPrice, PRICE_INTERVAL);
fetchPrice();

// ═══════════════════════════════════════════
// SIMULATED TX FEED
// ═══════════════════════════════════════════
const TX_TYPES = ['buy', 'sell'];
const TX_AMOUNTS = [
  [450, 2100], [1200, 5800], [2800, 13600], [500, 2420],
  [8500, 41400], [3200, 15580], [1500, 7310], [6200, 30200],
  [2100, 10230], [9500, 46280], [1800, 8770], [7400, 36050]
];
const TX_SPEEDS = ['2s', '3s', '5s', '8s', '12s', '18s', '25s', '42s', '1m', '2m'];

function simulateTx() {
  const type = TX_TYPES[Math.floor(Math.random() * TX_TYPES.length)];
  const [minAmt, maxAmt] = TX_AMOUNTS[Math.floor(Math.random() * TX_AMOUNTS.length)];
  const amount = Math.floor(minAmt + Math.random() * (maxAmt - minAmt));
  const pricePerToken = lastPrice || 0.00000486;
  const value = (amount * pricePerToken).toFixed(2);
  const time = TX_SPEEDS[Math.floor(Math.random() * TX_SPEEDS.length)];

  broadcast({
    type: 'tx',
    tx: {
      type: type,
      amount: amount.toLocaleString() + ' INF',
      value: '$' + value,
      time: time
    }
  });

  // Random interval between 3-15 seconds
  const nextDelay = 3000 + Math.random() * 12000;
  setTimeout(simulateTx, nextDelay);
}

// Start TX simulation after 2s delay
setTimeout(simulateTx, 2000);

// ═══════════════════════════════════════════
// AI FAQ BOT ENGINE (50+ responses)
// ═══════════════════════════════════════════
const FAQ_BOT = {
  'contract': 'CA: C8KsvkMBuqmvX416MWTJGKW9S9MpKiUjmpnj1fhzpump (Solana SPL). Verify on DexScreener or Jupiter.',
  'ca': 'CA: C8KsvkMBuqmvX416MWTJGKW9S9MpKiUjmpnj1fhzpump',
  'address': 'CA: C8KsvkMBuqmvX416MWTJGKW9S9MpKiUjmpnj1fhzpump',
  'token': '$INFINITE is a Solana SPL token. CA: C8KsvkMBuqmvX416MWTJGKW9S9MpKiUjmpnj1fhzpump',
  'what is infinite': 'Infinite Coin ($INFINITE) is a next-gen memecoin on Solana with P2E games, NFTs, staking (32% APY) and DAO governance.',
  'what is infinite coin': 'Infinite Coin is a community-driven memecoin ecosystem on Solana. Live price, games, staking, NFTs and more.',
  'buy': 'Buy on Jupiter: jup.ag/swap/SOL-C8KsvkMBuqmvX416MWTJGKW9S9MpKiUjmpnj1fhzpump. Connect Phantom, swap SOL for $INFINITE. Use 1-2% slippage.',
  'swap': 'Go to jup.ag and swap SOL -> $INFINITE. Set 1-2% slippage. Make sure you have SOL for gas.',
  'how to buy': '1) Get Phantom wallet 2) Buy SOL 3) Go to jup.ag 4) Swap SOL -> $INFINITE 5) Confirm transaction',
  'purchase': 'Use Jupiter DEX: jup.ag. Connect wallet, swap SOL for $INFINITE with 1-2% slippage.',
  'where to buy': 'Buy on Jupiter DEX (jup.ag) or via Phantom wallet. You can also track on DexScreener.',
  'jupiter': 'Jupiter is the leading DEX aggregator on Solana. Use jup.ag/swap/SOL-$INFINITE to buy.',
  'phantom': 'Phantom is the #1 Solana wallet. Download at phantom.app, fund with SOL, then swap for $INFINITE on Jupiter.',
  'wallet': 'We recommend Phantom (phantom.app) or Solflare. Both support $INFINITE and all Solana dApps.',
  'solflare': 'Solflare is a great Solana wallet. Download at solflare.com and swap for $INFINITE on Jupiter.',
  'stake': '32% APY staking coming Q3 2026. Auto-compounding daily. Early stakers get bonus rewards.',
  'staking': '32% APY. Auto-compounding. Launching Q3 2026. Connect wallet on the staking page to get notified.',
  'apy': '32% APY on staked $INFINITE. Rewards auto-compound daily. No lock-up period planned.',
  'apy?': '32% APY. One of the highest in the memecoin space.',
  'reward': 'Staking rewards are paid in $INFINITE with 32% APY, auto-compounding daily.',
  'compound': 'Yes! Staking rewards auto-compound daily. Set it and forget it.',
  'lock': 'No lock-up period planned. Flexible staking with 32% APY.',
  'game': 'P2E Games launching Q3 2026: Infinite Runner, Infinite Jumper, Infinite Puzzle. All reward $INF.',
  'p2e': 'Play-to-Earn Arcade: Infinite Runner (endless), Infinite Jumper (platformer), Infinite Puzzle (match-3). All pay $INF rewards.',
  'runner': 'Infinite Runner: Endless runner through infinite worlds. Collect power-ups, earn $INF. Launching Q3 2026.',
  'jumper': 'Infinite Jumper: Platform your way to the moon. Each level earns $INF. Launching Q3 2026.',
  'puzzle': 'Infinite Puzzle: Match-3 with crypto combos. Chain reactions for multiplier $INF rewards. Q3 2026.',
  'play': 'Games launch Q3 2026. Beta access for Genesis NFT holders. Connect wallet to join waitlist.',
  'beta': 'Beta opens Q3 2026. Genesis NFT holders get priority access. 4,200+ already signed up.',
  'nft': 'Genesis Collection: 5,000 exclusive pieces. Holders get airdrops, beta access, governance voting and staking boosts.',
  'genesis': '5,000 Genesis NFTs. Benefits: airdrops, P2E beta, DAO votes, staking multipliers, exclusive merch.',
  'collection': 'Genesis: 5,000 pieces. Legendary, Epic, Rare and Common tiers. Mint price TBA.',
  'mint': 'Genesis NFT mint date TBA. Follow @infinitecoinhq on X for announcements.',
  'airdrop': 'Genesis NFT holders receive periodic $INFINITE airdrops. Snapshot dates announced on X.',
  'supply': 'Total Supply: 1,000,000,000 $INFINITE (1B). No mint authority. Fixed supply forever.',
  'total supply': '1 billion $INFINITE. Fixed. No more can ever be minted.',
  'burn': 'Deflationary mechanics via game fees and NFT royalties. Burn address publicly tracked.',
  'deflationary': 'Yes. Game fees, NFT royalties and staking penalties feed the burn wallet. Supply decreases over time.',
  'mcap': 'Market cap updates live from Jupiter. Check the Trading Headquarters section for real-time data.',
  'market cap': 'Live market cap shown in Trading HQ. Calculated from Jupiter price * circulating supply.',
  'liquidity': '$1.1M+ liquidity on Solana DEXs via Jupiter. LP tokens locked.',
  'holders': '18,420+ real holders and growing fast. Check the live counter in the status bar.',
  'community': 'Join us: X @infinitecoinhq, TikTok @infinitecoinhq, YouTube @infinitecoinhq, Insta @infinitecoinhq, Telegram @InfiniteCoinHQ.',
  'dao': 'Decentralized governance launching 2026. Genesis NFT holders = voting power. 1 NFT = 1 vote.',
  'governance': 'DAO votes on: game features, staking changes, partnerships, burns. Power to the community.',
  'vote': 'Voting opens with DAO launch 2026. Genesis NFT holders decide the future.',
  'solana': 'Built on Solana for lightning-fast transactions (<400ms) and near-zero fees (<$0.001).',
  'chain': 'Solana. 65,000+ TPS, sub-second finality, <$0.001 per tx.',
  'fee': 'Transaction fees on Solana are <$0.001. Basically free.',
  'gas': 'You need a tiny amount of SOL for gas (~0.000005 SOL per tx).',
  'speed': 'Solana processes transactions in ~400ms. Near-instant.',
  'security': 'Contract audited. LP locked. No mint authority. Renounced ownership.',
  'audit': 'Smart contract audited by leading security firms. Report published on our X.',
  'roadmap': 'Q2 2026: Website + WS live. Q3 2026: Staking + P2E Games. Q4 2026: NFT Genesis + DAO. 2027: Mobile app + CEX listing.',
  'q3': 'Q3 2026: Staking platform (32% APY), P2E Games (Runner/Jumper/Puzzle), MemeFun upload.',
  'q4': 'Q4 2026: Genesis NFT mint, DAO governance, major partnerships, CEX discussions.',
  'cex': 'CEX listing discussions ongoing. Targeting Q4 2026 / Q1 2027.',
  'mobile': 'Mobile app planned for 2027. iOS + Android with wallet + games + staking.',
  'help': 'Ask me about: contract, buying, staking, games, NFTs, tokenomics, roadmap, community, or security.',
  'hello': 'Welcome to Infinite Coin HQ! How can I help you today?',
  'hi': 'Hey there! Ready to go infinite? Ask me anything about $INFINITE.',
  'hey': 'Hey! Ask about contract, buying, staking, games or say "help" for all topics.',
  'thank': 'You are welcome! LFG!',
  'thanks': 'Anytime! To the moon!',
  'lfg': 'LFG!!!',
  'moon': 'To the moon!',
  'wagmi': 'WAGMI! Diamond hands always!',
  'gm': 'GM! Another day closer to the moon!',
  'gn': 'GN! Rest well, holder! See you on the moon tomorrow!',
  'price': 'Check the live price in the Trading Headquarters section! Updated in real-time via WebSocket.',
  'chart': 'Live chart embedded from DexScreener in the Trading HQ section. Updated every block.',
  'dexscreener': 'Track $INFINITE at dexscreener.com/solana/C8KsvkMBuqmvX416MWTJGKW9S9MpKiUjmpnj1fhzpump',
  'pump': 'Launched on Pump.fun! Join: join.pump.fun/HSag/kuu4214i',
  'pump.fun': 'We launched on Pump.fun! Join the pump at join.pump.fun/HSag/kuu4214i',
  'lemon8': 'Follow us on Lemon8: v.lemon8-app.com/s/OgSbYMhydp',
  'youtube': 'Subscribe: youtube.com/@infinitecoinhq',
  'tiktok': 'Follow: tiktok.com/@infinitecoinhq',
  'twitter': 'Follow: x.com/infinitecoinhq',
  'x': 'Follow: x.com/infinitecoinhq (@infinitecoinhq)',
  'instagram': 'Follow: instagram.com/infinitecoinhq',
  'telegram': 'Join: t.me/InfiniteCoinHQ (@InfiniteCoinHQ)',
  'social': 'Find us everywhere: @infinitecoinhq on X, TikTok, YT, Insta, Lemon8 + Telegram @InfiniteCoinHQ',
  'contact': 'Reach us: X DM @infinitecoinhq or Telegram @InfiniteCoinHQ',
  'dm': 'DM us on X @infinitecoinhq or join Telegram @InfiniteCoinHQ',
  'scam': 'Beware of scams! Official CA: C8KsvkMBuqmvX416MWTJGKW9S9MpKiUjmpnj1fhzpump. We never DM first.',
  'fake': 'Only trust links from our official X @infinitecoinhq. Never click random links in DMs!',
  '_default': 'I am your AI assistant. Ask about: contract, buying, staking, games, NFTs, roadmap, or say "help" for all topics.'
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

  console.log(`[WS] Client connected from ${ip}. Total: ${clients.size}, Visitors: ${visitors.size}`);

  // Send visitor count to all
  broadcast({ type: 'visitorCount', count: visitors.size });

  // Send current price immediately
  if (lastPrice > 0) {
    send(ws, {
      type: 'price',
      price: lastPrice.toFixed(8),
      change: '0.00',
      mcap: (lastPrice * 1000000000).toFixed(0),
      vol: '$890K',
      liq: '$1.1M'
    });
  }

  // Message handler
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      // Rate limit check
      if (!checkRateLimit(ip)) {
        send(ws, { type: 'error', message: 'Rate limit exceeded. Max 20 msg/min.' });
        return;
      }

      switch (msg.type) {
        case 'subscribe':
          // Client subscribed to a channel
          send(ws, { type: 'subscribed', channel: msg.channel || 'all' });
          break;

        case 'chat':
          // AI bot response
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

  // Ping/pong keepalive
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[WS] Client disconnected. Total: ${clients.size}`);
  });

  ws.on('error', (err) => {
    console.error('[WS] Error:', err.message);
    clients.delete(ws);
  });
});

// Ping all clients periodically
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
  console.log('║   Infinite Coin HQ WS Server v1.0.0     ║');
  console.log('╠══════════════════════════════════════════╣');
  console.log(`║  Port:        ${PORT.toString().padEnd(27)} ║`);
  console.log(`║  Price Int:   ${PRICE_INTERVAL}ms${''.padEnd(18)} ║`);
  console.log(`║  Alert Thresh: ${ALERT_THRESHOLD}%${''.padEnd(20)} ║`);
  console.log(`║  Rate Limit:  ${RATE_LIMIT}/min${''.padEnd(17)} ║`);
  console.log('╚══════════════════════════════════════════╝');
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[Server] SIGTERM received, shutting down gracefully...');
  clearInterval(pingInterval);
  wss.close(() => {
    server.close(() => {
      process.exit(0);
    });
  });
});
