const express = require('express');
const fetch = require('node-fetch');
const cheerio = require('cheerio');

const app = express();
const PORT = process.env.PORT || 3001;
const YEAR = 2026;

// AFLTables all-teams season totals page — updates within hours of each game
const AFL_URL = `https://afltables.com/afl/stats/${YEAR}a.html`;
const CACHE_TTL = 15 * 60 * 1000; // 15 minutes

// AFLTables team abbreviation → full name (must match TEAM_COLORS keys in the app)
const TEAM_MAP = {
  'AD': 'Adelaide Crows',
  'BL': 'Brisbane Lions',
  'CA': 'Carlton Blues',
  'CW': 'Collingwood Magpies',
  'ES': 'Essendon Bombers',
  'FR': 'Fremantle Dockers',
  'GE': 'Geelong Cats',
  'GC': 'Gold Coast Suns',
  'GW': 'GWS Giants',
  'HW': 'Hawthorn Hawks',
  'ME': 'Melbourne Demons',
  'NM': 'North Melbourne',
  'PA': 'Port Adelaide',
  'RI': 'Richmond Tigers',
  'SK': 'St Kilda Saints',
  'SY': 'Sydney Swans',
  'WC': 'West Coast Eagles',
  'WB': 'Western Bulldogs',
};

// Confirmed column indices from AFLTables 2026a.html header row:
// Player(0) | TM(1) | GM(2) | KI(3) | MK(4) | HB(5) | DI(6) | GL(7) | BH(8) | HO(9) | TK(10) | ...
const COL = { NAME: 0, TEAM: 1, GAMES: 2, MARKS: 4, DISPOSALS: 6, GOALS: 7, BEHINDS: 8, HITOUTS: 9, TACKLES: 10 };

// Raw full player list (all players, unsorted) — shared between endpoints
let cachedRaw = null;
let cachedAt = 0;

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  next();
});

async function scrapeAFLTables() {
  console.log(`Fetching: ${AFL_URL}`);
  const res = await fetch(AFL_URL, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      'Accept': 'text/html',
    },
  });

  if (!res.ok) throw new Error(`AFLTables HTTP ${res.status}`);
  const html = await res.text();
  const $ = cheerio.load(html);

  // First table = season totals (sorted by disposals by default)
  const table = $('table').eq(0);
  const rows = table.find('tr');

  if (rows.length < 5) throw new Error('Table too small — page structure may have changed');

  // Verify header looks right
  const headerCells = [];
  $(rows[0]).find('td, th').each((_, c) => headerCells.push($(c).text().trim()));
  if (!headerCells.includes('DI') || !headerCells.includes('GL')) {
    throw new Error(`Unexpected headers: ${headerCells.slice(0, 10).join(', ')}`);
  }

  const players = [];

  rows.each((i, row) => {
    if (i === 0) return; // skip header

    const cells = [];
    $(row).find('td').each((_, c) => cells.push($(c).text().trim()));
    if (cells.length < 11) return;

    const rawName = cells[COL.NAME];
    if (!rawName) return;

    // AFLTables format: "Lastname, Firstname" → "Firstname Lastname"
    const parts = rawName.split(',');
    const name = parts.length === 2
      ? `${parts[1].trim()} ${parts[0].trim()}`
      : rawName.trim();

    const teamAbbr = cells[COL.TEAM];
    const games = parseInt(cells[COL.GAMES]) || 0;
    if (!name || games === 0) return;

    const int = (idx) => parseInt(cells[idx]) || 0;

    players.push({
      id: players.length + 1,
      name,
      team: TEAM_MAP[teamAbbr] || teamAbbr,
      games,
      disposals: int(COL.DISPOSALS),
      marks:     int(COL.MARKS),
      hitouts:   int(COL.HITOUTS),
      tackles:   int(COL.TACKLES),
      goals:     int(COL.GOALS),
      behinds:   int(COL.BEHINDS),
    });
  });

  if (players.length < 10) {
    throw new Error(`Only parsed ${players.length} players — scraper may be broken`);
  }

  // Deduplicate by name+team — AFLTables can list a player in multiple rows
  // (e.g. pre-season vs regular season) which would cause duplicates in the app
  const seen = new Set();
  const deduped = players.filter(p => {
    const key = `${p.name}|${p.team}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  console.log(`Parsed ${players.length} rows → ${deduped.length} unique players from ${YEAR} season`);
  return deduped;
}

// Returns top 100 forwards ranked by avg forward points
function rankForwards(raw) {
  return raw
    .map(p => ({ ...p, avgForwardPoints: (6 * p.goals + p.behinds) / p.games }))
    .sort((a, b) => b.avgForwardPoints - a.avgForwardPoints)
    .slice(0, 100)
    .map((p, i) => ({ ...p, rank: i + 1 }));
}

// Returns top 100 mids ranked by avg disposals per game
function rankMids(raw) {
  return raw
    .map(p => ({ ...p, avgDisposals: p.disposals / p.games }))
    .sort((a, b) => b.avgDisposals - a.avgDisposals)
    .slice(0, 100)
    .map((p, i) => ({ ...p, rank: i + 1 }));
}

// Returns top 100 tacklers ranked by avg tackle points per game (tackles × 4 / games)
function rankTacklers(raw) {
  return raw
    .map(p => ({ ...p, avgTacklePoints: (p.tackles * 4) / p.games }))
    .sort((a, b) => b.avgTacklePoints - a.avgTacklePoints)
    .slice(0, 100)
    .map((p, i) => ({ ...p, rank: i + 1 }));
}

// Returns top 100 rucks ranked by avg ruck points per game ((hitouts + marks) / games)
function rankRucks(raw) {
  return raw
    .map(p => ({ ...p, avgRuckPoints: (p.hitouts + p.marks) / p.games }))
    .sort((a, b) => b.avgRuckPoints - a.avgRuckPoints)
    .slice(0, 100)
    .map((p, i) => ({ ...p, rank: i + 1 }));
}

// Returns top 100 utilities ranked by avg utility points per game
// Formula: (goals×6 + behinds + disposals + tackles×4 + hitouts + marks) / 2 / games
function utilityScore(p) {
  return (p.goals * 6 + p.behinds + p.disposals + p.tackles * 4 + p.hitouts + p.marks) / 2;
}

function rankUtilities(raw) {
  return raw
    .map(p => ({ ...p, totalUtilityPoints: utilityScore(p), avgUtilityPoints: utilityScore(p) / p.games }))
    .sort((a, b) => b.avgUtilityPoints - a.avgUtilityPoints)
    .slice(0, 100)
    .map((p, i) => ({ ...p, rank: i + 1 }));
}

async function getOrFetchRaw() {
  const now = Date.now();
  if (cachedRaw && now - cachedAt < CACHE_TTL) {
    console.log('Serving from cache');
    return cachedRaw;
  }
  cachedRaw = await scrapeAFLTables();
  cachedAt = Date.now();
  return cachedRaw;
}

app.get('/api/refresh', async (req, res) => {
  cachedRaw = null;
  cachedAt = 0;
  cachedFixture = null;
  fixtureCachedAt = 0;
  try {
    const raw = await getOrFetchRaw();
    res.json({ ok: true, players: raw.length, cachedAt: new Date().toISOString() });
  } catch (err) {
    res.status(503).json({ ok: false, error: err.message });
  }
});

app.get('/api/players', async (req, res) => {
  try {
    const raw = await getOrFetchRaw();
    res.json(rankForwards(raw));
  } catch (err) {
    console.error('Scrape error:', err.message);
    if (cachedRaw) return res.json(rankForwards(cachedRaw));
    res.status(503).json({ error: err.message });
  }
});

// All players (unfiltered, unranked) — used by MyTeam screen to merge live stats
app.get('/api/all', async (req, res) => {
  try {
    const raw = await getOrFetchRaw();
    res.json(raw);
  } catch (err) {
    console.error('Scrape error:', err.message);
    if (cachedRaw) return res.json(cachedRaw);
    res.status(503).json({ error: err.message });
  }
});

app.get('/api/mids', async (req, res) => {
  try {
    const raw = await getOrFetchRaw();
    res.json(rankMids(raw));
  } catch (err) {
    console.error('Scrape error:', err.message);
    if (cachedRaw) return res.json(rankMids(cachedRaw));
    res.status(503).json({ error: err.message });
  }
});

app.get('/api/tacklers', async (req, res) => {
  try {
    const raw = await getOrFetchRaw();
    res.json(rankTacklers(raw));
  } catch (err) {
    console.error('Scrape error:', err.message);
    if (cachedRaw) return res.json(rankTacklers(cachedRaw));
    res.status(503).json({ error: err.message });
  }
});

app.get('/api/rucks', async (req, res) => {
  try {
    const raw = await getOrFetchRaw();
    res.json(rankRucks(raw));
  } catch (err) {
    console.error('Scrape error:', err.message);
    if (cachedRaw) return res.json(rankRucks(cachedRaw));
    res.status(503).json({ error: err.message });
  }
});

app.get('/api/utilities', async (req, res) => {
  try {
    const raw = await getOrFetchRaw();
    res.json(rankUtilities(raw));
  } catch (err) {
    console.error('Scrape error:', err.message);
    if (cachedRaw) return res.json(rankUtilities(cachedRaw));
    res.status(503).json({ error: err.message });
  }
});

// ── Fixture (next opponent) via Squiggle API ──────────────────────────────────
const SQUIGGLE_TO_APP = {
  'Adelaide':               'Adelaide Crows',
  'Brisbane':               'Brisbane Lions',
  'Carlton':                'Carlton Blues',
  'Collingwood':            'Collingwood Magpies',
  'Essendon':               'Essendon Bombers',
  'Fremantle':              'Fremantle Dockers',
  'Geelong':                'Geelong Cats',
  'Gold Coast':             'Gold Coast Suns',
  'GWS':                    'GWS Giants',
  'Greater Western Sydney': 'GWS Giants',
  'Hawthorn':               'Hawthorn Hawks',
  'Melbourne':              'Melbourne Demons',
  'North Melbourne':        'North Melbourne',
  'Port Adelaide':          'Port Adelaide',
  'Richmond':               'Richmond Tigers',
  'St Kilda':               'St Kilda Saints',
  'Sydney':                 'Sydney Swans',
  'West Coast':             'West Coast Eagles',
  'Western Bulldogs':       'Western Bulldogs',
};

let cachedFixture = null;
let fixtureCachedAt = 0;
const FIXTURE_TTL = 60 * 60 * 1000; // 1 hour

async function fetchFixture() {
  const r = await fetch('https://api.squiggle.com.au/?q=games;year=2026', {
    headers: { 'User-Agent': 'FFLoEDGE/1.0 (fantasy stats app)' },
  });
  if (!r.ok) throw new Error(`Squiggle HTTP ${r.status}`);
  const { games } = await r.json();

  const sorted = [...games].sort((a, b) => new Date(a.date) - new Date(b.date));
  const allTeams = [...new Set(Object.values(SQUIGGLE_TO_APP))];

  // Find the upcoming round: lowest round with any unplayed game
  let squiggleUpcoming = null;
  for (const g of sorted) {
    if (g.complete < 100 && g.hteam && g.ateam) {
      squiggleUpcoming = g.round;
      break;
    }
  }
  // App rounds match Squiggle round numbers directly (Squiggle Round 0 = preseason, Round 1 = AFL Rd 1)
  // AFLTables offset: afltablesRound = appRound + 1 (handled in scrapeRoundStats)
  const upcomingRound = squiggleUpcoming !== null ? squiggleUpcoming : null;

  // Build nextGame map
  const nextGame = {};
  if (squiggleUpcoming !== null) {
    const roundGames = sorted.filter(g => g.round === squiggleUpcoming && g.hteam && g.ateam);
    const playingThisRound = new Set();
    for (const g of roundGames) {
      const home = SQUIGGLE_TO_APP[g.hteam] || g.hteam;
      const away = SQUIGGLE_TO_APP[g.ateam] || g.ateam;
      playingThisRound.add(home);
      playingThisRound.add(away);
      if (g.complete < 100) {
        nextGame[home] = { opponent: away, round: g.roundname, date: g.date, home: true,  venue: g.venue || null };
        nextGame[away] = { opponent: home, round: g.roundname, date: g.date, home: false, venue: g.venue || null };
      }
    }
    for (const team of allTeams) {
      if (!playingThisRound.has(team)) {
        nextGame[team] = { played: true, round: `Round ${squiggleUpcoming}` };
      }
    }
  }
  // Fallback for teams with no upcoming entry
  for (const g of [...sorted].reverse()) {
    if (!g.hteam || !g.ateam) continue;
    const home = SQUIGGLE_TO_APP[g.hteam] || g.hteam;
    const away = SQUIGGLE_TO_APP[g.ateam] || g.ateam;
    if (!nextGame[home]) nextGame[home] = { opponent: away, round: g.roundname, date: g.date, home: true,  played: true };
    if (!nextGame[away]) nextGame[away] = { opponent: home, round: g.roundname, date: g.date, home: false, played: true };
  }

  // Build historical fixture/byes from ALL Squiggle rounds (not just Squiggle-complete ones)
  // so fixture info is available even when AFL Tables publishes data before Squiggle marks complete.
  const roundGroups = {};
  for (const g of sorted) {
    if (!g.hteam || !g.ateam) continue;
    if (!roundGroups[g.round]) roundGroups[g.round] = [];
    roundGroups[g.round].push(g);
  }

  const historicalFixture = {};
  const historicalByes    = {};

  for (const [sqRound, roundGames] of Object.entries(roundGroups)) {
    const appRound = parseInt(sqRound);
    const fixture  = {};
    const playing  = new Set();
    for (const g of roundGames) {
      const home = SQUIGGLE_TO_APP[g.hteam] || g.hteam;
      const away = SQUIGGLE_TO_APP[g.ateam] || g.ateam;
      fixture[home] = away;
      fixture[away] = home;
      playing.add(home);
      playing.add(away);
    }
    historicalFixture[appRound] = fixture;
    historicalByes[appRound]    = allTeams.filter(t => !playing.has(t));
  }

  // Use AFL Tables as the authority on which rounds have stats published.
  // A round is "complete" if AFL Tables has game pages for it — regardless of
  // whether Squiggle has marked every game complete yet.
  await refreshGameLinks();
  const completedRounds = [];
  for (const [afltablesRoundStr, links] of Object.entries(cachedGameLinks)) {
    if (links.length > 0) {
      completedRounds.push(parseInt(afltablesRoundStr) - 1); // convert to appRound
    }
  }
  completedRounds.sort((a, b) => a - b);

  // Derive upcomingRound: first app round not yet in AFL Tables
  const maxCompleted = completedRounds.length > 0 ? Math.max(...completedRounds) : -1;
  const derivedUpcoming = maxCompleted + 1;
  // Use the higher of Squiggle's upcoming and AFL Tables derived — ensures we never
  // show a round as "upcoming" that AFL Tables already has stats for.
  const finalUpcoming = Math.max(upcomingRound ?? 0, derivedUpcoming);

  console.log(`Fixture: Squiggle upcoming=${upcomingRound}, AFL Tables completedRounds=[${completedRounds.join(', ')}], finalUpcoming=${finalUpcoming}`);
  return { nextGame, upcomingRound: finalUpcoming, completedRounds, historicalFixture, historicalByes };
}

async function getOrFetchFixture() {
  const now = Date.now();
  if (cachedFixture && now - fixtureCachedAt < FIXTURE_TTL) return cachedFixture;
  cachedFixture = await fetchFixture();
  fixtureCachedAt = Date.now();
  return cachedFixture;
}

// ── Venue data: AFL grounds with coordinates + roofed flag ────────────────────
const VENUE_DATA = {
  'MCG':                            { lat: -37.8199, lon: 144.9836, roofed: false },
  'M.C.G.':                         { lat: -37.8199, lon: 144.9836, roofed: false },
  'Marvel Stadium':                 { lat: -37.8162, lon: 144.9477, roofed: true  },
  'Docklands':                      { lat: -37.8162, lon: 144.9477, roofed: true  },
  'GMHBA Stadium':                  { lat: -38.1584, lon: 144.3543, roofed: false },
  'Kardinia Park':                  { lat: -38.1584, lon: 144.3543, roofed: false },
  'Adelaide Oval':                  { lat: -34.9155, lon: 138.5963, roofed: false },
  'Optus Stadium':                  { lat: -31.9505, lon: 115.8890, roofed: false },
  'Perth Stadium':                  { lat: -31.9505, lon: 115.8890, roofed: false },
  'SCG':                            { lat: -33.8914, lon: 151.2247, roofed: false },
  'Stadium Australia':              { lat: -33.8474, lon: 151.0631, roofed: false },
  'Accor Stadium':                  { lat: -33.8474, lon: 151.0631, roofed: false },
  'Gabba':                          { lat: -27.4858, lon: 153.0381, roofed: false },
  'People First Stadium':           { lat: -28.0046, lon: 153.4160, roofed: false },
  'Metricon Stadium':               { lat: -28.0046, lon: 153.4160, roofed: false },
  'ENGIE Stadium':                  { lat: -33.8474, lon: 150.9480, roofed: false },
  'Giants Stadium':                 { lat: -33.8474, lon: 150.9480, roofed: false },
  'TIO Traeger Park':               { lat: -23.7021, lon: 133.8828, roofed: false },
  'TIO Stadium':                    { lat: -12.4117, lon: 130.8780, roofed: false },
  'Cazalys Stadium':                { lat: -16.9283, lon: 145.7522, roofed: false },
  'Riverway Stadium':               { lat: -19.2813, lon: 146.7892, roofed: false },
  'UTAS Stadium':                   { lat: -41.4516, lon: 147.1411, roofed: false },
  'Blundstone Arena':               { lat: -42.8912, lon: 147.3274, roofed: false },
  'Mars Stadium':                   { lat: -37.5643, lon: 143.8441, roofed: false },
  'Manuka Oval':                    { lat: -35.3200, lon: 149.1303, roofed: false },
  'University of Tasmania Stadium': { lat: -41.4516, lon: 147.1411, roofed: false },
};

function wmoToCondition(code) {
  if (code === 0)           return 'sunny';
  if (code <= 2)            return 'partly-sunny';
  if (code <= 3)            return 'cloudy';
  if (code <= 48)           return 'foggy';
  if (code <= 67)           return 'rainy';
  if (code <= 77)           return 'snowy';
  if (code <= 82)           return 'rainy';
  if (code <= 86)           return 'snowy';
  return 'stormy';
}

let cachedWeather = null;
let weatherCachedAt = 0;
const WEATHER_TTL = 60 * 60 * 1000; // 1 hour

async function fetchUpcomingWeather() {
  const { nextGame } = await getOrFetchFixture();

  // Collect unique venue→date pairs from unplayed upcoming games
  const venueMap = {};
  for (const game of Object.values(nextGame)) {
    if (!game.played && game.venue && game.date && !venueMap[game.venue]) {
      venueMap[game.venue] = game.date;
    }
  }

  const results = {};
  await Promise.all(
    Object.entries(venueMap).map(async ([venue, date]) => {
      const vd = VENUE_DATA[venue];
      if (!vd) return;
      if (vd.roofed) { results[venue] = { roofed: true }; return; }

      try {
        const gameDate = date.split(' ')[0];
        const url = `https://api.open-meteo.com/v1/forecast?latitude=${vd.lat}&longitude=${vd.lon}&daily=weather_code,temperature_2m_max&timezone=auto&forecast_days=14`;
        const wr = await fetch(url, { headers: { 'User-Agent': 'FFLoEDGE/1.0' } });
        if (!wr.ok) return;
        const data = await wr.json();
        const idx = (data.daily?.time || []).indexOf(gameDate);
        if (idx === -1) return;
        const code = data.daily.weather_code[idx];
        const temp = Math.round(data.daily.temperature_2m_max[idx]);
        results[venue] = { roofed: false, condition: wmoToCondition(code), code, temp };
      } catch (_) {}
    })
  );
  return results;
}

app.get('/api/weather/upcoming', async (req, res) => {
  try {
    const now = Date.now();
    if (cachedWeather && now - weatherCachedAt < WEATHER_TTL) {
      return res.json(cachedWeather);
    }
    cachedWeather = await fetchUpcomingWeather();
    weatherCachedAt = Date.now();
    res.json(cachedWeather);
  } catch (e) {
    console.error('Weather error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Per-round stats (scraped from AFLTables individual game pages) ────────────
// AFLTables numbers rounds from 1. Our app numbers from 0. Offset = +1.
// e.g. App Round 0 → AFLTables Round 1, App Round 1 → AFLTables Round 2.

const cachedRounds = {};        // { appRound: { players, cachedAt } }
const cachedGameLinks = {};     // { afltablesRound: [gameId, ...] }
let seasonPageCachedAt = 0;
const SEASON_PAGE_TTL = 60 * 60 * 1000; // re-check season page every hour

const SEASON_PAGE_URL = `https://afltables.com/afl/seas/${YEAR}.html`;
const GAME_BASE_URL   = `https://afltables.com/afl/stats/games/${YEAR}/`;
const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'text/html',
};

// Fetch season page and rebuild cachedGameLinks map: { afltablesRound → [gameId] }
async function refreshGameLinks() {
  const now = Date.now();
  if (seasonPageCachedAt && now - seasonPageCachedAt < SEASON_PAGE_TTL &&
      Object.keys(cachedGameLinks).length > 0) return;

  const res = await fetch(SEASON_PAGE_URL, { headers: HEADERS });
  if (!res.ok) throw new Error(`Season page HTTP ${res.status}`);
  const html = await res.text();

  // Split HTML by round anchors: <a name="N"></a>
  const parts = html.split(/<a name=["']?(\d+)["']?><\/a>/);
  // parts = [pre, '1', content1, '2', content2, ...]
  for (let i = 1; i < parts.length - 1; i += 2) {
    const afltablesRound = parseInt(parts[i]);
    const content = parts[i + 1] || '';
    const links = (content.match(/stats\/games\/\d+\/([^"<]+\.html)/g) || [])
      .map(l => l.replace('stats/games/' + YEAR + '/', ''));
    if (links.length > 0) cachedGameLinks[afltablesRound] = links;
  }

  seasonPageCachedAt = now;
  console.log(`Season page refreshed: rounds with games = [${Object.keys(cachedGameLinks).join(', ')}]`);
}

// Parse a single game stats page; returns array of player stat objects
async function scrapeGamePage(gameId) {
  const url = GAME_BASE_URL + gameId;
  const res = await fetch(url, { headers: HEADERS });
  if (!res.ok) throw new Error(`Game page ${gameId} HTTP ${res.status}`);
  const html = await res.text();
  const $ = cheerio.load(html);
  const players = [];

  // Player stat tables have class="sortable". Each has a header <th> like
  // "Sydney Match Statistics [...]" — extract team name from that.
  $('table.sortable').each((_, tbl) => {
    // Team name is in the first <th> cell: "TeamName Match Statistics [...]"
    const headerTh = $(tbl).find('thead th').first().text().trim();
    const teamShort = headerTh.replace(/\s*Match Statistics.*$/i, '').trim();
    const teamFull = SQUIGGLE_TO_APP[teamShort] || teamShort || null;

    // Build column index map from the second header row (has DI, GL, etc.)
    let colMap = null;
    $(tbl).find('tr').each((_, row) => {
      if (colMap) return;
      const cells = [];
      $(row).find('th').each((_, c) => cells.push($(c).text().trim()));
      if (cells.includes('DI') && cells.includes('GL')) {
        colMap = {};
        cells.forEach((c, idx) => { colMap[c] = idx; });
      }
    });
    if (!colMap) return;

    // Parse player rows (tbody rows only, skip header/footer)
    $(tbl).find('tbody tr').each((_, row) => {
      const cells = [];
      $(row).find('td').each((_, c) => cells.push($(c).text().trim()));
      if (cells.length < 10) return;
      const rawName = cells[colMap['Player'] ?? 1];
      if (!rawName || rawName === 'Player' || rawName === 'Totals') return;
      const parts = rawName.split(',');
      if (parts.length < 2) return;
      const name = `${parts[1].trim()} ${parts[0].trim()}`;
      const int = (key) => { const v = parseInt(cells[colMap[key]]); return isNaN(v) ? 0 : v; };
      players.push({
        name,
        team: teamFull,
        goals:     int('GL'),
        behinds:   int('BH'),
        disposals: int('DI'),
        tackles:   int('TK'),
        marks:     int('MK'),
        hitouts:   int('HO'),
      });
    });
  });

  return players;
}

// Aggregate all game pages for a given app round into one player list
async function scrapeRoundStats(appRound) {
  await refreshGameLinks();
  const afltablesRound = appRound + 1; // offset
  const gameIds = cachedGameLinks[afltablesRound];
  if (!gameIds || gameIds.length === 0) {
    throw new Error(`No games found for round ${appRound} (AFL Tables round ${afltablesRound})`);
  }

  console.log(`Round ${appRound}: fetching ${gameIds.length} game pages...`);
  const allPlayers = [];
  await Promise.all(gameIds.map(async (id) => {
    try {
      const ps = await scrapeGamePage(id);
      allPlayers.push(...ps);
    } catch (e) {
      console.warn(`  Skipping ${id}: ${e.message}`);
    }
  }));

  if (allPlayers.length < 10) {
    throw new Error(`Only ${allPlayers.length} players parsed across ${gameIds.length} games`);
  }
  console.log(`Round ${appRound}: ${allPlayers.length} player-game records from ${gameIds.length} games`);
  return allPlayers;
}

app.get('/api/round/:num', async (req, res) => {
  const roundNum = parseInt(req.params.num);
  if (isNaN(roundNum) || roundNum < 0 || roundNum > 30) {
    return res.status(400).json({ error: 'Invalid round number' });
  }
  const now = Date.now();
  const cached = cachedRounds[roundNum];
  if (cached && now - cached.cachedAt < CACHE_TTL) return res.json(cached.players);
  try {
    const players = await scrapeRoundStats(roundNum);
    cachedRounds[roundNum] = { players, cachedAt: now };
    res.json(players);
  } catch (err) {
    console.error(`Round ${roundNum} error:`, err.message);
    if (cached) return res.json(cached.players);
    res.status(404).json({ error: err.message });
  }
});

app.get('/api/rounds', (req, res) => {
  const available = Object.keys(cachedRounds)
    .filter(r => cachedRounds[r]?.players?.length > 0)
    .map(Number)
    .sort((a, b) => a - b);
  res.json({ available });
});

app.get('/api/fixture', async (req, res) => {
  try {
    res.json(await getOrFetchFixture());
  } catch (err) {
    console.error('Fixture error:', err.message);
    if (cachedFixture) return res.json(cachedFixture);
    res.status(503).json({ error: err.message });
  }
});

app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    year: YEAR,
    source: 'AFLTables',
    cached: !!cachedRaw,
    cachedAt: cachedAt ? new Date(cachedAt).toISOString() : null,
  });
});

app.listen(PORT, () => {
  console.log(`FFLoEDGE API running on http://localhost:${PORT}`);
  console.log(`Source: AFLTables ${YEAR} season (updates within hours of each game)`);
});
