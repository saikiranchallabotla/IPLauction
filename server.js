const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer);

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ============ DATA STORE ============
const INITIAL_BUDGET = 100;
const ROOMS_FILE = path.join(__dirname, 'data', 'rooms.json');
const MONGODB_URI = process.env.MONGODB_URI;
const CRICAPI_KEY = process.env.CRICAPI_KEY || '';
const CRICAPI_SERIES_ID = process.env.CRICAPI_SERIES_ID || '';
const IPL_FANTASY_UID = process.env.IPL_FANTASY_UID || '';
const IPL_FANTASY_AUTH_TOKEN = process.env.IPL_FANTASY_AUTH_TOKEN || '';
const FANTASY_REFRESH_INTERVAL = parseInt(process.env.FANTASY_REFRESH_INTERVAL) || 5; // minutes

// IPL Season configuration — determines which season's data to fetch
const IPL_SEASON_YEAR = parseInt(process.env.IPL_SEASON_YEAR) || new Date().getFullYear();
// Starting tourgamedayId for the current season (override if API uses cumulative IDs across seasons)
const IPL_START_GAMEDAY_ID = parseInt(process.env.IPL_START_GAMEDAY_ID) || 1;
// IPL season start date — used to validate fetched data belongs to the current season
// Format: YYYY-MM-DD. IPL typically starts in late March.
const IPL_SEASON_START_DATE = process.env.IPL_SEASON_START_DATE || `${IPL_SEASON_YEAR}-03-01`;

// Store multiple auction rooms (in-memory cache)
let rooms = new Map();

// Load players data
const playersData = require('./data/players.json');
console.log(`Loaded ${playersData.length} players from players.json`);

// ============ PERSISTENCE LAYER ============
// Uses MongoDB if MONGODB_URI is set, otherwise falls back to file-based

let db = null; // MongoDB database reference

async function initPersistence() {
    if (MONGODB_URI) {
        console.log('MONGODB_URI is set, attempting to connect...');
        console.log('URI starts with:', MONGODB_URI.substring(0, 20) + '...');
        try {
            const { MongoClient } = require('mongodb');
            const client = new MongoClient(MONGODB_URI, {
                tls: true,
                tlsAllowInvalidCertificates: true,
                serverSelectionTimeoutMS: 10000,
                connectTimeoutMS: 10000
            });
            await client.connect();
            db = client.db('ipl_auction');
            console.log('Connected to MongoDB for persistent storage');

            // Load rooms from MongoDB into memory
            const docs = await db.collection('rooms').find({}).toArray();
            rooms = new Map(docs.map(d => {
                const { _id, ...room } = d; // strip _id
                return [room.code, room];
            }));
            console.log(`Loaded ${rooms.size} rooms from MongoDB`);

            // Clean up rooms: pinned rooms last 6 months, unpinned rooms last 7 days
            const weekAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
            const sixMonthsAgo = Date.now() - (180 * 24 * 60 * 60 * 1000);
            let cleaned = 0;
            for (const [code, room] of rooms) {
                const cutoff = room.pinned ? sixMonthsAgo : weekAgo;
                if (room.createdAt < cutoff) {
                    rooms.delete(code);
                    await db.collection('rooms').deleteOne({ code });
                    cleaned++;
                }
            }
            if (cleaned > 0) console.log(`Cleaned up ${cleaned} old rooms`);
        } catch (err) {
            console.error('MongoDB connection failed, falling back to file:', err.message);
            db = null;
            loadRoomsFromFile();
        }
    } else {
        console.log('No MONGODB_URI set, using file-based persistence (data will be lost on Render redeploy)');
        loadRoomsFromFile();
    }
}

// Debounce save to prevent too many writes
let saveTimeout = null;
function saveRoomsDebounced() {
    if (saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => saveRooms(), 500);
}

// Save a single room (or all rooms for file mode)
function saveRooms(roomCode) {
    if (db) {
        // MongoDB: save specific room or all
        if (roomCode) {
            const room = rooms.get(roomCode);
            if (room) {
                db.collection('rooms').updateOne(
                    { code: roomCode },
                    { $set: room },
                    { upsert: true }
                ).catch(err => console.error('MongoDB save error:', err.message));
            }
        } else {
            // Save all rooms (used rarely)
            for (const [code, room] of rooms) {
                db.collection('rooms').updateOne(
                    { code },
                    { $set: room },
                    { upsert: true }
                ).catch(err => console.error('MongoDB save error:', err.message));
            }
        }
    } else {
        // File-based fallback
        try {
            const roomsArray = Array.from(rooms.entries());
            fs.writeFileSync(ROOMS_FILE, JSON.stringify(roomsArray));
        } catch (err) {
            console.error('Error saving rooms to file:', err);
        }
    }
}

function deleteRoomFromDb(code) {
    if (db) {
        db.collection('rooms').deleteOne({ code }).catch(err => console.error('MongoDB delete error:', err.message));
    }
}

// File-based load (fallback)
function loadRoomsFromFile() {
    try {
        const dataDir = path.dirname(ROOMS_FILE);
        if (!fs.existsSync(dataDir)) {
            fs.mkdirSync(dataDir, { recursive: true });
        }
        if (!fs.existsSync(ROOMS_FILE)) {
            fs.writeFileSync(ROOMS_FILE, '[]');
            console.log('Created new rooms.json file');
        }
        const data = fs.readFileSync(ROOMS_FILE, 'utf8');
        const roomsArray = JSON.parse(data);
        rooms = new Map(roomsArray);
        console.log(`Loaded ${rooms.size} rooms from file`);

        // Clean up old rooms: pinned rooms last 6 months, unpinned last 7 days
        const weekAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
        const sixMonthsAgo = Date.now() - (180 * 24 * 60 * 60 * 1000);
        let cleaned = 0;
        for (const [code, room] of rooms) {
            const cutoff = room.pinned ? sixMonthsAgo : weekAgo;
            if (room.createdAt < cutoff) {
                rooms.delete(code);
                cleaned++;
            }
        }
        if (cleaned > 0) {
            console.log(`Cleaned up ${cleaned} old rooms`);
            saveRooms();
        }
    } catch (err) {
        console.error('Error loading rooms from file:', err);
        rooms = new Map();
    }
}

// ============ FANTASY POINTS LAYER ============
const Fuse = require('fuse.js');
let fantasyCache = null;
let fantasyFetchTimer = null;
let liveMatchTimer = null;
let isRefreshing = false;

function isFantasyConfigured() {
    // Public IPL stats API is always available — no credentials required.
    // Manual overrides (IPL Fantasy cookies or CricAPI) are optional.
    return true;
}

function getFantasySource() {
    const iplUid = process.env.IPL_FANTASY_UID || IPL_FANTASY_UID;
    const iplToken = process.env.IPL_FANTASY_AUTH_TOKEN || IPL_FANTASY_AUTH_TOKEN;
    if (iplUid && iplToken) return 'ipl_cookie';
    const cricKey = process.env.CRICAPI_KEY || CRICAPI_KEY;
    if (cricKey) return 'cricapi'; // series ID auto-discovered if not provided
    return 'ipl_public'; // Official public IPL Fantasy API — no credentials needed
}

async function initFantasyPoints() {
    if (db) {
        // Load best available cached data on startup
        // Priority: IPL Fantasy cookies > IPL public > ESPN > IPL public stats > CricAPI
        const year = new Date().getFullYear();
        const preferred = [
            'ipl_fantasy_official',
            'ipl_fantasy_public',
            'cricbuzz_ipl',
            `espn_ipl_${year}`,
            `espn_ipl_${year - 1}`,
            'ipl_public_stats',
            CRICAPI_SERIES_ID,
        ].filter(Boolean);
        for (const sid of preferred) {
            const doc = await db.collection('fantasy_points').findOne({ seriesId: sid });
            if (doc?.matches?.length) {
                fantasyCache = doc;
                console.log(`Loaded ${doc.matches.length} matches from cache (${sid})`);
                break;
            }
        }
    }

    // Clear stale cache from a previous IPL season
    if (fantasyCache && isStaleSeasonCache(fantasyCache)) {
        console.log(`Clearing stale IPL ${fantasyCache.seasonYear || 'unknown'} cache — current season is IPL ${IPL_SEASON_YEAR}`);
        if (db) {
            await db.collection('fantasy_points').deleteMany({
                $or: [
                    { seasonYear: { $lt: IPL_SEASON_YEAR } },
                    { seasonYear: { $exists: false } }
                ]
            });
        }
        fantasyCache = null;
    }

    // Always start — public IPL stats requires no credentials
    startFantasyRefreshTimer();
    fetchAllFantasyPoints().catch(err =>
        console.error('Initial fantasy fetch failed:', err.message)
    );
}

// Detect if cached fantasy data belongs to a previous IPL season
function isStaleSeasonCache(cache) {
    if (!cache) return false;
    // If cache has explicit seasonYear from a prior year, it's stale
    if (cache.seasonYear && cache.seasonYear < IPL_SEASON_YEAR) return true;
    // If no seasonYear stored, check match dates — if all matches are from before
    // the current season start, the cache is stale
    const seasonStart = new Date(IPL_SEASON_START_DATE);
    if (cache.matches?.length > 0) {
        const matchesBeforeSeason = cache.matches.filter(m => {
            const d = new Date(m.matchDate);
            return !isNaN(d.getTime()) && d < seasonStart;
        });
        // If most matches have dates before the season start, cache is stale
        const matchesWithDates = cache.matches.filter(m => !isNaN(new Date(m.matchDate).getTime()));
        if (matchesWithDates.length > 0 && matchesBeforeSeason.length > matchesWithDates.length * 0.5) return true;
    }
    // Check match count vs what's possible this season — even if seasonYear is set.
    // The IPL Fantasy API reuses tourgamedayId across seasons, so a prior run may have
    // probed IDs 1-74 and stored 70+ IPL 2025 matches incorrectly tagged as the current season.
    // IPL has at most 2 matches per day, so match count should not exceed daysSinceStart * 2.
    if (cache.matches?.length > 0) {
        const seasonStart2 = new Date(IPL_SEASON_START_DATE);
        const daysSinceStart = Math.max(0, Math.floor((Date.now() - seasonStart2.getTime()) / (24 * 60 * 60 * 1000)));
        const maxExpected = Math.max(4, daysSinceStart * 2);
        if (cache.matches.length > maxExpected) {
            console.log(`Stale cache detected: ${cache.matches.length} matches exceeds max expected ${maxExpected} for ${daysSinceStart} days into IPL ${IPL_SEASON_YEAR}`);
            return true;
        }
    }
    return false;
}

function startFantasyRefreshTimer() {
    if (fantasyFetchTimer) clearInterval(fantasyFetchTimer);
    fantasyFetchTimer = setInterval(() => {
        fetchAllFantasyPoints().catch(err =>
            console.error('Fantasy auto-refresh failed:', err.message)
        );
    }, FANTASY_REFRESH_INTERVAL * 60 * 1000);
    console.log(`Fantasy points auto-refresh every ${FANTASY_REFRESH_INTERVAL} minutes`);
}

function startLiveMatchTimer() {
    if (liveMatchTimer) return; // already running
    liveMatchTimer = setInterval(() => {
        fetchAllFantasyPoints().catch(err =>
            console.error('Live match refresh failed:', err.message)
        );
    }, 30 * 1000); // every 30 seconds during live matches for near real-time updates
    console.log('Live match detected: accelerated refresh every 30 seconds');
}

function stopLiveMatchTimer() {
    if (liveMatchTimer) {
        clearInterval(liveMatchTimer);
        liveMatchTimer = null;
        console.log('No live matches: stopped live refresh timer');
    }
}

async function fetchAllFantasyPoints() {
    if (isRefreshing) {
        console.log('Fantasy refresh already in progress, skipping...');
        return { totalMatches: fantasyCache?.matches?.length || 0, newMatches: 0 };
    }
    isRefreshing = true;
    try {
        const source = getFantasySource();
        if (source === 'ipl_cookie') return await fetchFromIPLFantasy();
        if (source === 'cricapi')    return await fetchFromCricAPI();
        if (source === 'ipl_public') {
            try {
                return await fetchFromIPLFantasyPublic();
            } catch (err) {
                console.warn(`IPL Fantasy Public failed: ${err.message} — trying fallback sources...`);
            }
        }
        // 'auto' or fallback after ipl_public: Cricbuzz → ESPN → IPL Stats
        const sources = [
            { name: 'Cricbuzz', fn: fetchFromCricbuzz },
            { name: 'ESPN Cricinfo', fn: fetchFromESPN },
            { name: 'IPL Stats', fn: fetchFromIPLStats },
        ];
        let lastErr;
        for (const src of sources) {
            try {
                return await src.fn();
            } catch (err) {
                lastErr = err;
                console.warn(`${src.name} failed: ${err.message} — trying next source...`);
            }
        }
        throw lastErr;
    } finally {
        isRefreshing = false;
    }
}

// ============================================================
// CRICBUZZ — direct source (same data CricAPI uses internally)
// No API key, no cookies required.
// ============================================================

async function fetchFromCricbuzz() {
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.105 Mobile Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-IN,en;q=0.9',
        'Referer': 'https://www.cricbuzz.com/',
        'Origin': 'https://www.cricbuzz.com',
        'x-requested-with': 'com.cricbuzz.android'
    };

    console.log('Fetching IPL matches from Cricbuzz...');

    // Helper: extract IPL matches + series ID from a typeMatches response
    function extractIPLMatches(data, matchMap, seriesIds) {
        for (const typeGroup of (data.typeMatches || [])) {
            for (const seriesGroup of (typeGroup.seriesMatches || [])) {
                const wrapper = seriesGroup.seriesAdWrapper || seriesGroup;
                const seriesName = (wrapper.seriesName || '').toLowerCase();
                if (!seriesName.includes('ipl') && !seriesName.includes('indian premier')) continue;
                const seriesId = wrapper.seriesId || wrapper.seriesid;
                if (seriesId) seriesIds.add(String(seriesId));
                const matches = wrapper.matches || [];
                for (const m of matches) {
                    const info = m.matchInfo || m;
                    const matchId = String(info.matchId || info.id || '');
                    if (matchId && !matchMap.has(matchId)) matchMap.set(matchId, m);
                }
            }
        }
    }

    // Step 1: fetch live + recent in parallel, collect all IPL matches
    const [liveRes, recentRes] = await Promise.allSettled([
        fetch('https://www.cricbuzz.com/api/cricket-match/live', { headers }),
        fetch('https://www.cricbuzz.com/api/cricket-match/recent', { headers }),
    ]);

    const matchMap = new Map(); // matchId -> raw match object (deduped)
    const seriesIds = new Set();

    for (const result of [liveRes, recentRes]) {
        if (result.status === 'fulfilled' && result.value.ok) {
            try {
                const data = await result.value.json();
                extractIPLMatches(data, matchMap, seriesIds);
            } catch (_) {}
        }
    }

    // Step 2: if we have a series ID, fetch ALL series matches (includes completed ones)
    if (seriesIds.size > 0) {
        for (const sid of seriesIds) {
            try {
                const schedRes = await fetch(`https://www.cricbuzz.com/api/series/${sid}/matches`, { headers });
                if (schedRes.ok) {
                    const schedData = await schedRes.json();
                    // Format: { matchDetails: [ { matchDetailsMap: { match: [ { matchInfo, matchScore } ] } } ] }
                    for (const detail of (schedData.matchDetails || [])) {
                        const matches = detail.matchDetailsMap?.match || detail.matches || [];
                        for (const m of matches) {
                            const info = m.matchInfo || m;
                            const matchId = String(info.matchId || info.id || '');
                            if (matchId && !matchMap.has(matchId)) matchMap.set(matchId, m);
                        }
                    }
                    console.log(`Cricbuzz series ${sid}: fetched schedule, now ${matchMap.size} IPL matches total`);
                }
            } catch (e) { console.warn(`Cricbuzz series schedule error for ${sid}:`, e.message); }
        }
    }

    console.log(`Cricbuzz: found ${matchMap.size} IPL matches (live+recent+schedule)`);
    if (matchMap.size === 0) throw new Error('Cricbuzz: no IPL matches found in any feed');

    const now = Date.now();
    const existingMatches = fantasyCache?.matches || [];
    // Skip matches we already have that are completed and > 24h old
    const oneDayAgo = now - 24 * 60 * 60 * 1000;
    const existingCompleted = new Set(
        existingMatches
            .filter(m => m.status === 'completed' && new Date(m.matchDate).getTime() < oneDayAgo)
            .map(m => m.matchId)
    );

    const newMatches = [];
    for (const [matchId, m] of matchMap) {
        if (existingCompleted.has(matchId)) continue;

        const info = m.matchInfo || m;
        const state = String(info.state || info.matchState || info.status || info.matchDesc || '').toLowerCase();

        // Skip only clearly upcoming/scheduled matches that haven't started
        const isUpcoming = state === 'upcoming' || state === 'scheduled' || state === 'preview' ||
                           state === 'dormant';
        // Also skip by date: if match starts more than 1 hour in the future, skip
        const startMs = info.startDate ? parseInt(info.startDate) : 0;
        const isFuture = startMs > 0 && (startMs - now) > 60 * 60 * 1000;
        if (isUpcoming || isFuture) continue;

        const isLive = state.includes('progress') || state === 'live' || state.includes('innings break') ||
                       state.includes('lunch') || state.includes('tea') || state.includes('rain') ||
                       state.includes('stumps') || state.includes('toss');

        try {
            await new Promise(r => setTimeout(r, 300));
            const scRes = await fetch(`https://www.cricbuzz.com/api/cricket-scorecard/${matchId}`, { headers });
            if (!scRes.ok) {
                console.warn(`Cricbuzz scorecard HTTP ${scRes.status} for match ${matchId}`);
                continue;
            }
            const scData = await scRes.json();
            // Only process if there's actual innings data
            if (!scData.scoreCard?.length && !scData.scorecard?.length) {
                console.warn(`Cricbuzz match ${matchId}: scorecard has no innings data yet`);
                continue;
            }
            const playerPoints = computeCricbuzzMatchPoints(scData);
            if (playerPoints.length === 0) continue;

            const t1 = info.team1?.teamSName || info.team1?.teamName || '';
            const t2 = info.team2?.teamSName || info.team2?.teamName || '';
            newMatches.push({
                matchId,
                matchName: info.matchDesc || (t1 && t2 ? `${t1} vs ${t2}` : `Match ${matchId}`),
                matchDate: startMs ? new Date(startMs).toISOString() : '',
                status: isLive ? 'live' : 'completed',
                playerPoints
            });
            console.log(`  Cricbuzz: ${playerPoints.length} players for ${newMatches.at(-1).matchName}`);
        } catch (e) { console.error(`Cricbuzz match ${matchId}:`, e.message); }
    }

    const allCached = [...existingMatches];
    for (const nm of newMatches) {
        const idx = allCached.findIndex(x => x.matchId === nm.matchId);
        if (idx >= 0) allCached[idx] = nm; else allCached.push(nm);
    }
    // Filter to only current season matches — exclude matches with missing dates
    // (they could be from a prior IPL season fetched via a shared series endpoint)
    const seasonStart = new Date(IPL_SEASON_START_DATE);
    const seasonFiltered = allCached.filter(m => {
        if (!m.matchDate) return false; // no date → can't verify season, exclude
        const d = new Date(m.matchDate);
        return !isNaN(d.getTime()) && d >= seasonStart;
    });
    seasonFiltered.sort((a, b) => new Date(a.matchDate) - new Date(b.matchDate));

    const nameMapping = buildNameMapping(seasonFiltered);
    fantasyCache = { seriesId: 'cricbuzz_ipl', seasonYear: IPL_SEASON_YEAR, lastFetchedAt: now, matches: seasonFiltered, nameMapping };
    if (db) {
        await db.collection('fantasy_points').updateOne(
            { seriesId: 'cricbuzz_ipl' }, { $set: fantasyCache }, { upsert: true }
        );
    }
    console.log(`Cricbuzz IPL ${IPL_SEASON_YEAR}: ${seasonFiltered.length} total matches, ${newMatches.length} updated`);
    broadcastFantasyUpdate();
    if (seasonFiltered.some(m => m.status === 'live')) startLiveMatchTimer(); else stopLiveMatchTimer();
    return { totalMatches: allCached.length, newMatches: newMatches.length };
}

function computeCricbuzzMatchPoints(scData) {
    // Cricbuzz scorecard format:
    // { scoreCard: [ { inningsId, batTeamDetails: { batsmenData: { bat_1: { batName, runs, balls, fours, sixes, outDesc } } },
    //                  bowlTeamDetails: { bowlersData: { bowl_1: { bowlName, overs, maidens, runs, wickets } } } } ] }
    const playerMap = new Map();
    const ensure = (name) => {
        if (!name) return null;
        const n = name.trim();
        if (!n) return null;
        if (!playerMap.has(n)) {
            playerMap.set(n, {
                name: n, playingXI: true, batting: null, bowling: null,
                fielding: { catches: 0, stumpings: 0, directRunOuts: 0, indirectRunOuts: 0 }
            });
        }
        return playerMap.get(n);
    };

    for (const inning of (scData.scoreCard || scData.scorecard || [])) {
        // Batting
        const batsmenData = inning.batTeamDetails?.batsmenData || {};
        for (const key of Object.keys(batsmenData)) {
            const b = batsmenData[key];
            const name = b.batName || b.name;
            if (!name) continue;
            const p = ensure(name);
            const outDesc = String(b.outDesc || b.dismissal || '').toLowerCase();
            const isOut = outDesc && !outDesc.includes('not out') && !outDesc.includes('batting') && outDesc !== '';
            p.batting = {
                runs:  parseInt(b.runs  ?? 0),
                balls: parseInt(b.balls ?? 0),
                fours: parseInt(b.fours ?? 0),
                sixes: parseInt(b.sixes ?? 0),
                dismissed: isOut
            };

            // Fielding from dismissal description
            if (outDesc.startsWith('caught') || outDesc.startsWith('c ')) {
                const m = outDesc.match(/(?:caught|c)\s+(.+?)\s+(?:b|bowled)\s+/i);
                if (m?.[1]) { const fp = ensure(m[1]); if (fp) fp.fielding.catches++; }
            } else if (outDesc.startsWith('stumped') || outDesc.startsWith('st ')) {
                const m = outDesc.match(/(?:stumped|st)\s+(.+?)\s+(?:b|bowled)\s+/i);
                if (m?.[1]) { const fp = ensure(m[1]); if (fp) fp.fielding.stumpings++; }
            } else if (outDesc.includes('run out')) {
                const m = outDesc.match(/\((.+?)\)/);
                if (m?.[1]) { const fp = ensure(m[1]); if (fp) fp.fielding.directRunOuts++; }
            } else if (outDesc.startsWith('b ') || outDesc === 'bowled') {
                const m = outDesc.match(/^b\s+(.+)$/i);
                if (m?.[1]) { const bp = playerMap.get(m[1].trim()); if (bp?.bowling) bp.bowling.lbwBowled = (bp.bowling.lbwBowled || 0) + 1; }
            } else if (outDesc.startsWith('lbw')) {
                const m = outDesc.match(/b\s+(.+)$/i);
                if (m?.[1]) { const bp = playerMap.get(m[1].trim()); if (bp?.bowling) bp.bowling.lbwBowled = (bp.bowling.lbwBowled || 0) + 1; }
            }
        }

        // Bowling
        const bowlersData = inning.bowlTeamDetails?.bowlersData || {};
        for (const key of Object.keys(bowlersData)) {
            const bw = bowlersData[key];
            const name = bw.bowlName || bw.name;
            if (!name) continue;
            const p = ensure(name);
            const [ov, partial] = String(bw.overs ?? '0').split('.').map(Number);
            p.bowling = {
                wickets: parseInt(bw.wickets ?? 0),
                lbwBowled: p.bowling?.lbwBowled || 0,
                maidens: parseInt(bw.maidens ?? bw.maiden ?? 0),
                balls: (ov || 0) * 6 + (isNaN(partial) ? 0 : partial),
                runs: parseInt(bw.runs ?? 0)
            };
        }
    }

    return [...playerMap.values()].map(s => ({
        cricApiName: s.name,
        points: calcIPLFantasyPoints(s.batting, s.bowling, s.fielding, s.playingXI)
    }));
}


function computeCricAPIMatchPoints(matchData) {
    const playerMap = new Map();
    const ensure = (name) => {
        if (!name) return null;
        const trimmed = name.trim();
        if (!trimmed) return null;
        if (!playerMap.has(trimmed)) {
            playerMap.set(trimmed, {
                name: trimmed, playingXI: true, batting: null, bowling: null,
                fielding: { catches: 0, stumpings: 0, directRunOuts: 0, indirectRunOuts: 0 }
            });
        }
        return playerMap.get(trimmed);
    };

    for (const inning of (matchData.scorecard || [])) {
        // Batting
        for (const b of (inning.batting || [])) {
            const name = b.batsman;
            if (!name) continue;
            const p = ensure(name);
            const dcode = String(b['dismissal-code'] || b.dismissal || '').toLowerCase();
            const isOut = !dcode.includes('not out') && dcode !== '' && dcode !== 'not-out';
            p.batting = {
                runs: parseInt(b.r ?? 0),
                balls: parseInt(b.b ?? 0),
                fours: parseInt(b['4s'] ?? 0),
                sixes: parseInt(b['6s'] ?? 0),
                dismissed: isOut
            };

            // Extract fielding credits from dismissal text
            const dtext = String(b.dismissal || '');
            if (dcode === 'caught' || dcode === 'c') {
                const m = dtext.match(/^c\s+(.+?)\s+b\s+/i);
                if (m?.[1]) { const fp = ensure(m[1]); if (fp) fp.fielding.catches++; }
            } else if (dcode === 'stumped' || dcode === 'st') {
                const m = dtext.match(/^st\s+(.+?)\s+b\s+/i);
                if (m?.[1]) { const fp = ensure(m[1]); if (fp) fp.fielding.stumpings++; }
            } else if (dcode === 'run out') {
                const m = dtext.match(/\((.+?)\)/);
                if (m?.[1]) { const fp = ensure(m[1]); if (fp) fp.fielding.directRunOuts++; }
            } else if (dcode === 'bowled' || dcode === 'lbw') {
                // Count LBW/Bowled bonus for bowler
                const m = dtext.match(/\bb\s+(.+)$/i);
                if (m?.[1]) {
                    const bp = playerMap.get(m[1].trim());
                    if (bp?.bowling) bp.bowling.lbwBowled = (bp.bowling.lbwBowled || 0) + 1;
                }
            }
        }

        // Bowling
        for (const bw of (inning.bowling || [])) {
            const name = bw.bowler;
            if (!name) continue;
            const p = ensure(name);
            const [ov, partial] = String(bw.o ?? '0').split('.').map(Number);
            p.bowling = {
                wickets: parseInt(bw.w ?? 0),
                lbwBowled: p.bowling?.lbwBowled || 0, // already set from batting above
                maidens: parseInt(bw.m ?? 0),
                balls: (ov || 0) * 6 + (isNaN(partial) ? 0 : partial),
                runs: parseInt(bw.r ?? 0)
            };
        }
    }

    return [...playerMap.values()].map(s => ({
        cricApiName: s.name,
        points: calcIPLFantasyPoints(s.batting, s.bowling, s.fielding, s.playingXI)
    }));
}

// ---- ESPN Cricinfo (public, no auth) ----
async function fetchFromESPN() {
    const year = new Date().getFullYear();
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.espncricinfo.com/',
        'Origin': 'https://www.espncricinfo.com'
    };

    console.log(`Searching for IPL ${year} series on ESPN Cricinfo...`);

    // Step 1: find current IPL series
    let seriesId = null;
    const searchUrls = [
        `https://hs-consumer-api.espncricinfo.com/v1/search?lang=en&query=Indian+Premier+League+${year}&type=series&size=10`,
        `https://hs-consumer-api.espncricinfo.com/v1/search?lang=en&query=IPL+${year}&type=series&size=10`,
    ];
    for (const url of searchUrls) {
        try {
            const res = await fetch(url, { headers });
            if (!res.ok) { console.log(`ESPN search HTTP ${res.status}: ${url}`); continue; }
            const data = await res.json();
            const results = data?.results || data?.searchResults || [];
            const found = results.find(r => {
                const h = (r?.headline || r?.title || r?.name || '').toLowerCase();
                return (h.includes('indian premier league') || h.includes(' ipl ') || h.startsWith('ipl ')) &&
                       h.includes(String(year));
            });
            if (found?.id) { seriesId = found.id; break; }
        } catch (e) { console.log(`ESPN search error: ${e.message}`); }
    }
    if (!seriesId) throw new Error(`ESPN: IPL ${year} series not found`);
    console.log(`ESPN IPL ${year} series ID: ${seriesId}`);

    // Step 2: get match schedule
    const schedRes = await fetch(
        `https://hs-consumer-api.espncricinfo.com/v1/pages/series/schedule?lang=en&seriesId=${seriesId}`,
        { headers }
    );
    if (!schedRes.ok) throw new Error(`ESPN schedule HTTP ${schedRes.status}`);
    const schedData = await schedRes.json();

    const matchList = schedData?.content?.matches ||
                      schedData?.content?.matchGroups?.flatMap(g => g.matches || []) ||
                      schedData?.matches || schedData?.fixtures || schedData?.data?.matches || [];

    const threeHoursAgo = Date.now() - 3 * 60 * 60 * 1000;
    const toFetch = matchList.filter(m => {
        const stage = String(m?.stage || m?.status || m?.matchStatus || m?.state || '').toLowerCase();
        const isCompleted = stage === 'finished' || stage === 'complete' || stage === 'completed' ||
                            stage === 'result'   || stage === 'post'     || stage === 'closed' ||
                            stage.includes('result') || stage.includes('finish') || stage.includes('complet') ||
                            m?.isComplete === true || m?.matchEnded === true;
        const isLive = stage === 'live' || stage.includes('progress') || m?.isLive === true;
        // Fallback: include any match whose scheduled start was >3 hours ago (likely completed/started)
        const startDate = m?.startDate || m?.date || m?.dateTimeGMT;
        const likelyPast = startDate && new Date(startDate).getTime() < threeHoursAgo;
        return isCompleted || isLive || likelyPast;
    });
    console.log(`ESPN: ${matchList.length} total, ${toFetch.length} completed/live/past matches`);

    const existingMatches = fantasyCache?.matches || [];
    const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;
    const existingCompleted = new Set(
        existingMatches
            .filter(m => m.status === 'completed' && new Date(m.matchDate) < oneDayAgo)
            .map(m => m.matchId)
    );

    const newMatches = [];
    for (const m of toFetch) {
        const matchId = String(m?.objectId || m?.id || m?.matchId || '');
        if (!matchId || existingCompleted.has(matchId)) continue;

        try {
            await new Promise(r => setTimeout(r, 300));
            const scRes = await fetch(
                `https://hs-consumer-api.espncricinfo.com/v1/pages/match/scorecard?lang=en&matchId=${matchId}`,
                { headers }
            );
            if (!scRes.ok) { console.warn(`ESPN scorecard HTTP ${scRes.status} for match ${matchId}`); continue; }
            const scData = await scRes.json();

            const playerPoints = computeESPNMatchPoints(scData);
            if (playerPoints.length === 0) continue;

            const team1 = m?.teams?.[0]?.team?.shortName || m?.team1 || '';
            const team2 = m?.teams?.[1]?.team?.shortName || m?.team2 || '';
            const stage = String(m?.stage || m?.status || '').toLowerCase();
            newMatches.push({
                matchId,
                matchName: m?.title || (team1 && team2 ? `${team1} vs ${team2}` : `Match ${matchId}`),
                matchDate: m?.startDate || m?.date || '',
                status: (stage.includes('progress') || stage === 'live') ? 'live' : 'completed',
                playerPoints
            });
            console.log(`  ESPN: ${playerPoints.length} players for ${newMatches.at(-1).matchName}`);
        } catch (e) { console.error(`ESPN match ${matchId}:`, e.message); }
    }

    const allMatches = [...existingMatches];
    for (const nm of newMatches) {
        const idx = allMatches.findIndex(m => m.matchId === nm.matchId);
        if (idx >= 0) allMatches[idx] = nm; else allMatches.push(nm);
    }
    // Filter to current season only — exclude matches without valid dates
    const seasonStart = new Date(IPL_SEASON_START_DATE);
    const seasonMatches = allMatches.filter(m => {
        if (!m.matchDate) return false;
        const d = new Date(m.matchDate);
        return !isNaN(d.getTime()) && d >= seasonStart;
    });
    seasonMatches.sort((a, b) => new Date(a.matchDate) - new Date(b.matchDate));

    const nameMapping = buildNameMapping(seasonMatches);
    fantasyCache = { seriesId: `espn_ipl_${year}`, seasonYear: IPL_SEASON_YEAR, lastFetchedAt: Date.now(), matches: seasonMatches, nameMapping };

    if (db) {
        await db.collection('fantasy_points').updateOne(
            { seriesId: fantasyCache.seriesId }, { $set: fantasyCache }, { upsert: true }
        );
    }
    console.log(`ESPN IPL ${IPL_SEASON_YEAR}: ${seasonMatches.length} total matches, ${newMatches.length} updated`);
    broadcastFantasyUpdate();
    if (seasonMatches.some(m => m.status === 'live')) startLiveMatchTimer(); else stopLiveMatchTimer();
    return { totalMatches: allMatches.length, newMatches: newMatches.length };
}

function computeESPNMatchPoints(scData) {
    const playerMap = new Map();
    const ensure = (name) => {
        if (!name) return null;
        if (!playerMap.has(name)) {
            playerMap.set(name, {
                name, playingXI: true, batting: null, bowling: null,
                fielding: { catches: 0, stumpings: 0, directRunOuts: 0, indirectRunOuts: 0 }
            });
        }
        return playerMap.get(name);
    };

    const scorecardInnings = scData?.scorecard || scData?.innings || [];
    for (const inningWrapper of scorecardInnings) {
        const inning = inningWrapper?.innings || inningWrapper;

        // Batting
        for (const b of (inning?.inningsBatsmen || [])) {
            const name = b?.player?.longName || b?.player?.name || b?.name;
            if (!name) continue;
            const p = ensure(name);
            const dismissal = String(b?.dismissalText?.long || b?.dismissalText || b?.howOut || '').toLowerCase();
            p.batting = {
                runs: parseInt(b?.runs ?? 0),
                balls: parseInt(b?.balls ?? 0),
                fours: parseInt(b?.fours ?? 0),
                sixes: parseInt(b?.sixes ?? 0),
                dismissed: b?.isOut === true || (dismissal.length > 0 && !dismissal.includes('not out') && !dismissal.includes('batting'))
            };
        }

        // Bowling
        for (const bw of (inning?.inningsBowlers || [])) {
            const name = bw?.player?.longName || bw?.player?.name || bw?.name;
            if (!name) continue;
            const p = ensure(name);
            const oversStr = String(bw?.overs ?? '0');
            const [ov, partial] = oversStr.split('.').map(Number);
            const balls = (ov || 0) * 6 + (isNaN(partial) ? 0 : partial);
            p.bowling = {
                wickets: parseInt(bw?.wickets ?? 0),
                lbwBowled: 0,
                maidens: parseInt(bw?.maidens ?? 0),
                balls,
                runs: parseInt(bw?.conceded ?? bw?.runs ?? 0)
            };
        }

        // Fielding / LBW+Bowled from wickets
        for (const w of (inning?.inningsWickets || [])) {
            const dtype = String(w?.wktType || w?.wicketType || w?.dismissalType || '').toLowerCase();
            const fielder = w?.fielder1?.player?.longName || w?.fielderNames || w?.fielder?.longName;
            const bowlerName = w?.bowler?.longName || w?.bowler?.player?.longName;
            const indirectFielder = w?.fielder2?.player?.longName;

            if ((dtype.includes('lbw') || dtype === 'bowled') && bowlerName) {
                const bp = playerMap.get(bowlerName);
                if (bp?.bowling) bp.bowling.lbwBowled = (bp.bowling.lbwBowled || 0) + 1;
            }
            if (fielder) {
                const fp = ensure(fielder);
                if (dtype.includes('caught'))   fp.fielding.catches++;
                else if (dtype.includes('stump')) fp.fielding.stumpings++;
            }
            if (dtype.includes('run out')) {
                if (fielder)         { const p = ensure(fielder);         p.fielding.directRunOuts++; }
                if (indirectFielder) { const p = ensure(indirectFielder); p.fielding.indirectRunOuts++; }
            }
        }
    }

    return [...playerMap.values()].map(s => ({
        cricApiName: s.name,
        points: calcIPLFantasyPoints(s.batting, s.bowling, s.fielding, s.playingXI)
    }));
}

async function fetchFromIPLFantasy() {
    const uid = process.env.IPL_FANTASY_UID || IPL_FANTASY_UID;
    const authToken = process.env.IPL_FANTASY_AUTH_TOKEN || IPL_FANTASY_AUTH_TOKEN;
    if (!uid || !authToken) throw new Error('IPL_FANTASY_UID and IPL_FANTASY_AUTH_TOKEN must be set');

    const headers = {
        'Cookie': `my11c-uid=${uid}; my11c-authToken=${authToken}`,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://fantasy.iplt20.com/classic/home',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://fantasy.iplt20.com',
        'x-access-token': authToken
    };

    console.log('Fetching IPL Fantasy fixtures...');
    const fixturesRes = await fetch('https://fantasy.iplt20.com/classic/api/feed/tour-fixtures', { headers });
    if (fixturesRes.status === 401 || fixturesRes.status === 403) {
        throw new Error('IPL Fantasy session expired — please refresh your cookies (my11c-uid, my11c-authToken)');
    }
    if (!fixturesRes.ok) {
        throw new Error(`IPL Fantasy fixtures fetch failed: HTTP ${fixturesRes.status}`);
    }
    const fixturesJson = await fixturesRes.json();
    const allFixtures = parseIPLFixtures(fixturesJson);
    // Filter fixtures to only current season — exclude fixtures without valid dates
    const seasonStart = new Date(IPL_SEASON_START_DATE);
    const fixtures = allFixtures.filter(f => {
        if (!f.matchDate) return false;
        const d = new Date(f.matchDate);
        return !isNaN(d.getTime()) && d >= seasonStart;
    });
    if (allFixtures.length > fixtures.length) {
        console.log(`IPL Fantasy: filtered ${allFixtures.length - fixtures.length} fixtures from before IPL ${IPL_SEASON_YEAR}`);
    }
    if (fixtures.length === 0) {
        // Log the top-level keys to help diagnose response format changes
        const topKeys = Object.keys(fixturesJson || {});
        const dataKeys = fixturesJson?.data ? Object.keys(fixturesJson.data) : [];
        console.log(`No live/completed fixtures found. Response top-keys: [${topKeys}], data keys: [${dataKeys}]`);
    }
    console.log(`Found ${fixtures.length} completed/live IPL ${IPL_SEASON_YEAR} fixtures`);

    // Discard cache from a different season
    const existingMatches = (fantasyCache?.seasonYear === IPL_SEASON_YEAR)
        ? (fantasyCache?.matches || [])
        : [];
    // Only skip completed matches that are older than 24 hours — recent ones
    // may still have points being updated on the IPL Fantasy server.
    const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;
    const existingCompleted = new Set(
        existingMatches.filter(m => {
            if (m.status !== 'completed') return false;
            const matchTime = new Date(m.matchDate).getTime();
            return !isNaN(matchTime) && matchTime < oneDayAgo;
        }).map(m => m.matchId)
    );

    const newMatches = [];
    for (const fixture of fixtures) {
        if (existingCompleted.has(fixture.matchId)) continue;
        try {
            await new Promise(resolve => setTimeout(resolve, 300));
            const url = `https://fantasy.iplt20.com/classic/api/feed/gamedayplayers?optType=1&gamedayId=${fixture.gamedayId}&phaseId=${fixture.phaseId}&pageNo=0&topNo=500&pageChunk=500&minCount=0`;
            const playersRes = await fetch(url, { headers });
            if (playersRes.status === 401 || playersRes.status === 403) {
                throw new Error('IPL Fantasy session expired — please refresh your cookies');
            }
            if (!playersRes.ok) {
                console.warn(`Skipping gameday ${fixture.gamedayId}: HTTP ${playersRes.status}`);
                continue;
            }
            const playersJson = await playersRes.json();
            const playerPoints = parseIPLPlayerPoints(playersJson);
            if (playerPoints.length > 0) {
                newMatches.push({
                    matchId: fixture.matchId,
                    matchName: fixture.matchName,
                    matchDate: fixture.matchDate,
                    status: fixture.status,
                    playerPoints: playerPoints.map(p => ({
                        cricApiId: String(p.playerId),
                        cricApiName: p.playerName,
                        points: p.points
                    }))
                });
                console.log(`  Fetched ${playerPoints.length} players for ${fixture.matchName}`);
            }
        } catch (err) {
            if (err.message.includes('session expired')) throw err;
            console.error(`Error fetching gameday ${fixture.gamedayId}:`, err.message);
        }
    }

    const allMatches = [...existingMatches];
    for (const nm of newMatches) {
        const idx = allMatches.findIndex(m => m.matchId === nm.matchId);
        if (idx >= 0) allMatches[idx] = nm;
        else allMatches.push(nm);
    }
    allMatches.sort((a, b) => new Date(a.matchDate) - new Date(b.matchDate));

    const nameMapping = buildNameMapping(allMatches);
    fantasyCache = {
        seriesId: 'ipl_fantasy_official',
        seasonYear: IPL_SEASON_YEAR,
        lastFetchedAt: Date.now(),
        matches: allMatches,
        nameMapping
    };

    if (db) {
        await db.collection('fantasy_points').updateOne(
            { seriesId: 'ipl_fantasy_official' },
            { $set: fantasyCache },
            { upsert: true }
        );
    }

    console.log(`IPL ${IPL_SEASON_YEAR} Fantasy updated: ${allMatches.length} total matches, ${newMatches.length} new/updated`);
    broadcastFantasyUpdate();
    // Accelerate polling if any live match is ongoing
    if (allMatches.some(m => m.status === 'live')) startLiveMatchTimer();
    else stopLiveMatchTimer();
    return { totalMatches: allMatches.length, newMatches: newMatches.length };
}

// ============================================================
// IPL FANTASY PUBLIC API — no credentials required
// Fetches from https://fantasy.iplt20.com/classic/api/feed/gamedayplayers
// with lang=en&tourgamedayId=<N>&teamgamedayId=1
// Points are updated daily/real-time by the official IPL Fantasy platform.
// ============================================================

async function fetchFromIPLFantasyPublic() {
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://fantasy.iplt20.com/',
        'Origin': 'https://fantasy.iplt20.com',
    };

    // Step 1: Try to discover fixtures from the public tour-fixtures endpoint
    let fixtures = [];
    try {
        const fixturesRes = await fetch('https://fantasy.iplt20.com/classic/api/feed/tour-fixtures?lang=en', { headers });
        if (fixturesRes.ok) {
            const fixturesJson = await fixturesRes.json();
            fixtures = parseIPLFixtures(fixturesJson);
            console.log(`IPL Fantasy Public: discovered ${fixtures.length} fixtures from tour-fixtures`);
        }
    } catch (e) {
        console.log('IPL Fantasy Public: tour-fixtures endpoint unavailable, will probe gameday IDs');
    }

    // Step 2: If no fixtures found, probe tourgamedayId sequentially (IPL has up to 74 matches)
    // We'll use discovered fixtures for matchId/matchName/matchDate metadata;
    // if none, we generate placeholder metadata and stop on consecutive no-points responses.
    const useProbing = fixtures.length === 0;
    const MAX_GAMEDAY_ID = 74;
    const startId = IPL_START_GAMEDAY_ID; // Configurable starting gameday ID for IPL season

    // Calculate max possible matches based on days since season start.
    // IPL has at most 2 matches per day. This prevents accepting stale data from a prior season.
    const seasonStart = new Date(IPL_SEASON_START_DATE);
    const daysSinceSeasonStart = Math.max(0, Math.floor((Date.now() - seasonStart.getTime()) / (24 * 60 * 60 * 1000)));
    const maxPossibleMatches = Math.min(MAX_GAMEDAY_ID, Math.max(2, daysSinceSeasonStart * 2));

    // Only carry forward matches previously fetched by THIS source to avoid inheriting
    // stale data from Cricbuzz/ESPN/IPL-Stats (which may contain matches from prior seasons).
    // Also discard cache from a different season or if match count is unreasonable.
    let existingMatches = [];
    if (fantasyCache?.seriesId === 'ipl_fantasy_public' && fantasyCache?.seasonYear === IPL_SEASON_YEAR) {
        const cached = fantasyCache?.matches || [];
        // Validate: if cached match count exceeds what's possible this season, discard everything.
        // The IPL Fantasy API reuses tourgamedayId across seasons, so stale IPL 2025 data
        // may have been probed and stored with seasonYear set to the current year.
        if (cached.length > maxPossibleMatches) {
            console.log(`IPL Fantasy Public: discarding ${cached.length} cached matches (exceeds max possible ${maxPossibleMatches} for IPL ${IPL_SEASON_YEAR})`);
        } else {
            existingMatches = cached;
        }
    } else if (fantasyCache?.seriesId === 'ipl_fantasy_public') {
        console.log(`IPL Fantasy Public: discarding stale IPL ${fantasyCache.seasonYear || 'unknown'} cache for IPL ${IPL_SEASON_YEAR}`);
    }
    const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;
    const existingCompleted = new Set(
        existingMatches
            .filter(m => m.status === 'completed' && !isNaN(new Date(m.matchDate).getTime()) && new Date(m.matchDate).getTime() < oneDayAgo)
            .map(m => m.matchId)
    );

    const newMatches = [];
    let consecutiveNoPoints = 0; // counts gamedays that returned players but all with 0 pts
    let hitUnplayedBoundary = false; // true once we see a gameday with players but 0 points (match not yet played)

    if (useProbing) {
        console.log(`IPL ${IPL_SEASON_YEAR} Fantasy Public: probing tourgamedayId ${startId} to ${startId + MAX_GAMEDAY_ID - 1} (max ${maxPossibleMatches} matches expected)`);
    }

    // When using fixtures from the tour-fixtures endpoint, filter to only current season
    const filteredFixtures = fixtures.filter(f => {
        if (!f.matchDate) return true; // keep fixtures without dates (can't filter)
        const matchDate = new Date(f.matchDate);
        return !isNaN(matchDate.getTime()) && matchDate >= seasonStart;
    });
    if (fixtures.length > 0 && filteredFixtures.length < fixtures.length) {
        console.log(`IPL Fantasy Public: filtered ${fixtures.length - filteredFixtures.length} fixtures from before season start (${IPL_SEASON_START_DATE})`);
    }

    const gamedayEntries = useProbing
        ? Array.from({ length: MAX_GAMEDAY_ID }, (_, i) => ({
              tourgamedayId: startId + i,
              matchId: String(startId + i),
              matchName: `Match ${i + 1}`,
              matchDate: '',
              status: 'completed',
          }))
        : filteredFixtures.map(f => ({
              tourgamedayId: parseInt(f.gamedayId, 10) || parseInt(f.matchId, 10),
              matchId: f.matchId,
              matchName: f.matchName,
              matchDate: f.matchDate,
              status: f.status,
          }));

    for (const entry of gamedayEntries) {
        if (!entry.tourgamedayId) continue;
        if (existingCompleted.has(entry.matchId)) continue;

        // Stop probing if we've already found more matches than possible this season
        if (useProbing && newMatches.length >= maxPossibleMatches) {
            console.log(`IPL Fantasy Public: reached max possible matches (${maxPossibleMatches}) for ${daysSinceSeasonStart} days into IPL ${IPL_SEASON_YEAR}, stopping probe`);
            break;
        }

        // If we already hit an unplayed match boundary, skip all remaining gameday IDs.
        // The same tourgamedayId can return stale data from a prior IPL season (e.g. IPL 2025)
        // because the API reuses gameday IDs across seasons. Once an unplayed match is found,
        // all subsequent IDs with points belong to the previous season, not the current one.
        if (useProbing && hitUnplayedBoundary) {
            console.log(`IPL Fantasy Public: skipping tourgamedayId ${entry.tourgamedayId} — already past unplayed match boundary`);
            break;
        }

        try {
            await new Promise(resolve => setTimeout(resolve, 300));
            const url = `https://fantasy.iplt20.com/classic/api/feed/gamedayplayers?lang=en&tourgamedayId=${entry.tourgamedayId}&teamgamedayId=1`;
            const res = await fetch(url, { headers });

            if (!res.ok) {
                consecutiveNoPoints++;
                if (useProbing && consecutiveNoPoints >= 3) {
                    console.log(`IPL Fantasy Public: stopping probe after ${consecutiveNoPoints} consecutive failures at tourgamedayId ${entry.tourgamedayId}`);
                    break;
                }
                continue;
            }

            const json = await res.json();
            const playerPoints = parseIPLPublicPlayerPoints(json);

            // Skip entirely if no players returned at all
            if (playerPoints.length === 0) {
                consecutiveNoPoints++;
                if (useProbing && consecutiveNoPoints >= 3) {
                    console.log(`IPL Fantasy Public: stopping probe after ${consecutiveNoPoints} consecutive empty responses at tourgamedayId ${entry.tourgamedayId}`);
                    break;
                }
                continue;
            }

            // Check if the match has actually been played (at least one player with >0 GamedayPoints)
            const hasPoints = playerPoints.some(p => p.points > 0);

            if (!hasPoints) {
                // Match has players but no points — it hasn't been played in the CURRENT season.
                // Stop probing immediately: IPL matches are sequential, so if match N is unplayed,
                // match N+1 can't have been played either. Any data returned for subsequent gameday
                // IDs would be stale from a prior IPL season (same IDs are reused across seasons).
                if (useProbing) {
                    hitUnplayedBoundary = true;
                    console.log(`IPL Fantasy Public: tourgamedayId ${entry.tourgamedayId} has players but 0 points (unplayed in IPL ${IPL_SEASON_YEAR}) — stopping probe to avoid prior season data`);
                    break;
                }
                consecutiveNoPoints++;
                continue;
            }

            consecutiveNoPoints = 0;

            // Determine live vs completed: live if flagged by fixtures, or by detecting
            // changing points vs cached data (indicates an in-progress match)
            let matchStatus = 'completed';
            if (entry.status === 'live') {
                matchStatus = 'live';
            } else {
                // Check if this match's points changed compared to previous cache — if so, it's live
                const prevMatch = existingMatches.find(m => m.matchId === entry.matchId);
                if (prevMatch && prevMatch.playerPoints) {
                    const prevTotal = prevMatch.playerPoints.reduce((s, p) => s + (p.points || 0), 0);
                    const currTotal = playerPoints.reduce((s, p) => s + (p.points || 0), 0);
                    if (currTotal !== prevTotal) {
                        matchStatus = 'live';
                    } else if (prevMatch.status === 'live') {
                        // Points same but was previously live — keep as live for a grace period
                        // (will be flipped to completed once points stop changing)
                        matchStatus = 'live';
                    }
                }
                // If this is the last match with points (most recent), and it was just discovered
                // (no previous cache entry), treat as potentially live
                if (!prevMatch && useProbing) {
                    // This is a newly discovered match in probing — could be live
                    // Mark as live; will be corrected to completed on next refresh if points don't change
                    matchStatus = 'live';
                }
            }

            newMatches.push({
                matchId: entry.matchId,
                matchName: entry.matchName,
                matchDate: entry.matchDate || new Date().toISOString(),
                status: matchStatus,
                playerPoints,
            });

            console.log(`  IPL Fantasy Public: ${playerPoints.length} players for ${entry.matchName} (tourgamedayId=${entry.tourgamedayId})`);
        } catch (err) {
            console.error(`IPL Fantasy Public: error fetching tourgamedayId ${entry.tourgamedayId}:`, err.message);
            consecutiveNoPoints++;
            if (useProbing && consecutiveNoPoints >= 3) break;
        }
    }

    if (newMatches.length === 0 && existingMatches.length === 0) {
        throw new Error('IPL Fantasy Public: no player data found for any gameday');
    }

    // Merge new matches with existing cached matches
    let allMatches = [...existingMatches];
    for (const nm of newMatches) {
        const idx = allMatches.findIndex(m => m.matchId === nm.matchId);
        if (idx >= 0) allMatches[idx] = nm;
        else allMatches.push(nm);
    }

    // Final sanity check: if total matches exceed what's possible this season,
    // discard ALL existing matches and keep only newly fetched ones.
    // This catches stale IPL 2025 data that was cached with the wrong seasonYear.
    if (allMatches.length > maxPossibleMatches) {
        console.log(`IPL Fantasy Public: total ${allMatches.length} matches exceeds max possible ${maxPossibleMatches} — discarding stale cache, keeping only ${newMatches.length} new matches`);
        allMatches = [...newMatches];
    }

    // Filter out matches from before the current season start date.
    // Keep matches without valid dates (probed matches have no date) — season boundary
    // is enforced by hitUnplayedBoundary + maxPossibleMatches checks during probing.
    const validMatches = allMatches.filter(m => {
        if (!m.matchDate) return true; // probed matches have no date, keep them
        const d = new Date(m.matchDate);
        return isNaN(d.getTime()) || d >= seasonStart;
    });
    if (validMatches.length < allMatches.length) {
        console.log(`IPL Fantasy Public: removed ${allMatches.length - validMatches.length} matches from before IPL ${IPL_SEASON_YEAR} season start`);
    }

    validMatches.sort((a, b) => new Date(a.matchDate) - new Date(b.matchDate));

    const nameMapping = buildNameMapping(validMatches);
    fantasyCache = {
        seriesId: 'ipl_fantasy_public',
        seasonYear: IPL_SEASON_YEAR,
        lastFetchedAt: Date.now(),
        matches: validMatches,
        nameMapping,
    };

    if (db) {
        await db.collection('fantasy_points').updateOne(
            { seriesId: 'ipl_fantasy_public' },
            { $set: fantasyCache },
            { upsert: true }
        );
    }

    console.log(`IPL ${IPL_SEASON_YEAR} Fantasy Public: ${validMatches.length} total matches, ${newMatches.length} updated`);
    broadcastFantasyUpdate();
    if (validMatches.some(m => m.status === 'live')) startLiveMatchTimer();
    else stopLiveMatchTimer();
    return { totalMatches: validMatches.length, newMatches: newMatches.length };
}

// Parse the public gamedayplayers response.
// Response shape (from official IPL Fantasy API):
//   { Data: { Value: { Players: [ { Id, Name, ShortName, TeamName, TeamShortName,
//       SkillName, OverallPoints, GamedayPoints, IsActive, Value, ... } ] } } }
// We use GamedayPoints for per-match fantasy points.
function parseIPLPublicPlayerPoints(json) {
    // Try multiple possible paths — API may capitalise keys differently
    const list =
        json?.Data?.Value?.Players ||
        json?.data?.Value?.Players ||
        json?.Data?.value?.Players ||
        json?.data?.value?.Players ||
        json?.Data?.Players ||
        json?.data?.Players ||
        json?.Players ||
        json?.players ||
        [];

    if (list.length === 0) {
        const topKeys = Object.keys(json || {});
        const dataKeys = json?.Data ? Object.keys(json.Data) : (json?.data ? Object.keys(json.data) : []);
        if (topKeys.length > 0) {
            console.log(`  IPL Fantasy Public: no player list found. Top-keys: [${topKeys}], Data keys: [${dataKeys}]`);
        }
    }

    const players = [];
    for (const p of list) {
        const name = p?.Name || p?.name || p?.PlayerName || p?.playerName || p?.DisplayName || p?.FullName || '';
        if (!name) continue;

        // Prefer per-match GamedayPoints; fall back to OverallPoints then TotalPoints
        const pts = parseFloat(
            p?.GamedayPoints ?? p?.gamedayPoints ??
            p?.OverallPoints ?? p?.overallPoints ??
            p?.TotalPoints  ?? p?.totalPoints   ??
            p?.Points       ?? p?.points        ?? 0
        );
        const id = String(p?.Id || p?.id || p?.PlayerId || p?.playerId || '');

        players.push({ cricApiName: name, cricApiId: id, points: pts });
    }
    return players;
}

function parseIPLFixtures(json) {
    const fixtures = [];

    // Collect all fixture objects regardless of nesting structure
    let allFixtureObjects = [];

    // 1. Stage-based structure (classic IPL Fantasy API: data.Stages[].Fixtures[])
    const stages = json?.data?.Stages || json?.data?.stages || json?.Stages || json?.stages ||
                   json?.data?.stage || json?.stage || [];
    const stageArray = Array.isArray(stages) ? stages : (stages ? [stages] : []);
    for (const stage of stageArray) {
        const sf = stage?.Fixtures || stage?.fixtures || stage?.matches || stage?.Match || stage?.match || [];
        const fa = Array.isArray(sf) ? sf : [sf];
        allFixtureObjects.push(...fa);
    }

    // 2. Flat list structures — tried when stage-based yields nothing
    if (allFixtureObjects.length === 0) {
        const flat = json?.data?.fixtures || json?.data?.matches || json?.data?.matchList ||
                     json?.data?.Matches || json?.data?.MatchList || json?.data?.Fixtures ||
                     json?.fixtures || json?.matches || json?.matchList || json?.Matches || [];
        if (Array.isArray(flat)) allFixtureObjects.push(...flat);
        // data itself may be a plain array
        if (allFixtureObjects.length === 0 && Array.isArray(json?.data)) {
            allFixtureObjects.push(...json.data);
        }
    }

    if (allFixtureObjects.length === 0) {
        console.log('parseIPLFixtures: no fixture array found. Response keys:', Object.keys(json || {}),
            '| data keys:', Object.keys(json?.data || {}));
    }

    for (const f of allFixtureObjects) {
        if (!f || typeof f !== 'object') continue;

        const statusId = f?.StatusId ?? f?.statusId ?? f?.MatchStatus ?? f?.matchStatus ?? f?.status;
        const statusStr = statusId !== undefined && statusId !== null ? String(statusId).toLowerCase() : '';

        // Status IDs: 2=live, 3=completed in the classic API.
        // Some versions use 1=completed or 4=live; also accept boolean/string flags.
        const isLive = statusId === 2 || statusId === 4 || statusStr === '2' || statusStr === '4' ||
                       !!(f?.IsLive || f?.isLive || f?.matchStarted || f?.MatchStarted || f?.IsStarted || f?.isStarted) ||
                       statusStr === 'live' || statusStr === 'inprogress' || statusStr === 'started';
        const isCompleted = statusId === 1 || statusId === 3 || statusStr === '1' || statusStr === '3' ||
                            !!(f?.IsCompleted || f?.isCompleted || f?.MatchEnded || f?.matchEnded) ||
                            statusStr === 'completed' || statusStr === 'result' ||
                            statusStr === 'finished' || statusStr === 'post';

        // A "live" flag takes priority over "completed" classification
        if (!isLive && !isCompleted) continue;

        const gamedayId = f?.GamedayId || f?.gamedayId || f?.GameDayId || f?.gameDayId ||
                          f?.MatchId || f?.matchId || f?.FixtureId || f?.fixtureId;
        if (!gamedayId) continue;

        const home = f?.HomeTeam?.ShortName || f?.HomeTeam?.Name || f?.HomeTeam || f?.Team1 || f?.team1 || '';
        const away = f?.AwayTeam?.ShortName || f?.AwayTeam?.Name || f?.AwayTeam || f?.Team2 || f?.team2 || '';
        const matchName = f?.MatchName || f?.matchName || (home && away ? `${home} vs ${away}` : `Match ${gamedayId}`);

        fixtures.push({
            matchId: String(f?.FixtureId || f?.fixtureId || f?.MatchId || f?.matchId || gamedayId),
            matchName,
            matchDate: f?.StartDate || f?.startDate || f?.MatchDate || f?.matchDate || '',
            status: isLive ? 'live' : 'completed',
            gamedayId: String(gamedayId),
            phaseId: String(f?.PhaseId || f?.phaseId || 1)
        });
    }
    return fixtures;
}

function parseIPLPlayerPoints(json) {
    const players = [];
    const list = json?.data?.Players || json?.data?.players ||
                 json?.data?.PlayerList || json?.data?.playerList ||
                 json?.data?.playerpoints || json?.data?.PlayerPoints ||
                 json?.Players || json?.players ||
                 json?.playerList || json?.PlayerList || [];
    if (list.length === 0) {
        const dataKeys = json?.data ? Object.keys(json.data) : [];
        if (dataKeys.length > 0) console.log(`  Player points: no list found, data keys: [${dataKeys}]`);
    }
    for (const p of list) {
        const name = p?.PlayerName || p?.playerName || p?.Name || p?.name || p?.DisplayName || p?.displayName || p?.fullName || p?.FullName;
        const pts = parseFloat(p?.TotalPoints || p?.totalPoints || p?.Points || p?.points || p?.FantasyPoints || p?.fantasyPoints || 0);
        const id = String(p?.PlayerId || p?.playerId || p?.Id || p?.id || '');
        if (name) players.push({ playerId: id, playerName: name, points: pts });
    }
    return players;
}

async function findCricAPISeriesId(apiKey) {
    const year = new Date().getFullYear();
    const res = await fetch(`https://api.cricapi.com/v1/series?apikey=${apiKey}&search=Indian+Premier+League`);
    const data = await res.json();
    if (data.status !== 'success') throw new Error(`CricAPI series search error: ${JSON.stringify(data.info)}`);
    // Find the most recent IPL series matching current or last year
    const candidates = (data.data || [])
        .filter(s => {
            const n = (s.name || '').toLowerCase();
            return n.includes('indian premier league') || n.includes(' ipl');
        })
        .sort((a, b) => new Date(b.startDate || 0) - new Date(a.startDate || 0));
    const match = candidates.find(s => (s.season || s.name || '').includes(String(year))) || candidates[0];
    if (!match?.id) throw new Error('CricAPI: could not find IPL series');
    console.log(`CricAPI auto-discovered IPL series: "${match.name}" (${match.id})`);
    return match.id;
}

async function fetchFromCricAPI() {
    const apiKey = process.env.CRICAPI_KEY || CRICAPI_KEY;
    if (!apiKey) throw new Error('CRICAPI_KEY must be set');

    let seriesId = process.env.CRICAPI_SERIES_ID || CRICAPI_SERIES_ID;
    if (!seriesId) {
        // Auto-discover current IPL series — user only needs to provide the API key
        seriesId = await findCricAPISeriesId(apiKey);
        process.env.CRICAPI_SERIES_ID = seriesId; // cache for this session
    }

    console.log(`Fetching fantasy points from CricAPI (series: ${seriesId})...`);

    // Step 1: Get all match IDs for the series
    const seriesRes = await fetch(
        `https://api.cricapi.com/v1/series_info?apikey=${apiKey}&id=${seriesId}`
    );
    const seriesData = await seriesRes.json();
    if (seriesData.status !== 'success') {
        throw new Error(`CricAPI series_info error: ${JSON.stringify(seriesData.info || 'unknown')}`);
    }

    // CricAPI uses 'matchList' or 'matches' depending on version
    const matchList = seriesData.data?.matchList || seriesData.data?.matches || [];
    console.log(`Found ${matchList.length} matches in series`);

    // Step 2: For each completed/live match, fetch scorecard and compute fantasy points
    // NOTE: match_points requires a paid CricAPI plan; match_scorecard is free tier
    const existingMatches = (fantasyCache?.matches || []);
    const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;
    const existingCompleted = new Set(
        existingMatches.filter(m => {
            if (m.status !== 'completed') return false;
            const matchTime = new Date(m.matchDate).getTime();
            return !isNaN(matchTime) && matchTime < oneDayAgo;
        }).map(m => m.matchId)
    );

    const newMatches = [];
    let apiCalls = 0;

    for (const match of matchList) {
        if (existingCompleted.has(match.id)) continue;
        // Skip clearly future matches (neither started nor ended)
        const statusStr = String(match.status || '').toLowerCase();
        const isStarted = match.matchStarted || match.matchEnded ||
                          statusStr.includes('won') || statusStr.includes('lost') ||
                          statusStr.includes('draw') || statusStr.includes('tie') ||
                          statusStr.includes('live') || statusStr.includes('progress');
        if (!isStarted) continue;
        if (apiCalls >= 40) { console.log('Rate limit reached, resuming next cycle'); break; }

        try {
            await new Promise(r => setTimeout(r, 250));
            const scRes = await fetch(
                `https://api.cricapi.com/v1/match_scorecard?apikey=${apiKey}&id=${match.id}`
            );
            const scData = await scRes.json();
            apiCalls++;

            if (scData.status === 'success' && scData.data?.scorecard?.length) {
                const playerPoints = computeCricAPIMatchPoints(scData.data);
                if (playerPoints.length > 0) {
                    newMatches.push({
                        matchId: match.id,
                        matchName: match.name || scData.data.name || 'Match',
                        matchDate: match.date || match.dateTimeGMT || scData.data.date || '',
                        status: (match.matchEnded || statusStr.includes('won') || statusStr.includes('lost')) ? 'completed' : 'live',
                        playerPoints
                    });
                    console.log(`  CricAPI: ${playerPoints.length} players for ${newMatches.at(-1).matchName}`);
                }
            }
        } catch (err) {
            console.error(`Failed scorecard for match ${match.id}:`, err.message);
        }
    }

    // Step 3: Merge
    const allMatches = [...existingMatches];
    for (const nm of newMatches) {
        const idx = allMatches.findIndex(m => m.matchId === nm.matchId);
        if (idx >= 0) allMatches[idx] = nm;
        else allMatches.push(nm);
    }
    // Filter to current season only — exclude matches without valid dates
    const seasonStart = new Date(IPL_SEASON_START_DATE);
    const seasonMatches = allMatches.filter(m => {
        if (!m.matchDate) return false;
        const d = new Date(m.matchDate);
        return !isNaN(d.getTime()) && d >= seasonStart;
    });
    seasonMatches.sort((a, b) => new Date(a.matchDate) - new Date(b.matchDate));

    const nameMapping = buildNameMapping(seasonMatches);

    fantasyCache = {
        seriesId: seriesId,
        seasonYear: IPL_SEASON_YEAR,
        lastFetchedAt: Date.now(),
        matches: seasonMatches,
        nameMapping
    };

    if (db) {
        await db.collection('fantasy_points').updateOne(
            { seriesId: seriesId },
            { $set: fantasyCache },
            { upsert: true }
        );
    }

    console.log(`Fantasy points updated: ${allMatches.length} total matches, ${newMatches.length} new/updated`);
    broadcastFantasyUpdate();
    // Accelerate polling if any live match is ongoing
    if (allMatches.some(m => m.status === 'live')) startLiveMatchTimer();
    else stopLiveMatchTimer();
    return { totalMatches: allMatches.length, newMatches: newMatches.length };
}

// ============ IPL PUBLIC STATS — no authentication required ============

// Official IPL Fantasy / my11Circle T20 scoring rules
function calcIPLFantasyPoints(batting, bowling, fielding, playingXI) {
    let pts = playingXI ? 4 : 0;

    if (batting) {
        const { runs = 0, balls = 0, fours = 0, sixes = 0, dismissed = false } = batting;
        pts += runs;           // +1 per run
        pts += fours;          // +1 per boundary
        pts += sixes * 2;      // +2 per six
        if (runs >= 100) pts += 16;
        else if (runs >= 50) pts += 8;
        else if (runs >= 30) pts += 4;
        if (dismissed && runs === 0) pts -= 2; // duck
        if (balls >= 10) {
            const sr = (runs / balls) * 100;
            if (sr >= 170) pts += 6;
            else if (sr >= 150) pts += 4;
            else if (sr >= 130) pts += 2;
            else if (sr < 50) pts -= 6;
            else if (sr < 60) pts -= 4;
            else if (sr < 70) pts -= 2;
        }
    }

    if (bowling) {
        const { wickets = 0, lbwBowled = 0, maidens = 0, balls = 0, runs = 0 } = bowling;
        pts += wickets * 25;
        pts += lbwBowled * 8;
        pts += maidens * 4;
        if (wickets >= 5) pts += 16;
        else if (wickets >= 4) pts += 8;
        else if (wickets >= 3) pts += 4;
        if (balls >= 12) { // min 2 overs
            const er = runs / (balls / 6);
            if (er < 5) pts += 6;
            else if (er < 6) pts += 4;
            else if (er < 7) pts += 2;
            else if (er >= 12) pts -= 6;
            else if (er >= 11) pts -= 4;
            else if (er >= 10) pts -= 2;
        }
    }

    if (fielding) {
        const { catches = 0, stumpings = 0, directRunOuts = 0, indirectRunOuts = 0 } = fielding;
        pts += catches * 8;
        pts += stumpings * 12;
        pts += directRunOuts * 12;
        pts += indirectRunOuts * 6;
    }

    return Math.round(pts * 10) / 10;
}

function parseIPLStatsSchedule(json) {
    const fixtures = [];
    const matchList = json?.Matchsummary || json?.matchsummary ||
                      json?.MatchSchedule || json?.matches ||
                      json?.data?.matches || json?.data || [];
    for (const m of (Array.isArray(matchList) ? matchList : [])) {
        if (!m) continue;
        const status = String(m?.MatchStatus || m?.matchStatus || m?.status || '').toLowerCase();
        const isLive = status.includes('progress') || status === 'live';
        const isCompleted = status === 'result' || status === 'completed' || status.includes('result');
        if (!isLive && !isCompleted) continue;

        const matchCode = m?.MatchCode || m?.matchCode || m?.MatchID || m?.id;
        if (!matchCode) continue;

        const team1 = m?.Team1ShortName || m?.TeamAShortName || m?.team1 || '';
        const team2 = m?.Team2ShortName || m?.TeamBShortName || m?.team2 || '';
        const matchName = m?.MatchName || m?.matchName ||
            (team1 && team2 ? `${team1} vs ${team2}` : `Match ${matchCode}`);

        fixtures.push({
            matchId: String(matchCode),
            matchCode: String(matchCode),
            matchName,
            matchDate: m?.MatchDate || m?.matchDate || m?.date || '',
            status: isLive ? 'live' : 'completed'
        });
    }
    return fixtures;
}

async function fetchIPLScorecard(matchCode, headers) {
    const urls = [
        `https://ipl-stats.iplt20.com/ipl/json/${matchCode}/GetMatchScorecard.json`,
        `https://ipl-stats.iplt20.com/ipl/json/MatchScorecard.json?MatchCode=${matchCode}`,
        `https://ipl-stats.iplt20.com/ipl/json/${matchCode}/Scorecard.json`,
    ];
    for (const url of urls) {
        try {
            const res = await fetch(url, { headers });
            if (res.ok) return await res.json();
        } catch (_) { /* try next */ }
    }
    return null;
}

function computeMatchFantasyPoints(scorecardJson) {
    const playerMap = new Map();
    const ensure = (name) => {
        if (!name) return null;
        if (!playerMap.has(name)) {
            playerMap.set(name, {
                name, playingXI: true,
                batting: null, bowling: null,
                fielding: { catches: 0, stumpings: 0, directRunOuts: 0, indirectRunOuts: 0 }
            });
        }
        return playerMap.get(name);
    };

    const innings = scorecardJson?.Innings || scorecardJson?.innings ||
                    scorecardJson?.ScoreCard || scorecardJson?.scorecard || [];

    for (const inning of (Array.isArray(innings) ? innings : [])) {
        // Batting
        for (const b of (inning?.InningBatsmen || inning?.batsmen || [])) {
            const name = b?.StrikerName || b?.BatsmanName || b?.name;
            if (!name) continue;
            const p = ensure(name);
            p.batting = {
                runs: parseInt(b?.Runs ?? b?.runs ?? 0),
                balls: parseInt(b?.Balls ?? b?.balls ?? 0),
                fours: parseInt(b?.Fours ?? b?.fours ?? 0),
                sixes: parseInt(b?.Sixes ?? b?.sixes ?? 0),
                dismissed: !!(b?.IsOut || b?.isOut || b?.Out ||
                    (b?.StrikerDismissal && !String(b.StrikerDismissal).toLowerCase().includes('not out')))
            };
        }

        // Bowling
        for (const bw of (inning?.InningBowlers || inning?.bowlers || [])) {
            const name = bw?.BowlerName || bw?.name;
            if (!name) continue;
            const p = ensure(name);
            const oversStr = String(bw?.Overs ?? bw?.overs ?? '0');
            const [ov, partial] = oversStr.split('.').map(Number);
            const totalBalls = (ov || 0) * 6 + (isNaN(partial) ? 0 : partial);
            p.bowling = {
                wickets: parseInt(bw?.Wickets ?? bw?.wickets ?? 0),
                lbwBowled: 0, // populated from wickets list below
                maidens: parseInt(bw?.Maidens ?? bw?.maidens ?? 0),
                balls: totalBalls,
                runs: parseInt(bw?.Runs ?? bw?.runs ?? 0)
            };
        }

        // Fielding + LBW/Bowled from dismissal records
        for (const w of (inning?.InningWickets || inning?.wickets || inning?.dismissals || [])) {
            const dtype = String(w?.DismissalType || w?.dismissalType ||
                                 w?.Wicket || w?.wicket || '').toLowerCase();
            const fielder = w?.FielderName || w?.fielder;
            const bowler  = w?.BowlerName  || w?.bowler;

            if ((dtype.includes('lbw') || dtype === 'bowled') && bowler) {
                const bp = playerMap.get(bowler);
                if (bp?.bowling) bp.bowling.lbwBowled = (bp.bowling.lbwBowled || 0) + 1;
            }
            if (fielder) {
                const fp = ensure(fielder);
                if (dtype.includes('caught'))   fp.fielding.catches++;
                else if (dtype.includes('stump')) fp.fielding.stumpings++;
            }
            if (dtype.includes('run out')) {
                const direct   = w?.FielderName || w?.DirectFielder;
                const indirect = w?.IndirectFielder;
                if (direct)   { const p = ensure(direct);   p.fielding.directRunOuts++; }
                if (indirect) { const p = ensure(indirect); p.fielding.indirectRunOuts++; }
            }
        }
    }

    return [...playerMap.values()].map(s => ({
        cricApiName: s.name,
        points: calcIPLFantasyPoints(s.batting, s.bowling, s.fielding, s.playingXI)
    }));
}

async function fetchFromIPLStats() {
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.iplt20.com/',
        'Origin': 'https://www.iplt20.com',
        'Cache-Control': 'no-cache'
    };

    console.log('Fetching IPL stats from public API (no auth required)...');

    const scheduleUrls = [
        'https://ipl-stats.iplt20.com/ipl/json/MatchSchedule.json',
        'https://ipl-stats.iplt20.com/ipl/json/Fixtures.json',
        'https://ipl-stats.iplt20.com/ipl/json/matchschedule/MatchSchedule.json',
    ];

    let scheduleData = null;
    for (const url of scheduleUrls) {
        try {
            const res = await fetch(url, { headers });
            if (res.ok) { scheduleData = await res.json(); console.log(`IPL schedule from: ${url}`); break; }
            else console.log(`Schedule URL HTTP ${res.status}: ${url}`);
        } catch (err) { console.log(`Schedule URL error (${url}): ${err.message}`); }
    }
    if (!scheduleData) throw new Error('IPL public stats API unreachable — all schedule URLs failed (HTTP 403 / network error)');

    const fixtures = parseIPLStatsSchedule(scheduleData);
    console.log(`Found ${fixtures.length} completed/live fixtures from public IPL stats`);

    const existingMatches = fantasyCache?.matches || [];
    const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;
    const existingCompleted = new Set(
        existingMatches
            .filter(m => m.status === 'completed' && !isNaN(new Date(m.matchDate)) && new Date(m.matchDate) < oneDayAgo)
            .map(m => m.matchId)
    );

    const newMatches = [];
    for (const fixture of fixtures) {
        if (existingCompleted.has(fixture.matchId)) continue;
        try {
            await new Promise(r => setTimeout(r, 200));
            const scorecard = await fetchIPLScorecard(fixture.matchCode, headers);
            if (!scorecard) { console.warn(`  No scorecard for ${fixture.matchName}`); continue; }
            const playerPoints = computeMatchFantasyPoints(scorecard);
            if (playerPoints.length > 0) {
                newMatches.push({ matchId: fixture.matchId, matchName: fixture.matchName,
                    matchDate: fixture.matchDate, status: fixture.status, playerPoints });
                console.log(`  ${playerPoints.length} players computed for ${fixture.matchName}`);
            }
        } catch (err) { console.error(`Error for ${fixture.matchName}:`, err.message); }
    }

    const allMatches = [...existingMatches];
    for (const nm of newMatches) {
        const idx = allMatches.findIndex(m => m.matchId === nm.matchId);
        if (idx >= 0) allMatches[idx] = nm; else allMatches.push(nm);
    }
    // Filter to current season only — exclude matches without valid dates
    const seasonStart = new Date(IPL_SEASON_START_DATE);
    const seasonMatches = allMatches.filter(m => {
        if (!m.matchDate) return false;
        const d = new Date(m.matchDate);
        return !isNaN(d.getTime()) && d >= seasonStart;
    });
    seasonMatches.sort((a, b) => new Date(a.matchDate) - new Date(b.matchDate));

    const nameMapping = buildNameMapping(seasonMatches);
    fantasyCache = { seriesId: 'ipl_public_stats', seasonYear: IPL_SEASON_YEAR, lastFetchedAt: Date.now(), matches: seasonMatches, nameMapping };

    if (db) {
        await db.collection('fantasy_points').updateOne(
            { seriesId: 'ipl_public_stats' }, { $set: fantasyCache }, { upsert: true }
        );
    }

    console.log(`IPL ${IPL_SEASON_YEAR} public stats: ${seasonMatches.length} total matches, ${newMatches.length} updated`);
    broadcastFantasyUpdate();
    if (seasonMatches.some(m => m.status === 'live')) startLiveMatchTimer(); else stopLiveMatchTimer();
    return { totalMatches: allMatches.length, newMatches: newMatches.length };
}

function buildNameMapping(matches) {
    const cricApiNames = new Set();
    for (const match of matches) {
        for (const pp of match.playerPoints) {
            cricApiNames.add(pp.cricApiName);
        }
    }

    const fuse = new Fuse(playersData, {
        keys: ['name'],
        threshold: 0.3,
        distance: 100,
        includeScore: true,
    });

    const mapping = {};
    for (const apiName of cricApiNames) {
        // Exact match
        const exact = playersData.find(p => p.name.toLowerCase() === apiName.toLowerCase());
        if (exact) { mapping[apiName] = exact.name; continue; }

        // Last-name match
        const apiLastName = apiName.split(' ').pop().toLowerCase();
        const lastNameMatches = playersData.filter(p => p.name.toLowerCase().endsWith(apiLastName));
        if (lastNameMatches.length === 1) { mapping[apiName] = lastNameMatches[0].name; continue; }

        // Fuzzy match
        const results = fuse.search(apiName);
        if (results.length > 0 && results[0].score < 0.35) {
            mapping[apiName] = results[0].item.name;
        }
    }

    return mapping;
}

function computeRoomFantasyPoints(room) {
    const matches = fantasyCache?.matches || [];
    const nameMapping = fantasyCache?.nameMapping || {};
    // Reverse mapping: our player name -> array of CricAPI names
    const reverseMapping = {};
    for (const [apiName, ourName] of Object.entries(nameMapping)) {
        if (!reverseMapping[ourName]) reverseMapping[ourName] = [];
        reverseMapping[ourName].push(apiName);
    }

    const teamPoints = (room.teams || []).map(team => {
        const playerBreakdowns = (team.players || []).map(player => {
            const apiNames = reverseMapping[player.name] || [];
            const matchPoints = matches.map(match => {
                let pts = 0;
                for (const apiName of apiNames) {
                    const found = match.playerPoints.find(pp => pp.cricApiName === apiName);
                    if (found) { pts = found.points; break; }
                }
                return { matchId: match.matchId, matchName: match.matchName, points: pts };
            });
            const totalPoints = matchPoints.reduce((sum, mp) => sum + mp.points, 0);
            return {
                playerName: player.name,
                role: player.role,
                country: player.country,
                soldPrice: player.soldPrice,
                totalPoints,
                matchPoints
            };
        });

        const sortedByPoints = [...playerBreakdowns].sort((a, b) => b.totalPoints - a.totalPoints);
        const best11Names = new Set(sortedByPoints.slice(0, 11).map(pb => pb.playerName));
        const markedBreakdowns = playerBreakdowns.map(pb => ({
            ...pb,
            isInBest11: best11Names.has(pb.playerName)
        }));
        const squadTotalPoints = markedBreakdowns.reduce((sum, pb) => sum + pb.totalPoints, 0);
        const teamTotal = markedBreakdowns
            .filter(pb => pb.isInBest11)
            .reduce((sum, pb) => sum + pb.totalPoints, 0);

        return {
            teamId: team.id,
            teamName: team.name,
            ownerName: team.ownerName || '',
            totalPoints: teamTotal,
            squadTotalPoints,
            playerBreakdowns: markedBreakdowns,
            countedPlayers: Math.min(11, playerBreakdowns.length)
        };
    });

    teamPoints.sort((a, b) => b.totalPoints - a.totalPoints || a.teamName.localeCompare(b.teamName));

    return {
        teams: teamPoints,
        matches: matches.map(m => ({
            matchId: m.matchId,
            matchName: m.matchName,
            matchDate: m.matchDate,
            status: m.status
        })),
        lastUpdated: fantasyCache?.lastFetchedAt || null
    };
}

function broadcastFantasyUpdate() {
    for (const [code, room] of rooms) {
        const data = computeRoomFantasyPoints(room);
        data.configured = isFantasyConfigured();
        io.to(code).emit('fantasyPointsUpdate', data);
    }
}

// Emit fantasy points update to a single room (called after team/player changes)
function emitFantasyUpdate(roomCode) {
    const room = getRoom(roomCode);
    if (!room) return;
    const data = computeRoomFantasyPoints(room);
    data.configured = isFantasyConfigured();
    io.to(roomCode).emit('fantasyPointsUpdate', data);
}

// Generate random 6-character room code
function generateRoomCode() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let code = '';
    for (let i = 0; i < 6; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
}

// Create a new room with fresh player data
function createRoom(code, adminPassword, roomName) {
    const players = playersData.map((p, index) => ({
        id: index + 1,
        ...p,
        status: 'available'
    }));

    rooms.set(code, {
        code,
        roomName: roomName || 'Auction Room',
        createdAt: Date.now(),
        pinned: false,
        adminPassword: adminPassword || '',
        teams: [],
        players,
        auctionState: {
            status: 'waiting',
            currentPlayer: null,
            currentBid: 0,
            currentBidder: null,
            soldPlayers: [],
            unsoldPlayers: [],
            bidLog: [],
            bidHistory: [],
            auctionFinished: false
        }
    });

    saveRooms(code); // Save immediately after creating room
    return rooms.get(code);
}

// Get room or return null
function getRoom(code) {
    return rooms.get(code?.toUpperCase()) || null;
}

// ============ REST API ============

// List active rooms for landing page
app.get('/api/rooms', (req, res) => {
    const roomList = [];
    for (const [code, room] of rooms) {
        roomList.push({
            code: room.code,
            roomName: room.roomName || 'Auction Room',
            teamCount: room.teams.length,
            playersSold: room.auctionState.soldPlayers.length,
            status: room.auctionState.status,
            createdAt: room.createdAt,
            pinned: !!room.pinned
        });
    }
    // Pinned rooms first, then by newest
    roomList.sort((a, b) => {
        if (a.pinned !== b.pinned) return a.pinned ? -1 : 1;
        return b.createdAt - a.createdAt;
    });
    res.json(roomList.slice(0, 50));
});

// Create a new auction room
app.post('/api/room/create', (req, res) => {
    const { adminPassword, roomName } = req.body || {};
    if (!adminPassword || adminPassword.trim().length === 0) {
        return res.status(400).json({ error: 'Admin password is required.' });
    }
    if (!roomName || roomName.trim().length === 0) {
        return res.status(400).json({ error: 'Room name is required.' });
    }
    let code = generateRoomCode();
    // Make sure code is unique
    while (rooms.has(code)) {
        code = generateRoomCode();
    }
    createRoom(code, adminPassword.trim(), roomName.trim());
    console.log(`Room created: ${code} (${roomName.trim()})`);
    res.json({ code });
});

// Import auction data from Excel export to create a new room
app.post('/api/room/import', (req, res) => {
    const { adminPassword, roomName, teams, soldPlayers, unsoldPlayers, allPlayers } = req.body || {};
    if (!adminPassword || adminPassword.trim().length === 0) {
        return res.status(400).json({ error: 'Admin password is required.' });
    }
    if (!roomName || roomName.trim().length === 0) {
        return res.status(400).json({ error: 'Room name is required.' });
    }
    if (!teams || !Array.isArray(teams) || teams.length === 0) {
        return res.status(400).json({ error: 'No teams found in Excel data.' });
    }

    let code = generateRoomCode();
    while (rooms.has(code)) code = generateRoomCode();

    // Build players list: start from allPlayers (with base prices), fall back to players.json
    const basePlayerMap = new Map(playersData.map(p => [p.name.toLowerCase(), p]));

    // Build a map from player name → base price from allPlayers sheet
    const allPlayersMap = new Map();
    (allPlayers || []).forEach(p => {
        if (p.name) allPlayersMap.set(p.name.toLowerCase(), p);
    });

    // Collect all unique player names across sold + unsold + allPlayers
    const playerNames = new Set();
    (allPlayers || []).forEach(p => p.name && playerNames.add(p.name));
    (soldPlayers || []).forEach(p => p.name && playerNames.add(p.name));
    (unsoldPlayers || []).forEach(p => p.name && playerNames.add(p.name));

    // Build reconstructed players array
    let playerIdCounter = 1;
    const reconstructedPlayers = [];
    const playerIdByName = new Map();

    for (const name of playerNames) {
        const key = name.toLowerCase();
        const fromAll = allPlayersMap.get(key);
        const fromBase = basePlayerMap.get(key);
        const player = {
            id: playerIdCounter++,
            name: fromAll?.name || fromBase?.name || name,
            role: fromAll?.role || fromBase?.role || 'Batsman',
            country: fromAll?.country || fromBase?.country || 'India',
            previousTeam: fromAll?.previousTeam || fromBase?.previousTeam || 'Uncapped',
            basePrice: fromAll?.basePrice || fromBase?.basePrice || 0.2,
            status: fromAll?.status || 'available',
            soldTo: null,
            soldToName: null,
            soldPrice: null
        };
        playerIdByName.set(key, player.id);
        reconstructedPlayers.push(player);
    }

    // Build team objects
    const reconstructedTeams = teams.map((t, i) => ({
        id: `team_${Date.now()}_${i}`,
        name: t.name,
        ownerName: t.ownerName || '',
        budget: typeof t.budget === 'number' ? t.budget : INITIAL_BUDGET,
        players: []
    }));
    const teamByName = new Map(reconstructedTeams.map(t => [t.name.toLowerCase(), t]));

    // Apply sold players: update player status and add to teams
    const reconstructedSoldPlayers = [];
    (soldPlayers || []).forEach(sp => {
        if (!sp.name || !sp.soldToName) return;
        const key = sp.name.toLowerCase();
        let player = reconstructedPlayers.find(p => p.name.toLowerCase() === key);
        if (!player) {
            // Player not in allPlayers sheet - create it
            const fromBase = basePlayerMap.get(key);
            player = {
                id: playerIdCounter++,
                name: sp.name,
                role: sp.role || fromBase?.role || 'Batsman',
                country: sp.country || fromBase?.country || 'India',
                previousTeam: sp.previousTeam || fromBase?.previousTeam || 'Uncapped',
                basePrice: fromBase?.basePrice || 0.2,
                status: 'available',
                soldTo: null, soldToName: null, soldPrice: null
            };
            playerIdByName.set(key, player.id);
            reconstructedPlayers.push(player);
        }
        const team = teamByName.get(sp.soldToName.toLowerCase());
        if (!team) return;

        const soldPrice = typeof sp.soldPrice === 'number' ? sp.soldPrice : 0;
        player.status = 'sold';
        player.soldTo = team.id;
        player.soldToName = team.name;
        player.soldPrice = soldPrice;

        const playerForTeam = { ...player };
        team.players.push(playerForTeam);
        reconstructedSoldPlayers.push({ ...player });
    });

    // Apply unsold players
    const reconstructedUnsoldPlayers = [];
    (unsoldPlayers || []).forEach(up => {
        if (!up.name) return;
        const key = up.name.toLowerCase();
        let player = reconstructedPlayers.find(p => p.name.toLowerCase() === key);
        if (!player) {
            const fromBase = basePlayerMap.get(key);
            player = {
                id: playerIdCounter++,
                name: up.name,
                role: up.role || fromBase?.role || 'Batsman',
                country: up.country || fromBase?.country || 'India',
                previousTeam: up.previousTeam || fromBase?.previousTeam || 'Uncapped',
                basePrice: up.basePrice || fromBase?.basePrice || 0.2,
                status: 'available',
                soldTo: null, soldToName: null, soldPrice: null
            };
            reconstructedPlayers.push(player);
        }
        player.status = 'unsold';
        reconstructedUnsoldPlayers.push({ ...player });
    });

    // Any player still 'available' in allPlayers with SOLD/UNSOLD status from that sheet
    reconstructedPlayers.forEach(p => {
        const fromAll = allPlayersMap.get(p.name.toLowerCase());
        if (!fromAll) return;
        if (fromAll.status === 'sold' && p.status === 'available') p.status = 'sold';
        else if (fromAll.status === 'unsold' && p.status === 'available') p.status = 'unsold';
    });

    const room = {
        code,
        roomName: roomName.trim(),
        createdAt: Date.now(),
        adminPassword: adminPassword.trim(),
        teams: reconstructedTeams,
        players: reconstructedPlayers,
        auctionState: {
            status: 'waiting',
            currentPlayer: null,
            currentBid: 0,
            currentBidder: null,
            soldPlayers: reconstructedSoldPlayers,
            unsoldPlayers: reconstructedUnsoldPlayers,
            bidLog: [],
            bidHistory: [],
            auctionFinished: false
        }
    };

    rooms.set(code, room);
    saveRooms(code);
    console.log(`Room imported: ${code} (${roomName.trim()}) - ${reconstructedTeams.length} teams, ${reconstructedSoldPlayers.length} sold players`);
    res.json({ code });
});

// Join an existing room
app.post('/api/room/join', (req, res) => {
    const { code } = req.body;
    const room = getRoom(code);
    if (room) {
        res.json({ success: true, code: room.code, roomName: room.roomName || 'Auction Room', pinned: !!room.pinned });
    } else {
        res.status(404).json({ error: 'Room not found. Check the code and try again.' });
    }
});

// Get room data
app.get('/api/room/:code/data', (req, res) => {
    const room = getRoom(req.params.code);
    if (!room) {
        return res.status(404).json({ error: 'Room not found' });
    }
    res.json({
        teams: room.teams,
        players: room.players,
        auctionState: room.auctionState,
        config: { initialBudget: INITIAL_BUDGET }
    });
});

// Create team in room
app.post('/api/room/:code/teams', (req, res) => {
    try {
        const room = getRoom(req.params.code);
        if (!room) {
            console.log(`Team creation failed: Room ${req.params.code} not found`);
            return res.status(404).json({ error: 'Room not found. Please refresh and rejoin.' });
        }

        const { name, ownerName } = req.body;
        if (!name || !name.trim()) {
            return res.status(400).json({ error: 'Team name required' });
        }

        // Check for duplicate team names
        const existingTeam = room.teams.find(t => t.name.toLowerCase() === name.trim().toLowerCase());
        if (existingTeam) {
            return res.status(400).json({ error: 'A team with this name already exists' });
        }

        const team = {
            id: `team_${Date.now()}_${Math.floor(Math.random() * 100000)}`, // String ID for safety
            name: name.trim(),
            ownerName: ownerName || '',
            budget: INITIAL_BUDGET,
            players: []
        };

        room.teams.push(team);
        saveRooms(room.code); // Save after team creation
        console.log(`Team "${team.name}" added to room ${room.code}. Total teams: ${room.teams.length}`);
        io.to(room.code).emit('teamsUpdated', room.teams);
        emitFantasyUpdate(room.code);
        res.json(team);
    } catch (err) {
        console.error('Error creating team:', err);
        res.status(500).json({ error: 'Server error creating team' });
    }
});

// Update player base price in room
app.post('/api/room/:code/players/:id/price', (req, res) => {
    const room = getRoom(req.params.code);
    if (!room) {
        return res.status(404).json({ error: 'Room not found' });
    }

    const id = parseInt(req.params.id);
    const { basePrice } = req.body;

    const playerIndex = room.players.findIndex(p => p.id === id);
    if (playerIndex !== -1) {
        room.players[playerIndex].basePrice = basePrice;
        saveRooms(room.code); // Save after price update
        io.to(room.code).emit('playersUpdated', room.players);
        res.json({ success: true });
    } else {
        res.status(404).json({ error: 'Player not found' });
    }
});

// Bulk save all player prices
app.post('/api/room/:code/players/save-prices', (req, res) => {
    const room = getRoom(req.params.code);
    if (!room) {
        return res.status(404).json({ error: 'Room not found' });
    }
    saveRooms(room.code);
    console.log(`Prices saved for room ${room.code} (${room.players.length} players)`);
    res.json({ success: true, message: 'All prices saved successfully' });
});

// Delete team from room
app.delete('/api/room/:code/teams/:id', (req, res) => {
    const room = getRoom(req.params.code);
    if (!room) {
        return res.status(404).json({ error: 'Room not found' });
    }

    const id = req.params.id; // String ID
    room.teams = room.teams.filter(t => t.id !== id);
    saveRooms(room.code); // Save after team deletion
    io.to(room.code).emit('teamsUpdated', room.teams);
    emitFantasyUpdate(room.code);
    res.json({ success: true });
});

// Reset auction in room
app.post('/api/room/:code/reset', (req, res) => {
    const room = getRoom(req.params.code);
    if (!room) {
        return res.status(404).json({ error: 'Room not found' });
    }

    // Reset all players
    room.players = room.players.map(p => ({ ...p, status: 'available', soldTo: null, soldPrice: null }));

    // Reset teams
    room.teams = room.teams.map(t => ({ ...t, budget: INITIAL_BUDGET, players: [] }));

    // Reset auction state
    room.auctionState = {
        status: 'waiting',
        currentPlayer: null,
        currentBid: 0,
        currentBidder: null,
        soldPlayers: [],
        unsoldPlayers: [],
        bidLog: [],
        bidHistory: [],
        auctionFinished: false
    };

    saveRooms(room.code); // Save after reset
    io.to(room.code).emit('fullUpdate', { teams: room.teams, players: room.players, auctionState: room.auctionState, config: { initialBudget: INITIAL_BUDGET } });
    res.json({ success: true });
});

// Pin or unpin a room (admin only). Pinned rooms are retained for 6 months.
app.post('/api/room/:code/pin', (req, res) => {
    const room = getRoom(req.params.code);
    if (!room) return res.status(404).json({ error: 'Room not found' });
    const { adminPassword, pin } = req.body || {};
    if (!adminPassword || adminPassword !== room.adminPassword) {
        return res.status(403).json({ error: 'Invalid admin password' });
    }
    room.pinned = !!pin;
    saveRooms(room.code);
    console.log(`Room ${room.code} ${room.pinned ? 'pinned' : 'unpinned'}`);
    io.to(room.code).emit('roomPinned', { pinned: room.pinned });
    res.json({ success: true, pinned: room.pinned });
});

// ============ FANTASY POINTS API ============

// Get fantasy points for a room
app.get('/api/room/:code/fantasy-points', (req, res) => {
    const room = getRoom(req.params.code);
    if (!room) return res.status(404).json({ error: 'Room not found' });

    // Always compute — returns all teams with 0 pts when no data yet
    const data = computeRoomFantasyPoints(room);
    data.configured = isFantasyConfigured();
    data.seasonYear = IPL_SEASON_YEAR;
    res.json(data);
});

// Admin: Trigger manual fantasy points refresh
app.post('/api/room/:code/fantasy-points/refresh', async (req, res) => {
    try {
        const result = await fetchAllFantasyPoints();
        res.json({ success: true, source: getFantasySource(), ...result });
    } catch (err) {
        console.error('Manual fantasy refresh failed:', err);
        res.status(500).json({ error: err.message });
    }
});

// Admin: Update fantasy data source at runtime
app.post('/api/room/:code/fantasy-config', (req, res) => {
    const { apiKey, seriesId, iplFantasyUid, iplFantasyToken, source } = req.body;

    if (source === 'auto') {
        // Reset to default: public IPL stats (no credentials)
        process.env.IPL_FANTASY_UID = '';
        process.env.IPL_FANTASY_AUTH_TOKEN = '';
        process.env.CRICAPI_KEY = '';
        process.env.CRICAPI_SERIES_ID = '';
    } else {
        // IPL Fantasy cookies (most accurate)
        if (iplFantasyUid !== undefined)   process.env.IPL_FANTASY_UID = iplFantasyUid;
        if (iplFantasyToken !== undefined)  process.env.IPL_FANTASY_AUTH_TOKEN = iplFantasyToken;
        // CricAPI
        if (apiKey !== undefined)  process.env.CRICAPI_KEY = apiKey;
        if (seriesId !== undefined) process.env.CRICAPI_SERIES_ID = seriesId;
    }

    startFantasyRefreshTimer();
    fetchAllFantasyPoints().catch(err =>
        console.error('Fantasy fetch after config save failed:', err.message)
    );

    res.json({ success: true, source: getFantasySource() });
});

// ============ SOCKET.IO EVENTS ============
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);

    // Join a room
    socket.on('joinRoom', (code, adminPassword) => {
        const room = getRoom(code);
        if (room) {
            socket.join(room.code);
            socket.roomCode = room.code;
            const isAdmin = !!(adminPassword && room.adminPassword && adminPassword === room.adminPassword);
            socket.isAdmin = isAdmin;
            console.log(`Socket ${socket.id} joined room ${room.code} (admin: ${isAdmin})`);
            socket.emit('fullUpdate', {
                teams: room.teams,
                players: room.players,
                auctionState: room.auctionState,
                config: { initialBudget: INITIAL_BUDGET }
            });
            socket.emit('adminStatus', { isAdmin });
        } else {
            socket.emit('roomError', 'Room not found. It may have been lost after a server restart. Please create a new room.');
        }
    });

    // Admin: Select player for auction
    socket.on('selectPlayer', (playerId) => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        const player = room.players.find(p => p.id === playerId);
        if (player && player.status === 'available') {
            room.auctionState.currentPlayer = player;
            room.auctionState.currentBid = player.basePrice;
            room.auctionState.currentBidder = null;
            room.auctionState.status = 'bidding';
            room.auctionState.timerStart = Date.now();
            room.auctionState.bidLog = [{ type: 'start', playerName: player.name, amount: player.basePrice, time: Date.now() }];
            saveRooms(room.code);
            io.to(room.code).emit('auctionUpdate', room.auctionState);
        }
    });

    // Admin: Cancel current bid and return player to pool
    socket.on('cancelBid', () => {
        const room = getRoom(socket.roomCode);
        if (!room) return;
        if (room.auctionState.status !== 'bidding') return;
        if (!room.auctionState.currentPlayer) return;

        // Reset auction state - player stays 'available'
        room.auctionState.currentPlayer = null;
        room.auctionState.currentBid = 0;
        room.auctionState.currentBidder = null;
        room.auctionState.status = 'waiting';
        room.auctionState.timerStart = null;
        room.auctionState.bidLog = [];
        saveRooms(room.code);
        io.to(room.code).emit('auctionUpdate', room.auctionState);
    });

    // Admin: Re-auction an unsold player
    socket.on('reAuctionPlayer', (playerId) => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        const player = room.players.find(p => p.id === playerId);
        if (player && player.status === 'unsold') {
            // Move back to available
            player.status = 'available';
            // Remove from unsoldPlayers list
            room.auctionState.unsoldPlayers = room.auctionState.unsoldPlayers.filter(p => p.id !== playerId);
            saveRooms(room.code);
            io.to(room.code).emit('fullUpdate', {
                teams: room.teams,
                players: room.players,
                auctionState: room.auctionState,
                config: { initialBudget: INITIAL_BUDGET }
            });
        }
    });

    // Admin: Update bid
    socket.on('updateBid', ({ amount, teamId }) => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        // Round to avoid floating-point drift
        amount = Math.round(amount * 100) / 100;

        const team = room.teams.find(t => t.id === teamId);
        if (!team || room.auctionState.status !== 'bidding') return;

        // Budget check (round to handle floating-point precision)
        if (amount > Math.round(team.budget * 100) / 100) return;

        // Max 16 players check
        const playerCount = (team.players || []).length;
        if (playerCount >= 16) return;

        // Overseas limit check (max 6)
        const currentPlayer = room.auctionState.currentPlayer;
        if (currentPlayer && currentPlayer.country !== 'India') {
            const overseasCount = (team.players || []).filter(p => p.country !== 'India').length;
            if (overseasCount >= 6) return;
        }

        // Min 12 players budget check - ensure remaining budget can fill squad
        const remainingBudgetAfterBid = Math.round((team.budget - amount) * 100) / 100;
        const playersNeededAfterThis = 12 - (playerCount + 1);
        if (playersNeededAfterThis > 0) {
            const minBudgetNeeded = Math.round(playersNeededAfterThis * 0.2 * 100) / 100; // 20L minimum per player
            if (remainingBudgetAfterBid < minBudgetNeeded) return;
        }

        room.auctionState.currentBid = amount;
        room.auctionState.currentBidder = team;
        if (!room.auctionState.bidLog) room.auctionState.bidLog = [];
        room.auctionState.bidLog.push({ type: 'bid', teamName: team.name, teamId: team.id, amount, time: Date.now() });
        saveRooms(room.code); // Save after each bid
        io.to(room.code).emit('auctionUpdate', room.auctionState);
    });

    // Admin: Undo last bid
    socket.on('undoBid', () => {
        const room = getRoom(socket.roomCode);
        if (!room) return;
        if (room.auctionState.status !== 'bidding') return;

        const bidLog = room.auctionState.bidLog || [];
        // Find the last 'bid' entry to remove
        let lastBidIndex = -1;
        for (let i = bidLog.length - 1; i >= 0; i--) {
            if (bidLog[i].type === 'bid') { lastBidIndex = i; break; }
        }
        if (lastBidIndex === -1) return; // No bids to undo

        // Remove the last bid
        bidLog.splice(lastBidIndex, 1);

        // Find the new latest bid to restore state
        let prevBid = null;
        for (let i = bidLog.length - 1; i >= 0; i--) {
            if (bidLog[i].type === 'bid') { prevBid = bidLog[i]; break; }
        }

        if (prevBid) {
            // Restore to previous bid
            room.auctionState.currentBid = prevBid.amount;
            const prevTeam = room.teams.find(t => t.id === prevBid.teamId);
            room.auctionState.currentBidder = prevTeam || null;
        } else {
            // No bids left, restore to base price
            room.auctionState.currentBid = room.auctionState.currentPlayer ? room.auctionState.currentPlayer.basePrice : 0.2;
            room.auctionState.currentBidder = null;
        }

        saveRooms(room.code);
        io.to(room.code).emit('auctionUpdate', room.auctionState);
    });

    // Admin: Mark as sold
    socket.on('markSold', () => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        // Prevent double-click: only process if status is 'bidding'
        if (room.auctionState.status !== 'bidding') return;
        if (!room.auctionState.currentPlayer || !room.auctionState.currentBidder) return;

        const player = room.auctionState.currentPlayer;
        const team = room.auctionState.currentBidder;
        const price = room.auctionState.currentBid;

        // Find player and team with bounds check
        const playerIndex = room.players.findIndex(p => p.id === player.id);
        const teamIndex = room.teams.findIndex(t => t.id === team.id);
        if (playerIndex === -1 || teamIndex === -1) return;

        // Update player
        room.players[playerIndex].status = 'sold';
        room.players[playerIndex].soldTo = team.id;
        room.players[playerIndex].soldToName = team.name;
        room.players[playerIndex].soldPrice = price;

        // Update team (round to avoid floating-point drift)
        room.teams[teamIndex].budget = Math.round((room.teams[teamIndex].budget - price) * 100) / 100;
        room.teams[teamIndex].players.push({
            ...room.players[playerIndex],
            soldPrice: price
        });

        // Update auction state
        room.auctionState.soldPlayers.push({
            ...room.players[playerIndex],
            soldPrice: price,
            soldToName: team.name
        });

        room.auctionState.status = 'sold';
        if (!room.auctionState.bidLog) room.auctionState.bidLog = [];
        room.auctionState.bidLog.push({ type: 'sold', teamName: team.name, amount: price, playerName: player.name, time: Date.now() });
        // Save to bid history
        if (!room.auctionState.bidHistory) room.auctionState.bidHistory = [];
        room.auctionState.bidHistory.push({
            playerName: player.name,
            iplTeam: player.previousTeam || 'Uncapped',
            country: player.country,
            basePrice: player.basePrice,
            result: 'sold',
            soldTo: team.name,
            soldPrice: price,
            bids: (room.auctionState.bidLog || []).filter(b => b.type === 'bid')
        });

        saveRooms(room.code); // Save immediately after sale

        io.to(room.code).emit('playerSold', {
            player: room.players[playerIndex],
            team: room.teams[teamIndex],
            price
        });

        io.to(room.code).emit('fullUpdate', { teams: room.teams, players: room.players, auctionState: room.auctionState, config: { initialBudget: INITIAL_BUDGET } });
        emitFantasyUpdate(room.code);

        // Reset after 3 seconds
        setTimeout(() => {
            room.auctionState.status = 'waiting';
            room.auctionState.currentPlayer = null;
            room.auctionState.currentBid = 0;
            room.auctionState.currentBidder = null;
            saveRooms(room.code);
            io.to(room.code).emit('auctionUpdate', room.auctionState);
        }, 3000);
    });

    // Admin: Mark as unsold
    socket.on('markUnsold', () => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        // Prevent double-click: only process if status is 'bidding'
        if (room.auctionState.status !== 'bidding') return;
        if (!room.auctionState.currentPlayer) return;

        const player = room.auctionState.currentPlayer;

        // Find player with bounds check
        const playerIndex = room.players.findIndex(p => p.id === player.id);
        if (playerIndex === -1) return;

        room.players[playerIndex].status = 'unsold';

        // Update auction state
        room.auctionState.unsoldPlayers.push(room.players[playerIndex]);
        room.auctionState.status = 'unsold';
        if (!room.auctionState.bidLog) room.auctionState.bidLog = [];
        room.auctionState.bidLog.push({ type: 'unsold', playerName: player.name, time: Date.now() });
        // Save to bid history
        if (!room.auctionState.bidHistory) room.auctionState.bidHistory = [];
        room.auctionState.bidHistory.push({
            playerName: player.name,
            iplTeam: player.previousTeam || 'Uncapped',
            country: player.country,
            basePrice: player.basePrice,
            result: 'unsold',
            soldTo: null,
            soldPrice: null,
            bids: (room.auctionState.bidLog || []).filter(b => b.type === 'bid')
        });

        saveRooms(room.code); // Save immediately after unsold

        io.to(room.code).emit('fullUpdate', { teams: room.teams, players: room.players, auctionState: room.auctionState, config: { initialBudget: INITIAL_BUDGET } });

        // Reset after 2 seconds
        setTimeout(() => {
            room.auctionState.status = 'waiting';
            room.auctionState.currentPlayer = null;
            room.auctionState.currentBid = 0;
            room.auctionState.currentBidder = null;
            saveRooms(room.code);
            io.to(room.code).emit('auctionUpdate', room.auctionState);
        }, 2000);
    });

    // Fantasy points request
    socket.on('requestFantasyPoints', () => {
        const room = getRoom(socket.roomCode);
        if (!room) return;
        const data = computeRoomFantasyPoints(room);
        data.configured = isFantasyConfigured();
        socket.emit('fantasyPointsUpdate', data);
    });

    // Admin: Finish auction (lock teams)
    socket.on('finishAuction', () => {
        const room = getRoom(socket.roomCode);
        if (!room) return;
        room.auctionState.auctionFinished = true;
        room.auctionState.status = 'waiting';
        room.auctionState.currentPlayer = null;
        room.auctionState.currentBid = 0;
        room.auctionState.currentBidder = null;
        saveRooms(room.code);
        io.to(room.code).emit('fullUpdate', { teams: room.teams, players: room.players, auctionState: room.auctionState, config: { initialBudget: INITIAL_BUDGET } });
    });

    // Admin: Resume auction (unlock teams)
    socket.on('resumeAuction', () => {
        const room = getRoom(socket.roomCode);
        if (!room) return;
        room.auctionState.auctionFinished = false;
        saveRooms(room.code);
        io.to(room.code).emit('fullUpdate', { teams: room.teams, players: room.players, auctionState: room.auctionState, config: { initialBudget: INITIAL_BUDGET } });
    });

    // Admin: Trade player between teams
    socket.on('tradePlayer', ({ playerName, fromTeamId, toTeamId, tradePrice }) => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        const fromTeam = room.teams.find(t => t.id === fromTeamId);
        const toTeam = room.teams.find(t => t.id === toTeamId);
        if (!fromTeam || !toTeam) return;

        const playerIndex = fromTeam.players.findIndex(p => p.name === playerName);
        if (playerIndex === -1) return;

        const player = fromTeam.players[playerIndex];
        const price = tradePrice != null ? tradePrice : player.soldPrice;

        // Check target team budget (round to handle floating-point precision)
        if (price > Math.round(toTeam.budget * 100) / 100) return;

        // Check max 16 players
        if ((toTeam.players || []).length >= 16) return;

        // Check overseas limit
        if (player.country !== 'India') {
            const overseasCount = (toTeam.players || []).filter(p => p.country !== 'India').length;
            if (overseasCount >= 6) return;
        }

        // Remove from source team
        fromTeam.players.splice(playerIndex, 1);
        fromTeam.budget = Math.round((fromTeam.budget + player.soldPrice) * 100) / 100;

        // Add to target team
        player.soldTo = toTeamId;
        player.soldToName = toTeam.name;
        player.soldPrice = price;
        toTeam.players.push(player);
        toTeam.budget = Math.round((toTeam.budget - price) * 100) / 100;

        // Update in players array too
        const globalPlayer = room.players.find(p => p.name === playerName);
        if (globalPlayer) {
            globalPlayer.soldTo = toTeamId;
            globalPlayer.soldToName = toTeam.name;
            globalPlayer.soldPrice = price;
        }

        // Update in soldPlayers array
        const soldEntry = room.auctionState.soldPlayers.find(p => p.name === playerName);
        if (soldEntry) {
            soldEntry.soldTo = toTeamId;
            soldEntry.soldToName = toTeam.name;
            soldEntry.soldPrice = price;
        }

        saveRooms(room.code);
        console.log(`Trade: ${playerName} from ${fromTeam.name} to ${toTeam.name} for ${price} Cr`);
        io.to(room.code).emit('fullUpdate', { teams: room.teams, players: room.players, auctionState: room.auctionState, config: { initialBudget: INITIAL_BUDGET } });
        emitFantasyUpdate(room.code);
    });

    // Admin: Release player from team (return to unsold pool)
    socket.on('releasePlayer', ({ teamId, playerName }) => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        const team = room.teams.find(t => t.id === teamId);
        if (!team) return;

        const playerIndex = team.players.findIndex(p => p.name === playerName);
        if (playerIndex === -1) return;

        const player = team.players[playerIndex];
        const refundAmount = player.soldPrice || 0;

        // Remove player from team
        team.players.splice(playerIndex, 1);

        // Refund budget (round to avoid floating-point drift)
        team.budget = Math.round((team.budget + refundAmount) * 100) / 100;

        // Update global player status back to unsold
        const globalPlayer = room.players.find(p => p.name === playerName);
        if (globalPlayer) {
            globalPlayer.status = 'unsold';
            globalPlayer.soldTo = null;
            globalPlayer.soldToName = null;
            globalPlayer.soldPrice = null;
        }

        // Remove from soldPlayers list
        room.auctionState.soldPlayers = room.auctionState.soldPlayers.filter(
            p => p.name !== playerName
        );

        // Add to unsoldPlayers list (so they can be re-auctioned)
        if (globalPlayer) {
            room.auctionState.unsoldPlayers.push(globalPlayer);
        }

        saveRooms(room.code);
        console.log(`Released: ${playerName} from ${team.name}, refunded ${refundAmount} Cr`);
        io.to(room.code).emit('fullUpdate', { teams: room.teams, players: room.players, auctionState: room.auctionState, config: { initialBudget: INITIAL_BUDGET } });
        emitFantasyUpdate(room.code);
    });

    // Admin: Add new player and assign to a team
    socket.on('addPlayer', ({ name, role, country, previousTeam, basePrice, assignToTeamId }) => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        // Generate a new unique ID
        const maxId = room.players.reduce((max, p) => Math.max(max, p.id || 0), 0);
        const newPlayer = {
            id: maxId + 1,
            name: name.trim(),
            role: role || 'Unknown',
            country: country || 'India',
            previousTeam: previousTeam || 'Uncapped',
            basePrice: parseFloat(basePrice) || 0.2,
            status: 'available'
        };

        room.players.push(newPlayer);

        // If assigning directly to a team
        if (assignToTeamId) {
            const team = room.teams.find(t => t.id === assignToTeamId);
            if (team) {
                const price = newPlayer.basePrice;
                if (price <= Math.round(team.budget * 100) / 100 && (team.players || []).length < 16) {
                    newPlayer.status = 'sold';
                    newPlayer.soldTo = team.id;
                    newPlayer.soldToName = team.name;
                    newPlayer.soldPrice = price;
                    team.players.push({ ...newPlayer });
                    team.budget = Math.round((team.budget - price) * 100) / 100;
                    room.auctionState.soldPlayers.push({ ...newPlayer });
                }
            }
        }

        saveRooms(room.code);
        console.log(`New player added: ${newPlayer.name} (${newPlayer.role}) - ${newPlayer.basePrice} Cr`);
        io.to(room.code).emit('fullUpdate', { teams: room.teams, players: room.players, auctionState: room.auctionState, config: { initialBudget: INITIAL_BUDGET } });
    });

    socket.on('disconnect', () => {
        console.log('Client disconnected:', socket.id);
    });
});

// ============ START SERVER ============
const PORT = process.env.PORT || 3000;

initPersistence().then(async () => {
    await initFantasyPoints();
    httpServer.listen(PORT, () => {
        console.log(`
    ╔═══════════════════════════════════════╗
    ║     IPL ${IPL_SEASON_YEAR} Fantasy Auction          ║
    ║     Server running on port ${PORT}        ║
    ╠═══════════════════════════════════════╣
    ║  Storage: ${db ? 'MongoDB (persistent)' : 'File (local only)'}        ║
    ║  Fantasy: ${isFantasyConfigured() ? 'Configured' : 'Not configured'}               ║
    ╚═══════════════════════════════════════╝

    Open http://localhost:${PORT} in your browser
        `);
    });
}).catch(err => {
    console.error('Failed to start server:', err);
    process.exit(1);
});
