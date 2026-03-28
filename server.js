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

            // Clean up rooms older than 7 days
            const weekAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
            let cleaned = 0;
            for (const [code, room] of rooms) {
                if (room.createdAt < weekAgo) {
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

        // Clean up old rooms
        const weekAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
        let cleaned = 0;
        for (const [code, room] of rooms) {
            if (room.createdAt < weekAgo) {
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
    const cricSeries = process.env.CRICAPI_SERIES_ID || CRICAPI_SERIES_ID;
    if (cricKey && cricSeries) return 'cricapi';
    return 'ipl_stats'; // public, no auth
}

async function initFantasyPoints() {
    if (db) {
        // Prefer IPL Fantasy cookies cache > public stats cache > CricAPI cache
        const iplCookieDoc = await db.collection('fantasy_points').findOne({ seriesId: 'ipl_fantasy_official' });
        if (iplCookieDoc) {
            fantasyCache = iplCookieDoc;
            console.log(`Loaded IPL Fantasy (cookie) cache: ${iplCookieDoc.matches?.length || 0} matches`);
        } else {
            const statsDoc = await db.collection('fantasy_points').findOne({ seriesId: 'ipl_public_stats' });
            if (statsDoc) {
                fantasyCache = statsDoc;
                console.log(`Loaded IPL public stats cache: ${statsDoc.matches?.length || 0} matches`);
            } else if (CRICAPI_SERIES_ID) {
                const cricDoc = await db.collection('fantasy_points').findOne({ seriesId: CRICAPI_SERIES_ID });
                if (cricDoc) {
                    fantasyCache = cricDoc;
                    console.log(`Loaded CricAPI cache: ${cricDoc.matches?.length || 0} matches`);
                }
            }
        }
    }
    // Always start — public IPL stats requires no credentials
    startFantasyRefreshTimer();
    fetchAllFantasyPoints().catch(err =>
        console.error('Initial fantasy fetch failed:', err.message)
    );
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
    }, 2 * 60 * 1000); // every 2 minutes during live matches
    console.log('Live match detected: accelerated refresh every 2 minutes');
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
        return await fetchFromIPLStats(); // default: public stats, no auth needed
    } finally {
        isRefreshing = false;
    }
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
    const fixtures = parseIPLFixtures(fixturesJson);
    if (fixtures.length === 0) {
        // Log the top-level keys to help diagnose response format changes
        const topKeys = Object.keys(fixturesJson || {});
        const dataKeys = fixturesJson?.data ? Object.keys(fixturesJson.data) : [];
        console.log(`No live/completed fixtures found. Response top-keys: [${topKeys}], data keys: [${dataKeys}]`);
    }
    console.log(`Found ${fixtures.length} completed/live IPL fixtures`);

    const existingMatches = (fantasyCache?.matches || []);
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

    console.log(`IPL Fantasy updated: ${allMatches.length} total matches, ${newMatches.length} new/updated`);
    broadcastFantasyUpdate();
    // Accelerate polling if any live match is ongoing
    if (allMatches.some(m => m.status === 'live')) startLiveMatchTimer();
    else stopLiveMatchTimer();
    return { totalMatches: allMatches.length, newMatches: newMatches.length };
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

async function fetchFromCricAPI() {
    const apiKey = process.env.CRICAPI_KEY || CRICAPI_KEY;
    const seriesId = process.env.CRICAPI_SERIES_ID || CRICAPI_SERIES_ID;
    if (!apiKey || !seriesId) {
        throw new Error('CRICAPI_KEY and CRICAPI_SERIES_ID must be set');
    }

    console.log('Fetching fantasy points from CricAPI...');

    // Step 1: Get all match IDs for the series
    const seriesRes = await fetch(
        `https://api.cricapi.com/v1/series_info?apikey=${apiKey}&id=${seriesId}`
    );
    const seriesData = await seriesRes.json();
    if (seriesData.status !== 'success') {
        throw new Error(`CricAPI series_info error: ${JSON.stringify(seriesData.info || 'unknown')}`);
    }

    const matchList = seriesData.data?.matchList || [];
    console.log(`Found ${matchList.length} matches in series`);

    // Step 2: For each completed/live match, fetch fantasy points
    const existingMatches = (fantasyCache?.matches || []);
    // Only skip completed matches older than 24 hours — recent ones may still
    // have points being updated on the fantasy server.
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
        // Skip already-fetched completed matches (older than 24 hours)
        if (existingCompleted.has(match.id)) continue;
        // Skip future matches
        if (!match.matchStarted && !match.matchEnded) continue;
        // Rate limit
        if (apiCalls >= 50) {
            console.log('Rate limit reached, will fetch remaining next cycle');
            break;
        }

        try {
            const pointsRes = await fetch(
                `https://api.cricapi.com/v1/match_points?apikey=${apiKey}&id=${match.id}&ruleset=0&offset=0`
            );
            const pointsData = await pointsRes.json();
            apiCalls++;

            if (pointsData.status === 'success' && pointsData.data?.totals) {
                newMatches.push({
                    matchId: match.id,
                    matchName: match.name || `Match`,
                    matchDate: match.date || match.dateTimeGMT || '',
                    status: match.matchEnded ? 'completed' : 'live',
                    playerPoints: pointsData.data.totals.map(p => ({
                        cricApiId: p.id || '',
                        cricApiName: p.name,
                        points: parseFloat(p.points) || 0
                    }))
                });
            }
        } catch (err) {
            console.error(`Failed to fetch points for match ${match.id}:`, err.message);
        }

        await new Promise(resolve => setTimeout(resolve, 200));
    }

    // Step 3: Merge
    const allMatches = [...existingMatches];
    for (const nm of newMatches) {
        const idx = allMatches.findIndex(m => m.matchId === nm.matchId);
        if (idx >= 0) allMatches[idx] = nm;
        else allMatches.push(nm);
    }
    allMatches.sort((a, b) => new Date(a.matchDate) - new Date(b.matchDate));

    const nameMapping = buildNameMapping(allMatches);

    fantasyCache = {
        seriesId: seriesId,
        lastFetchedAt: Date.now(),
        matches: allMatches,
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
    allMatches.sort((a, b) => new Date(a.matchDate) - new Date(b.matchDate));

    const nameMapping = buildNameMapping(allMatches);
    fantasyCache = { seriesId: 'ipl_public_stats', lastFetchedAt: Date.now(), matches: allMatches, nameMapping };

    if (db) {
        await db.collection('fantasy_points').updateOne(
            { seriesId: 'ipl_public_stats' }, { $set: fantasyCache }, { upsert: true }
        );
    }

    console.log(`IPL public stats: ${allMatches.length} total matches, ${newMatches.length} updated`);
    broadcastFantasyUpdate();
    if (allMatches.some(m => m.status === 'live')) startLiveMatchTimer(); else stopLiveMatchTimer();
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
            createdAt: room.createdAt
        });
    }
    roomList.sort((a, b) => b.createdAt - a.createdAt);
    res.json(roomList);
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
        res.json({ success: true, code: room.code, roomName: room.roomName || 'Auction Room' });
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

// ============ FANTASY POINTS API ============

// Get fantasy points for a room
app.get('/api/room/:code/fantasy-points', (req, res) => {
    const room = getRoom(req.params.code);
    if (!room) return res.status(404).json({ error: 'Room not found' });

    // Always compute — returns all teams with 0 pts when no data yet
    const data = computeRoomFantasyPoints(room);
    data.configured = isFantasyConfigured();
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
    ║     IPL 2026 Fantasy Auction          ║
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
