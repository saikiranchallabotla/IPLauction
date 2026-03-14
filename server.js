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
        try {
            const { MongoClient } = require('mongodb');
            const client = new MongoClient(MONGODB_URI);
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
function createRoom(code) {
    const players = playersData.map((p, index) => ({
        id: index + 1,
        ...p,
        status: 'available'
    }));

    rooms.set(code, {
        code,
        createdAt: Date.now(),
        teams: [],
        players,
        auctionState: {
            status: 'waiting',
            currentPlayer: null,
            currentBid: 0,
            currentBidder: null,
            soldPlayers: [],
            unsoldPlayers: []
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

// List all active rooms (for landing page)
app.get('/api/rooms', (req, res) => {
    const roomList = [];
    for (const [code, room] of rooms) {
        roomList.push({
            code: room.code,
            teamCount: room.teams.length,
            playersSold: room.auctionState.soldPlayers.length,
            status: room.auctionState.status,
            createdAt: room.createdAt
        });
    }
    // Sort by most recently created first
    roomList.sort((a, b) => b.createdAt - a.createdAt);
    res.json(roomList);
});

// Create a new auction room
app.post('/api/room/create', (req, res) => {
    let code = generateRoomCode();
    // Make sure code is unique
    while (rooms.has(code)) {
        code = generateRoomCode();
    }
    createRoom(code);
    console.log(`Room created: ${code}`);
    res.json({ code });
});

// Join an existing room
app.post('/api/room/join', (req, res) => {
    const { code } = req.body;
    const room = getRoom(code);
    if (room) {
        res.json({ success: true, code: room.code });
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
        unsoldPlayers: []
    };

    saveRooms(room.code); // Save after reset
    io.to(room.code).emit('fullUpdate', { teams: room.teams, players: room.players, auctionState: room.auctionState, config: { initialBudget: INITIAL_BUDGET } });
    res.json({ success: true });
});

// ============ SOCKET.IO EVENTS ============
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);

    // Join a room
    socket.on('joinRoom', (code) => {
        const room = getRoom(code);
        if (room) {
            socket.join(room.code);
            socket.roomCode = room.code;
            console.log(`Socket ${socket.id} joined room ${room.code}`);
            socket.emit('fullUpdate', {
                teams: room.teams,
                players: room.players,
                auctionState: room.auctionState,
                config: { initialBudget: INITIAL_BUDGET }
            });
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
            saveRooms(room.code); // Save when player selected
            io.to(room.code).emit('auctionUpdate', room.auctionState);
        }
    });

    // Admin: Update bid
    socket.on('updateBid', ({ amount, teamId }) => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        const team = room.teams.find(t => t.id === teamId);
        if (!team || room.auctionState.status !== 'bidding') return;

        // Budget check
        if (amount > team.budget) return;

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
        const remainingBudgetAfterBid = team.budget - amount;
        const playersNeededAfterThis = 12 - (playerCount + 1);
        if (playersNeededAfterThis > 0) {
            const minBudgetNeeded = playersNeededAfterThis * 0.2; // 20L minimum per player
            if (remainingBudgetAfterBid < minBudgetNeeded) return;
        }

        room.auctionState.currentBid = amount;
        room.auctionState.currentBidder = team;
        saveRooms(room.code); // Save after each bid
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

        // Update team
        room.teams[teamIndex].budget -= price;
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

        saveRooms(room.code); // Save immediately after sale

        io.to(room.code).emit('playerSold', {
            player: room.players[playerIndex],
            team: room.teams[teamIndex],
            price
        });

        io.to(room.code).emit('fullUpdate', { teams: room.teams, players: room.players, auctionState: room.auctionState, config: { initialBudget: INITIAL_BUDGET } });

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

    socket.on('disconnect', () => {
        console.log('Client disconnected:', socket.id);
    });
});

// ============ START SERVER ============
const PORT = process.env.PORT || 3000;

initPersistence().then(() => {
    httpServer.listen(PORT, () => {
        console.log(`
    ╔═══════════════════════════════════════╗
    ║     IPL 2026 Fantasy Auction          ║
    ║     Server running on port ${PORT}        ║
    ╠═══════════════════════════════════════╣
    ║  Storage: ${db ? 'MongoDB (persistent)' : 'File (local only)'}        ║
    ╚═══════════════════════════════════════╝

    Open http://localhost:${PORT} in your browser
        `);
    });
}).catch(err => {
    console.error('Failed to start server:', err);
    process.exit(1);
});
