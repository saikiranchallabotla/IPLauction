const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer);

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ============ DATA STORE (In-Memory) ============
const INITIAL_BUDGET = 100;

// Store multiple auction rooms
const rooms = new Map();

// Load players data
const playersData = require('./data/players.json');
console.log(`Loaded ${playersData.length} players from players.json`);

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

    return rooms.get(code);
}

// Get room or return null
function getRoom(code) {
    return rooms.get(code?.toUpperCase()) || null;
}

// ============ REST API ============

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
            id: Date.now() + Math.random(), // Add random to avoid ID collision
            name: name.trim(),
            ownerName: ownerName || '',
            budget: INITIAL_BUDGET,
            players: []
        };

        room.teams.push(team);
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

    const id = parseInt(req.params.id);
    room.teams = room.teams.filter(t => t.id !== id);
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
            io.to(room.code).emit('auctionUpdate', room.auctionState);
        }
    });

    // Admin: Update bid
    socket.on('updateBid', ({ amount, teamId }) => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        const team = room.teams.find(t => t.id === teamId);
        if (team && amount <= team.budget && room.auctionState.status === 'bidding') {
            room.auctionState.currentBid = amount;
            room.auctionState.currentBidder = team;
            io.to(room.code).emit('auctionUpdate', room.auctionState);
        }
    });

    // Admin: Mark as sold
    socket.on('markSold', () => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        if (room.auctionState.currentPlayer && room.auctionState.currentBidder) {
            const player = room.auctionState.currentPlayer;
            const team = room.auctionState.currentBidder;
            const price = room.auctionState.currentBid;

            // Update player
            const playerIndex = room.players.findIndex(p => p.id === player.id);
            room.players[playerIndex].status = 'sold';
            room.players[playerIndex].soldTo = team.id;
            room.players[playerIndex].soldToName = team.name;
            room.players[playerIndex].soldPrice = price;

            // Update team
            const teamIndex = room.teams.findIndex(t => t.id === team.id);
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
                io.to(room.code).emit('auctionUpdate', room.auctionState);
            }, 3000);
        }
    });

    // Admin: Mark as unsold
    socket.on('markUnsold', () => {
        const room = getRoom(socket.roomCode);
        if (!room) return;

        if (room.auctionState.currentPlayer) {
            const player = room.auctionState.currentPlayer;

            // Update player
            const playerIndex = room.players.findIndex(p => p.id === player.id);
            room.players[playerIndex].status = 'unsold';

            // Update auction state
            room.auctionState.unsoldPlayers.push(room.players[playerIndex]);
            room.auctionState.status = 'unsold';

            io.to(room.code).emit('fullUpdate', { teams: room.teams, players: room.players, auctionState: room.auctionState, config: { initialBudget: INITIAL_BUDGET } });

            // Reset after 2 seconds
            setTimeout(() => {
                room.auctionState.status = 'waiting';
                room.auctionState.currentPlayer = null;
                room.auctionState.currentBid = 0;
                room.auctionState.currentBidder = null;
                io.to(room.code).emit('auctionUpdate', room.auctionState);
            }, 2000);
        }
    });

    socket.on('disconnect', () => {
        console.log('Client disconnected:', socket.id);
    });
});

// ============ START SERVER ============
const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
    console.log(`
    ╔═══════════════════════════════════════╗
    ║     IPL 2026 Fantasy Auction          ║
    ║     Server running on port ${PORT}        ║
    ╠═══════════════════════════════════════╣
    ║  Create/Join rooms with unique codes  ║
    ╚═══════════════════════════════════════╝

    Open http://localhost:${PORT} in your browser
    `);
});
