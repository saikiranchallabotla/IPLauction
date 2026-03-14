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
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'ipl2026';
const INITIAL_BUDGET = 100;

let teams = [];
let players = [];
let auctionState = {
    status: 'waiting', // waiting, bidding, sold, unsold, paused
    currentPlayer: null,
    currentBid: 0,
    currentBidder: null,
    soldPlayers: [],
    unsoldPlayers: []
};

// Initialize players from data
const playersData = require('./data/players.json');
players = playersData.map((p, index) => ({
    id: index + 1,
    ...p,
    status: 'available' // available, sold, unsold
}));

// ============ REST API ============

// Get all data for initial load
app.get('/api/data', (req, res) => {
    res.json({
        teams,
        players,
        auctionState,
        config: { initialBudget: INITIAL_BUDGET }
    });
});

// Admin login (no password required)
app.post('/api/admin/login', (req, res) => {
    res.json({ success: true });
});

// Create team (admin only)
app.post('/api/teams', (req, res) => {
    const { name, ownerName } = req.body;
    if (!name) {
        return res.status(400).json({ error: 'Team name required' });
    }

    const team = {
        id: Date.now(),
        name,
        ownerName: ownerName || '',
        budget: INITIAL_BUDGET,
        players: []
    };

    teams.push(team);
    io.emit('teamsUpdated', teams);
    res.json(team);
});

// Update player base price
app.post('/api/players/:id/price', (req, res) => {
    const id = parseInt(req.params.id);
    const { basePrice } = req.body;

    const playerIndex = players.findIndex(p => p.id === id);
    if (playerIndex !== -1) {
        players[playerIndex].basePrice = basePrice;
        io.emit('playersUpdated', players);
        res.json({ success: true });
    } else {
        res.status(404).json({ error: 'Player not found' });
    }
});

// Get all players
app.get('/api/players', (req, res) => {
    res.json(players);
});

// Delete team
app.delete('/api/teams/:id', (req, res) => {
    const id = parseInt(req.params.id);
    teams = teams.filter(t => t.id !== id);
    io.emit('teamsUpdated', teams);
    res.json({ success: true });
});

// Reset auction
app.post('/api/reset', (req, res) => {
    // Reset all players
    players = players.map(p => ({ ...p, status: 'available', soldTo: null, soldPrice: null }));

    // Reset teams
    teams = teams.map(t => ({ ...t, budget: INITIAL_BUDGET, players: [] }));

    // Reset auction state
    auctionState = {
        status: 'waiting',
        currentPlayer: null,
        currentBid: 0,
        currentBidder: null,
        soldPlayers: [],
        unsoldPlayers: []
    };

    io.emit('fullUpdate', { teams, players, auctionState });
    res.json({ success: true });
});

// ============ SOCKET.IO EVENTS ============
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);

    // Send current state on connect
    socket.emit('fullUpdate', { teams, players, auctionState });

    // Admin: Select player for auction
    socket.on('selectPlayer', (playerId) => {
        const player = players.find(p => p.id === playerId);
        if (player && player.status === 'available') {
            auctionState.currentPlayer = player;
            auctionState.currentBid = player.basePrice;
            auctionState.currentBidder = null;
            auctionState.status = 'bidding';
            io.emit('auctionUpdate', auctionState);
        }
    });

    // Admin: Update bid
    socket.on('updateBid', ({ amount, teamId }) => {
        const team = teams.find(t => t.id === teamId);
        if (team && amount <= team.budget && auctionState.status === 'bidding') {
            auctionState.currentBid = amount;
            auctionState.currentBidder = team;
            io.emit('auctionUpdate', auctionState);
        }
    });

    // Admin: Mark as sold
    socket.on('markSold', () => {
        if (auctionState.currentPlayer && auctionState.currentBidder) {
            const player = auctionState.currentPlayer;
            const team = auctionState.currentBidder;
            const price = auctionState.currentBid;

            // Update player
            const playerIndex = players.findIndex(p => p.id === player.id);
            players[playerIndex].status = 'sold';
            players[playerIndex].soldTo = team.id;
            players[playerIndex].soldToName = team.name;
            players[playerIndex].soldPrice = price;

            // Update team
            const teamIndex = teams.findIndex(t => t.id === team.id);
            teams[teamIndex].budget -= price;
            teams[teamIndex].players.push({
                ...players[playerIndex],
                soldPrice: price
            });

            // Update auction state
            auctionState.soldPlayers.push({
                ...players[playerIndex],
                soldPrice: price,
                soldToName: team.name
            });

            auctionState.status = 'sold';

            io.emit('playerSold', {
                player: players[playerIndex],
                team: teams[teamIndex],
                price
            });

            io.emit('fullUpdate', { teams, players, auctionState });

            // Reset after 3 seconds
            setTimeout(() => {
                auctionState.status = 'waiting';
                auctionState.currentPlayer = null;
                auctionState.currentBid = 0;
                auctionState.currentBidder = null;
                io.emit('auctionUpdate', auctionState);
            }, 3000);
        }
    });

    // Admin: Mark as unsold
    socket.on('markUnsold', () => {
        if (auctionState.currentPlayer) {
            const player = auctionState.currentPlayer;

            // Update player
            const playerIndex = players.findIndex(p => p.id === player.id);
            players[playerIndex].status = 'unsold';

            // Update auction state
            auctionState.unsoldPlayers.push(players[playerIndex]);
            auctionState.status = 'unsold';

            io.emit('fullUpdate', { teams, players, auctionState });

            // Reset after 2 seconds
            setTimeout(() => {
                auctionState.status = 'waiting';
                auctionState.currentPlayer = null;
                auctionState.currentBid = 0;
                auctionState.currentBidder = null;
                io.emit('auctionUpdate', auctionState);
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
    ║  Admin Password: ${ADMIN_PASSWORD.padEnd(20)}║
    ╚═══════════════════════════════════════╝

    Open http://localhost:${PORT} in your browser
    `);
});
