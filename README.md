# IPL 2026 Fantasy Auction

A real-time fantasy auction platform for IPL 2026. Perfect for conducting player auctions among friends during video calls.

## Features
- Real-time updates using Socket.io
- Admin panel for auction control
- Team budget management
- 100 IPL players pre-loaded
- Beautiful dark-mode UI

## Quick Start

```bash
npm install
npm start
```

Open http://localhost:3000

**Admin Password:** `ipl2026` (set via `ADMIN_PASSWORD` env variable)

## Deployment

### Railway
1. Connect your GitHub repo to Railway
2. Set environment variable: `ADMIN_PASSWORD=your_secret_password`
3. Deploy!

## How to Use

1. **Add Teams**: Admin adds team names (your friends' team names)
2. **Start Auction**: Select a player and start bidding
3. **Record Bids**: Update bid amount and select bidding team
4. **Mark Sold/Unsold**: Finalize each player's sale
5. **Watch Live**: Friends open the same URL to watch real-time updates

## Tech Stack
- Node.js + Express
- Socket.io for real-time
- Vanilla JavaScript frontend
