# bawdash

Web dashboard for Binance Agentic Wallet (BAW) sessions and on-chain activity on BNB Smart Chain.

## Features

- **Session & Limits**: Active session countdown, daily quota tracking, and dev mode status.
- **Holdings & Activity**: Token balances, valuations, and recent transaction log.
- **NFT Tracking**: Single-row overview and gallery view with hold/sold status and multi-token modal support.
- **Metadata & Links**: Resolves token URIs via on-chain calls, IPFS gateways, and dApp sites.
- **Realized PnL Card**: 16:9 performance breakdown for closed NFT sales. Includes direct clipboard image copy and PNG export.
- **Privacy Mode**: Toggle to blur wallet balances, token amounts, and USD values.

## Stack

- **Backend**: FastAPI, Uvicorn, httpx
- **Frontend**: Vanilla JS, HTML/CSS, Canvas API
- **RPC**: BSC Mainnet

## Setup

```bash
git clone https://github.com/c1gs4/bawdash.git
cd bawdash

python3 -m venv .venv
source .venv/bin/activate
pip install fastapi uvicorn httpx pillow

# Run checks
python tests/selfcheck.py

# Start server
uvicorn app.main:app --host 127.0.0.1 --port 4331
```

## Service

Systemd service file available at `deploy/bawdash.service`.
