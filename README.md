# Get Sepolia ETH
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A lightweight self-hosted service for obtaining Ethereum Sepolia ETH.

https://getsepolia.2bd.net/

This repository contains the software powering the GetSepolia service.
No warranty is provided regarding availability of the public service,
faucet funding, RPC endpoints or blockchain infrastructure.

## Features

-   Free Sepolia faucet
-   Instant USDC→Sepolia ETH delivery
-   Automatic payment verification
-   Automatic payout worker
-   Inventory management
-   Order tracking
-   EIP-6963 wallet support
-   SQLite + FastAPI backend

## Architecture

    Browser
       │
       ▼
    FastAPI
       ├── SQLite
       ├── Arbitrum RPC
       └── Sepolia RPC
             │
             ▼
       payout_worker.py

## Installation

``` bash
git clone https://github.com/<YOUR_GITHUB_USERNAME>/getsepolia.git
cd getsepolia

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` from `.env.example`.
``` bash
cp .env.example .env
```

Edit the required variables:

``` bash
SEPOLIA_PAYOUT_PRIVATE_KEY
SEPOLIA_PAYOUT_ADDRESS
USDC_TREASURY_ADDRESS
```
Then start the API and payout worker.

Start API:

``` bash
uvicorn backend.main:app --host 127.0.0.1 --port 8010
```

Run payout worker:

``` bash
python -m backend.payout_worker
```

## Faucet

-   Free faucet mode
-   24 h cooldown
-   Recipient rate limit
-   Hashed IP rate limiting
-   Claim status endpoint

## Security

-   Native USDC verification
-   Exact sender verification
-   Exact amount verification
-   Duplicate payment protection
-   Automatic payout recovery

## License

MIT
