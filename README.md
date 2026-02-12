# Virtuals Realtime Assistant

This repo is now split for deployment:

- `backend/`: Node.js + Express + SSE service (run on your Linux server with pm2)
- `frontend/`: static site (deploy to Vercel)

## Project structure

```text
.
|- backend/                      # Node.js backend service
|  |- src/                       # Express API, tracker, DB, rules
|  \- scripts/                   # utility scripts (e.g. replay)
|- frontend/                     # static frontend app (for Vercel)
|  |- public/                    # index.html and static assets
|  |- build.mjs                  # inject API_BASE into runtime-config.js
|  |- package.json
|  \- vercel.json
|- .env.example
|- DEPLOY_LINUX_PM2_NGINX.md
|- TECHNICAL_DOC.md
\- package.json
```

## Backend run

1. Copy env and configure:

```bash
cp .env.example .env
```

Required key envs:

- `BASE_HTTP_RPC`
- `VIRTUAL_CA`
- `FRONTEND_ORIGIN`

Optional:

- `BASE_WS_RPC`

2. Start:

```bash
npm install
npm run start
```

Backend listens on `0.0.0.0:${PORT}` (default `3000`).

## Health check

- `GET /healthz` -> `ok`

## Frontend API base

Frontend reads runtime config from `window.__API_BASE__`.

- Local fallback: `frontend/public/runtime-config.js`
- Vercel build injects env `API_BASE` into generated `dist/runtime-config.js`

Deploy frontend directory with Vercel, set env:

- `API_BASE=https://api.your-domain.com`

## Full deployment guide

See `DEPLOY_LINUX_PM2_NGINX.md` for pm2 + nginx + certbot commands.
