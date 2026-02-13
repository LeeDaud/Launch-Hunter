# Backend On Linux + Frontend On Vercel

## 1) Clone and install

```bash
git clone <YOUR_REPO_URL> app
cd app
npm ci
mkdir -p backend/data
touch .env
```

Edit `.env` and set:

- `FRONTEND_ORIGIN=https://your-frontend.vercel.app`
- `BASE_HTTP_RPC=...`
- `BASE_WS_RPC=...` (optional)
- `VIRTUAL_CA=...`

## 2) Start backend with pm2

```bash
npm i -g pm2
pm2 start npm --name virtuals-backend -- run start
pm2 save
pm2 startup systemd -u $USER --hp /home/$USER
```

## 3) Nginx reverse proxy (SSE-ready)

Create `/etc/nginx/sites-available/virtuals-backend.conf`:

```nginx
server {
    listen 80;
    server_name api.your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_buffering off;
        proxy_read_timeout 3600s;
        add_header Cache-Control no-cache;
    }
}
```

Enable and reload:

```bash
sudo ln -s /etc/nginx/sites-available/virtuals-backend.conf /etc/nginx/sites-enabled/virtuals-backend.conf
sudo nginx -t
sudo systemctl reload nginx
```

## 4) HTTPS with certbot

```bash
sudo apt update
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d api.your-domain.com
sudo systemctl status certbot.timer
```

## 5) Vercel frontend env

In Vercel project settings, set:

- `API_BASE=https://api.your-domain.com`

Then redeploy frontend.
