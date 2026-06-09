# streambox

Self-hosted, Dockerized RTMP routing platform with a web control plane.

## Features

- FastAPI backend for orchestration and persisted state.
- NGINX-RTMP core for ingest and internal matrix stream transport.
- Dark-mode SPA dashboard (Tailwind CSS) for:
  - Input management (custom OBS stream keys)
  - Output management (Twitch/Kick/YouTube/custom RTMP)
  - Live matrix switching (active input to global outputs)
  - Routing start/stop controls
- Docker Compose deployment with persistent local state.
- `install.sh` for single-line bootstrap.

## Quick Start

From this repository:

```bash
./install.sh
```

One-line installer (remote):

```bash
curl -fsSL https://raw.githubusercontent.com/beaudenison/streambox/main/install.sh | bash
```

After startup:

- Dashboard: `http://localhost:8080`
- OBS ingest server: `rtmp://localhost/ingest`
- RTMP status endpoint: `http://localhost:8081`

## How Routing Works

1. OBS publishers stream to `rtmp://<host>/ingest` with the input's custom stream key.
2. When routing starts, backend launches one FFmpeg router process:
	- Source: `rtmp://rtmp:1935/ingest/<active_input_key>`
	- Target: `rtmp://rtmp:1935/matrix/live`
3. For each enabled output, backend launches one FFmpeg worker:
	- Source: `rtmp://rtmp:1935/matrix/live`
	- Target: `<output_ingest_url>/<output_stream_key>`
4. Switching the active input restarts only the internal router process, so OBS and output configuration remain unchanged.

## Project Layout

```text
.
├── Dockerfile
├── docker-compose.yml
├── install.sh
├── main.py
├── requirements.txt
├── rtmp/
│   └── nginx.conf
├── static/
│   └── index.html
└── data/
	 └── .gitkeep
```

## Notes

- State is persisted in `./data/state.json`.
- Set `PUBLIC_HOST` in your shell before `docker compose up` if clients connect by non-localhost hostname/IP.
- This is a practical boilerplate for live routing and control; platform-level continuity depends on ingest stability and destination timeout behavior.