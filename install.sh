#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://github.com/beaudenison/streambox.git"
PROJECT_DIR="${PROJECT_DIR:-$PWD}"

has_cmd() {
  command -v "$1" >/dev/null 2>&1
}

run_sudo() {
  if [[ "${EUID}" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

install_docker_if_missing() {
  if has_cmd docker; then
    return
  fi

  echo "[streambox] Docker not found. Installing Docker..."
  curl -fsSL https://get.docker.com | run_sudo sh

  if [[ "${EUID}" -ne 0 ]]; then
    run_sudo usermod -aG docker "$USER" || true
    echo "[streambox] Added ${USER} to docker group. You may need to re-login for group changes."
  fi
}

install_compose_if_missing() {
  if docker compose version >/dev/null 2>&1; then
    return
  fi

  echo "[streambox] Docker Compose plugin not found. Installing..."
  run_sudo apt-get update
  run_sudo apt-get install -y docker-compose-plugin
}

prepare_project() {
  if [[ -f "${PROJECT_DIR}/docker-compose.yml" ]]; then
    cd "${PROJECT_DIR}"
    return
  fi

  local target="${PROJECT_DIR}/streambox"
  if [[ ! -d "${target}" ]]; then
    echo "[streambox] Cloning repository into ${target}"
    git clone "${REPO_URL}" "${target}"
  fi

  cd "${target}"
}

detect_host_ip() {
  local ip
  # Try outbound-route IP first (most reliable on a server)
  ip=$(ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src") {print $(i+1); exit}}')

  # Fallback: first non-loopback IPv4 via hostname -I
  if [[ -z "${ip}" ]]; then
    ip=$(hostname -I 2>/dev/null | awk '{print $1}')
  fi

  # Last resort: localhost
  echo "${ip:-localhost}"
}

start_stack() {
  echo "[streambox] Building and starting containers..."
  docker compose up -d --build
}

print_summary() {
  local host="${1}"
  local dashboard="http://${host}:8080"
  local rtmp_ingest="rtmp://${host}/ingest"

  echo
  echo "╔══════════════════════════════════════════════╗"
  echo "║           streambox is running!              ║"
  echo "╠══════════════════════════════════════════════╣"
  printf "║  Dashboard:   %-32s║\n" "${dashboard}"
  printf "║  RTMP ingest: %-32s║\n" "${rtmp_ingest}"
  printf "║  OBS server:  %-32s║\n" "${rtmp_ingest}"
  echo "╚══════════════════════════════════════════════╝"
  echo
  echo "  Click or copy the Dashboard link above to open the control panel."
  echo "  In OBS: Server = ${rtmp_ingest}"
  echo "          Stream Key = <your custom key>"
  echo
}

main() {
  if ! has_cmd curl; then
    run_sudo apt-get update
    run_sudo apt-get install -y curl
  fi

  install_docker_if_missing
  install_compose_if_missing
  prepare_project

  local host
  host=$(detect_host_ip)
  export PUBLIC_HOST="${host}"

  start_stack
  print_summary "${host}"
}

main "$@"
