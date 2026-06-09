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

start_stack() {
  echo "[streambox] Building and starting containers..."
  docker compose up -d --build
}

main() {
  if ! has_cmd curl; then
    run_sudo apt-get update
    run_sudo apt-get install -y curl
  fi

  install_docker_if_missing
  install_compose_if_missing
  prepare_project
  start_stack

  echo
  echo "[streambox] Ready."
  echo "Dashboard: http://localhost:8080"
  echo "RTMP ingest: rtmp://localhost/ingest"
  echo "RTMP test endpoint: http://localhost:8081"
}

main "$@"
