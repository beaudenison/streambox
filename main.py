import asyncio
import atexit
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


STATE_PATH = Path(os.getenv("STATE_PATH", "/data/state.json"))
RTMP_HOST = os.getenv("RTMP_HOST", "rtmp")
RTMP_PORT = int(os.getenv("RTMP_PORT", "1935"))
PUBLIC_HOST = os.getenv("PUBLIC_HOST", "localhost")
INGEST_APP = "ingest"
MATRIX_APP = "matrix"
MATRIX_STREAM = "live"


class InputCreate(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    stream_key: str = Field(min_length=3, max_length=128)


class OutputCreate(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    ingest_url: str = Field(min_length=1, max_length=512)
    stream_key: str = Field(min_length=1, max_length=256)
    enabled: bool = True


class OutputUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=64)
    ingest_url: Optional[str] = Field(default=None, min_length=1, max_length=512)
    stream_key: Optional[str] = Field(default=None, min_length=1, max_length=256)
    enabled: Optional[bool] = None


class ActiveInputSelect(BaseModel):
    input_id: str


class InputRouteUpdate(BaseModel):
    input_id: str
    output_id: str
    selected: bool


class Runtime:
    def __init__(self) -> None:
        self.output_procs: Dict[str, subprocess.Popen[Any]] = {}
        self.lock = asyncio.Lock()
        self.input_status_cache: Dict[str, Dict[str, Any]] = {}
        self.input_status_cache_at: float = 0.0


runtime = Runtime()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_state() -> Dict[str, Any]:
    return {
        "inputs": [],
        "outputs": [],
        "active_input_id": None,
        "input_output_routes": {},
        "routing_enabled": False,
        "updated_at": now_iso(),
    }


def ensure_state_file() -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not STATE_PATH.exists():
        STATE_PATH.write_text(json.dumps(default_state(), indent=2), encoding="utf-8")


def load_state() -> Dict[str, Any]:
    ensure_state_file()
    with STATE_PATH.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    merged = default_state()
    merged.update(data)
    merged["inputs"] = data.get("inputs", [])
    merged["outputs"] = data.get("outputs", [])
    merged["input_output_routes"] = data.get("input_output_routes", {})
    normalize_routes(merged)
    return merged


def save_state(state: Dict[str, Any]) -> None:
    state["updated_at"] = now_iso()
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def process_alive(proc: Optional[subprocess.Popen[Any]]) -> bool:
    return proc is not None and proc.poll() is None


def terminate_process(proc: Optional[subprocess.Popen[Any]], grace_seconds: float = 3.0) -> None:
    if not process_alive(proc):
        return

    assert proc is not None
    proc.terminate()
    try:
        proc.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()


def find_input(state: Dict[str, Any], input_id: str) -> Dict[str, Any]:
    for item in state["inputs"]:
        if item["id"] == input_id:
            return item
    raise HTTPException(status_code=404, detail="Input not found")


def find_output(state: Dict[str, Any], output_id: str) -> Dict[str, Any]:
    for item in state["outputs"]:
        if item["id"] == output_id:
            return item
    raise HTTPException(status_code=404, detail="Output not found")


def ingest_url_for_key(stream_key: str) -> str:
    return f"rtmp://{PUBLIC_HOST}/{INGEST_APP}"


def input_source_url(stream_key: str) -> str:
    return f"rtmp://{RTMP_HOST}:{RTMP_PORT}/{INGEST_APP}/{stream_key}"


def output_target_url(output: Dict[str, Any]) -> str:
    base = output["ingest_url"].strip().rstrip("/")
    key = output["stream_key"].strip()
    return f"{base}/{key}"


def parse_frame_rate(raw: str) -> Optional[float]:
    if not raw or raw == "0/0":
        return None

    if "/" in raw:
        num_s, den_s = raw.split("/", 1)
        try:
            num = float(num_s)
            den = float(den_s)
            if den == 0:
                return None
            return round(num / den, 2)
        except ValueError:
            return None

    try:
        return round(float(raw), 2)
    except ValueError:
        return None


def parse_bitrate_kbps(value: Any) -> Optional[int]:
    if value is None:
        return None

    try:
        return int(int(value) / 1000)
    except (TypeError, ValueError):
        return None


def probe_input_stream(input_item: Dict[str, Any]) -> Dict[str, Any]:
    source = input_source_url(input_item["stream_key"])
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-analyzeduration",
        "1000000",
        "-probesize",
        "32768",
        "-show_entries",
        "stream=codec_type,codec_name,width,height,avg_frame_rate,bit_rate",
        "-show_entries",
        "format=bit_rate",
        "-of",
        "json",
        source,
    ]

    try:
        completed = subprocess.run(cmd, capture_output=True, text=True, timeout=2.0, check=False)
    except subprocess.TimeoutExpired:
        return {
            "online": False,
            "reason": "Probe timeout",
        }

    if completed.returncode != 0:
        reason = completed.stderr.strip() or "No stream detected"
        return {
            "online": False,
            "reason": reason,
        }

    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError:
        return {
            "online": False,
            "reason": "Probe parse error",
        }

    streams: List[Dict[str, Any]] = payload.get("streams", [])
    if not streams:
        return {
            "online": False,
            "reason": "No active media stream",
        }

    video_stream = next((x for x in streams if x.get("codec_type") == "video"), None)
    primary_stream = video_stream or streams[0]
    fmt = payload.get("format", {})

    bitrate_kbps = parse_bitrate_kbps(fmt.get("bit_rate"))
    if bitrate_kbps is None:
        bitrate_kbps = parse_bitrate_kbps(primary_stream.get("bit_rate"))

    width = primary_stream.get("width")
    height = primary_stream.get("height")
    resolution = f"{width}x{height}" if width and height else None

    return {
        "online": True,
        "bitrate_kbps": bitrate_kbps,
        "codec": primary_stream.get("codec_name"),
        "fps": parse_frame_rate(str(primary_stream.get("avg_frame_rate", ""))),
        "resolution": resolution,
        "last_seen": now_iso(),
    }


async def collect_input_status(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    now_mono = time.monotonic()
    if now_mono - runtime.input_status_cache_at < 2.5:
        return runtime.input_status_cache

    inputs = state.get("inputs", [])
    if not inputs:
        runtime.input_status_cache = {}
        runtime.input_status_cache_at = now_mono
        return {}

    tasks = [asyncio.to_thread(probe_input_stream, item) for item in inputs]
    results = await asyncio.gather(*tasks)

    merged: Dict[str, Dict[str, Any]] = {}
    previous = runtime.input_status_cache
    for input_item, status in zip(inputs, results):
        existing = previous.get(input_item["id"], {})
        if not status.get("online") and existing.get("last_seen"):
            status["last_seen"] = existing.get("last_seen")
        merged[input_item["id"]] = status

    runtime.input_status_cache = merged
    runtime.input_status_cache_at = now_mono
    return merged


def normalize_routes(state: Dict[str, Any]) -> None:
    valid_input_ids = {item["id"] for item in state["inputs"]}
    valid_output_ids = {item["id"] for item in state["outputs"]}
    raw_routes = state.get("input_output_routes", {})
    normalized: Dict[str, List[str]] = {}

    for input_id in valid_input_ids:
        requested = raw_routes.get(input_id, [])
        cleaned: List[str] = []
        for output_id in requested:
            if output_id in valid_output_ids and output_id not in cleaned:
                cleaned.append(output_id)
        normalized[input_id] = cleaned

    state["input_output_routes"] = normalized


def assigned_input_for_output(state: Dict[str, Any], output_id: str) -> Optional[Dict[str, Any]]:
    routes = state.get("input_output_routes", {})
    for input_item in state["inputs"]:
        if output_id in routes.get(input_item["id"], []):
            return input_item
    return None


def assign_output_route(state: Dict[str, Any], input_id: str, output_id: str, selected: bool) -> None:
    routes: Dict[str, List[str]] = state.get("input_output_routes", {})
    for candidate_input_id in list(routes.keys()):
        routes[candidate_input_id] = [x for x in routes.get(candidate_input_id, []) if x != output_id]

    routes.setdefault(input_id, [])
    if selected and output_id not in routes[input_id]:
        routes[input_id].append(output_id)

    state["input_output_routes"] = routes


def start_output_worker(output: Dict[str, Any], input_item: Dict[str, Any]) -> None:
    output_id = output["id"]
    existing = runtime.output_procs.get(output_id)
    terminate_process(existing)

    source = input_source_url(input_item["stream_key"])
    target = output_target_url(output)

    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-reconnect",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_on_network_error",
        "1",
        "-reconnect_delay_max",
        "2",
        "-i",
        source,
        "-c",
        "copy",
        "-f",
        "flv",
        target,
    ]
    runtime.output_procs[output_id] = subprocess.Popen(cmd)


def stop_output_worker(output_id: str) -> None:
    proc = runtime.output_procs.pop(output_id, None)
    terminate_process(proc)


def stop_all_routing() -> None:
    for output_id in list(runtime.output_procs.keys()):
        stop_output_worker(output_id)


def reconcile_output_workers(state: Dict[str, Any]) -> None:
    if not state.get("routing_enabled", False):
        for output_id in list(runtime.output_procs.keys()):
            stop_output_worker(output_id)
        return

    desired_output_ids: set[str] = set()
    for output in state["outputs"]:
        if not output.get("enabled", False):
            continue
        source_input = assigned_input_for_output(state, output["id"])
        if source_input is None:
            continue

        desired_output_ids.add(output["id"])
        start_output_worker(output, source_input)

    for output_id in list(runtime.output_procs.keys()):
        if output_id not in desired_output_ids:
            stop_output_worker(output_id)


def runtime_status_snapshot(state: Dict[str, Any]) -> Dict[str, Any]:
    alive_outputs: Dict[str, bool] = {}
    for output in state["outputs"]:
        proc = runtime.output_procs.get(output["id"])
        alive_outputs[output["id"]] = process_alive(proc)

    return {
        "router_running": False,
        "output_workers": alive_outputs,
        "routing_enabled": state.get("routing_enabled", False),
    }


def cleanup_processes() -> None:
    stop_all_routing()


atexit.register(cleanup_processes)


app = FastAPI(title="streambox", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup() -> None:
    state = load_state()
    changed = False
    # Migration: remove legacy synthetic default input and keep only user-defined inputs.
    default_ids = {
        item["id"]
        for item in state["inputs"]
        if item.get("name") == "Default_Input" and item.get("stream_key") == "default_input"
    }
    if default_ids:
        state["inputs"] = [item for item in state["inputs"] if item["id"] not in default_ids]
        for input_id in default_ids:
            state.get("input_output_routes", {}).pop(input_id, None)
        changed = True

    if state.get("active_input_id") and not any(x["id"] == state["active_input_id"] for x in state["inputs"]):
        state["active_input_id"] = state["inputs"][0]["id"] if state["inputs"] else None
        changed = True

    if not state["inputs"] and state.get("routing_enabled"):
        state["routing_enabled"] = False
        changed = True

    before_routes = json.dumps(state.get("input_output_routes", {}), sort_keys=True)
    normalize_routes(state)
    after_routes = json.dumps(state.get("input_output_routes", {}), sort_keys=True)
    if before_routes != after_routes:
        changed = True

    if changed:
        save_state(state)


@app.get("/api/state")
async def get_state() -> Dict[str, Any]:
    state = load_state()
    runtime_snapshot = runtime_status_snapshot(state)
    runtime_snapshot["input_status"] = await collect_input_status(state)
    return {
        "state": state,
        "runtime": runtime_snapshot,
        "obs_server": f"rtmp://{PUBLIC_HOST}/{INGEST_APP}",
    }


@app.post("/api/inputs")
async def create_input(payload: InputCreate) -> Dict[str, Any]:
    async with runtime.lock:
        state = load_state()

        if any(x["stream_key"] == payload.stream_key for x in state["inputs"]):
            raise HTTPException(status_code=409, detail="stream_key already exists")

        created = {
            "id": str(uuid4()),
            "name": payload.name.strip(),
            "stream_key": payload.stream_key.strip(),
            "created_at": now_iso(),
        }
        state["inputs"].append(created)
        state.setdefault("input_output_routes", {})[created["id"]] = []

        if not state.get("active_input_id"):
            state["active_input_id"] = created["id"]

        save_state(state)
        return {
            "input": created,
            "obs": {
                "server": ingest_url_for_key(created["stream_key"]),
                "stream_key": created["stream_key"],
            },
        }


@app.delete("/api/inputs/{input_id}")
async def delete_input(input_id: str) -> Dict[str, Any]:
    async with runtime.lock:
        state = load_state()
        existing = find_input(state, input_id)
        state["inputs"] = [x for x in state["inputs"] if x["id"] != input_id]
        state.setdefault("input_output_routes", {}).pop(input_id, None)

        if state.get("active_input_id") == input_id:
            state["active_input_id"] = state["inputs"][0]["id"] if state["inputs"] else None
        normalize_routes(state)
        reconcile_output_workers(state)

        save_state(state)
        return {"deleted": existing["id"]}


@app.post("/api/outputs")
async def create_output(payload: OutputCreate) -> Dict[str, Any]:
    async with runtime.lock:
        state = load_state()
        created = {
            "id": str(uuid4()),
            "name": payload.name.strip(),
            "ingest_url": payload.ingest_url.strip(),
            "stream_key": payload.stream_key.strip(),
            "enabled": payload.enabled,
            "created_at": now_iso(),
        }
        state["outputs"].append(created)
        save_state(state)
        reconcile_output_workers(state)

        return {"output": created}


@app.patch("/api/outputs/{output_id}")
async def update_output(output_id: str, payload: OutputUpdate) -> Dict[str, Any]:
    async with runtime.lock:
        state = load_state()
        output = find_output(state, output_id)

        if payload.name is not None:
            output["name"] = payload.name.strip()
        if payload.ingest_url is not None:
            output["ingest_url"] = payload.ingest_url.strip()
        if payload.stream_key is not None:
            output["stream_key"] = payload.stream_key.strip()
        if payload.enabled is not None:
            output["enabled"] = payload.enabled

        save_state(state)
        reconcile_output_workers(state)

        return {"output": output}


@app.delete("/api/outputs/{output_id}")
async def delete_output(output_id: str) -> Dict[str, Any]:
    async with runtime.lock:
        state = load_state()
        existing = find_output(state, output_id)
        state["outputs"] = [x for x in state["outputs"] if x["id"] != output_id]
        for input_id, output_ids in state.get("input_output_routes", {}).items():
            state["input_output_routes"][input_id] = [x for x in output_ids if x != output_id]
        save_state(state)
        stop_output_worker(output_id)
        reconcile_output_workers(state)
        return {"deleted": existing["id"]}


@app.post("/api/control/select-input")
async def select_active_input(payload: ActiveInputSelect) -> Dict[str, Any]:
    async with runtime.lock:
        state = load_state()
        selected = find_input(state, payload.input_id)
        state["active_input_id"] = selected["id"]
        for output in state["outputs"]:
            if output.get("enabled", False):
                assign_output_route(state, selected["id"], output["id"], True)

        save_state(state)
        reconcile_output_workers(state)
        return {"active_input_id": selected["id"]}


@app.post("/api/control/routes")
async def set_input_output_route(payload: InputRouteUpdate) -> Dict[str, Any]:
    async with runtime.lock:
        state = load_state()
        find_input(state, payload.input_id)
        find_output(state, payload.output_id)

        assign_output_route(state, payload.input_id, payload.output_id, payload.selected)
        state["active_input_id"] = payload.input_id
        save_state(state)
        reconcile_output_workers(state)
        return {"updated": True}


@app.post("/api/control/start")
async def start_routing() -> Dict[str, Any]:
    async with runtime.lock:
        state = load_state()
        state["routing_enabled"] = True
        save_state(state)
        reconcile_output_workers(state)
        return {"started": True}


@app.post("/api/control/stop")
async def stop_routing() -> Dict[str, Any]:
    async with runtime.lock:
        state = load_state()
        stop_all_routing()
        state["routing_enabled"] = False
        save_state(state)
        return {"stopped": True}


@app.get("/healthz")
async def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/")
async def index() -> FileResponse:
    return FileResponse("static/index.html")


app.mount("/static", StaticFiles(directory="static"), name="static")
