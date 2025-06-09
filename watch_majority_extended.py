#!/usr/bin/env python3
"""
watch_majority_extended.py

Monitors multiple Docker containers (“miners”) and:
1) Checks “Latest ID / Latest State” across all containers; if any warmed-up container lags
   behind the majority by ≥ grace_period, dumps its last 500 logs and restarts it.
2) Logs ⭐ markers when this miner’s address appears in “Assigned Miners:” during State 1 or 2.
3) When a State-3+Assigned TX is detected, waits 5 s, then polls its receipt every 5 s
   until success/failure or timeout. If it succeeds, moves on to State 4; if it fails or times out,
   dumps logs and (after warm-up) restarts.
4) Similarly for State-4+Assigned TX. Each container keeps a “last_tx_message” that is displayed
   beneath its “Latest ID / Latest State” line until overwritten. Whenever the “Latest ID” changes,
   we re-print that last TX message so you can see it for the new session—and we reset the TX-FSM
   so that State 3 detection will run again.
5) **If the majority state == 6 (SessionEnded), we verify all configured containers are running.
   Any container that is missing or not “running” is dumped (last 500 logs) and restarted.**
6) **If “Pinging network…” appears ≥ 2 times within the last 52 lines of logs, we immediately dump
   its last 500 lines and restart.**

“Warm-up” is defined as 3 minutes (180 s) from when this watcher script starts.
After 3 minutes have elapsed, all containers are considered warmed-up, regardless of TX success.

Before any restart, saves the last 500 log lines to:
    restart_logs/<container>_<reason>_<timestamp>.log
and appends one line to watcher.log.

config.json (in the same folder) must contain:
{
  "containers": ["cortensor-1","cortensor-2", …],
  "node_addresses": {
    "cortensor-1": "0x....",
    "cortensor-2": "0x.....",
    …
  },
  "tail_lines": 60,
  "check_interval_seconds": 2.5,
  "grace_period_seconds": 30,
  "rpc_url": "https://arb-sep.scerb.uk",
  "watch_tx_for_containers": ["cortensor-1","cortensor-2", …],
  "tx_timeout_seconds": 30
}
"""

import json
import re
import sys
import time
import os
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone, timedelta

import docker
import requests

# ─────────────────────────────────────────────────────────────────────────────
def load_config(path="config.json"):
    try:
        raw = Path(path).read_text()
        cfg = json.loads(raw)
    except Exception as e:
        print(f"Error reading '{path}': {e}", file=sys.stderr)
        sys.exit(1)

    required = [
        ("containers", list),
        ("node_addresses", dict),
        ("tail_lines", int),
        ("check_interval_seconds", (int, float)),
        ("grace_period_seconds", (int, float)),
        ("rpc_url", str),
        ("watch_tx_for_containers", list),
        ("tx_timeout_seconds", (int, float)),
    ]
    for key, expected in required:
        if key not in cfg or not isinstance(cfg[key], expected):
            print(f"Config error: '{key}' missing or not of type {expected}", file=sys.stderr)
            sys.exit(1)

    for cid in cfg["containers"]:
        if cid not in cfg["node_addresses"]:
            print(f"Config error: no node_address for container '{cid}'", file=sys.stderr)
            sys.exit(1)

    return cfg

# ─────────────────────────────────────────────────────────────────────────────
# Precompile regexes
LOG_STATE_RE       = re.compile(r"Latest ID:\s*(\d+)\s*/\s*Latest State:\s*(\d+)")
ASSIGNED_RE        = re.compile(r"Assigned Miners:\s*(.*)")
TX_RE              = re.compile(r"TX:\s*(0x[0-9a-fA-F]+)")
LOG_EVENT_RE       = re.compile(r"\* Event:\s*(\S+)")
PING_FAIL_PATTERN  = "Load ping thresholds..."

def rpc_get_receipt(rpc_url: str, tx_hash: str, timeout: float = 5.0):
    """
    Single JSON-RPC call to eth_getTransactionReceipt.
    Returns the parsed JSON “result” field (a dict) if found,
    or None if the node replies but the “receipt” is still null,
    or None on network errors.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getTransactionReceipt",
        "params": [tx_hash],
    }
    try:
        r = requests.post(rpc_url, json=payload, headers={"Content-Type": "application/json"}, timeout=timeout)
        r.raise_for_status()
        return r.json().get("result", None)
    except Exception:
        return None

def find_latest_assigned_stage(log_lines: list[str], miner_addr: str, state_code: int, lookahead: int = 20):
    idx_found = None
    for idx, ln in enumerate(log_lines):
        m = LOG_STATE_RE.search(ln)
        if m and int(m.group(2)) == state_code:
            for j in range(idx + 1, min(idx + 1 + lookahead, len(log_lines))):
                ma = ASSIGNED_RE.search(log_lines[j])
                if ma:
                    assigned = [a.strip().lower() for a in ma.group(1).split(",")]
                    if miner_addr.lower() in assigned:
                        idx_found = idx
    return idx_found

def find_first_tx_after(log_lines: list[str], start_idx: int):
    for ln in log_lines[start_idx:]:
        m = TX_RE.search(ln)
        if m:
            return m.group(1)
    return None

# ─────────────────────────────────────────────────────────────────────────────
def main():
    cfg = load_config("config.json")
    containers          = cfg["containers"]
    node_addresses      = cfg["node_addresses"]
    tail_lines          = cfg["tail_lines"]
    interval            = float(cfg["check_interval_seconds"])
    grace               = float(cfg["grace_period_seconds"])
    rpc_url             = cfg["rpc_url"]
    watch_tx_containers = set(cfg["watch_tx_for_containers"])
    tx_timeout          = float(cfg["tx_timeout_seconds"])

    # Record when this watcher started; define warm-up as 3 minutes from now
    start_time = datetime.now(timezone.utc)
    WARMUP_SECONDS = 3 * 60  # 3 minutes

    # ─── Version check initialization ─────────────────────────────────────────
    local_version      = "v1.0.2"
    version_api        = "https://api.github.com/repos/scerb/node_watch/releases/latest"
    last_version_check = start_time - timedelta(days=1)  # force initial check
    remote_version     = ""
    version_status     = ""

    # Initial version fetch
    try:
        resp = requests.get(version_api, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        remote_version = data.get("tag_name", "") or ""
    except Exception:
        remote_version = ""
    if remote_version == local_version:
        version_status = "latest"
    else:
        version_status = f"please update ({remote_version})" if remote_version else "unknown"
    last_version_check = start_time
    # ─────────────────────────────────────────────────────────────────────────

    # Prepare output dirs & files
    log_dir = Path("restart_logs"); log_dir.mkdir(exist_ok=True)
    master_log = Path("watcher.log")
    if not master_log.exists():
        master_log.write_text("")

    # Connect to Docker
    try:
        client = docker.from_env()
    except Exception as e:
        print(f"Error: cannot connect to Docker daemon: {e}", file=sys.stderr)
        sys.exit(1)

    # 1) Majority logic state
    first_deviation = {}

    # 2) Per-container TX-FSM state
    tx_state      = {cid: 0 for cid in containers}
    tx_seen       = {cid: {"pre": False, "commit": False} for cid in containers}
    tx_deadline_pre    = {cid: None for cid in containers}
    tx_deadline_commit = {cid: None for cid in containers}

    # 3) “Warmed-up” flags, now driven by time elapsed
    warmed_up = {cid: False for cid in containers}

    # 4) Pending TX checks: maps cid -> info dict
    pending_checks = {}

    # 5) Track last TX message per container
    last_tx_message = {cid: "(no TX yet)" for cid in containers}

    # 6) Track previous Latest ID per container, so we can detect “new session”
    prev_id_val = {cid: None for cid in containers}

    print("Starting watcher (majority + 5s-delayed TX checks; warm-up = 3 min)…")
    print("Press Ctrl+C to exit.\n")

    try:
        while True:
            os.system("clear")
            now = datetime.now(timezone.utc)

            # Re-check version every 24h
            if (now - last_version_check).total_seconds() >= 24 * 3600:
                try:
                    resp = requests.get(version_api, timeout=10)
                    resp.raise_for_status()
                    data = resp.json()
                    remote_version = data.get("tag_name", "") or ""
                except Exception:
                    remote_version = ""
                if remote_version == local_version:
                    version_status = "latest"
                else:
                    version_status = f"please update ({remote_version})" if remote_version else "unknown"
                last_version_check = now

            # Update warmed_up flags based solely on elapsed time
            elapsed_total = (now - start_time).total_seconds()
            if elapsed_total >= WARMUP_SECONDS:
                for cid in containers:
                    if not warmed_up[cid]:
                        warmed_up[cid] = True

            # Header now includes version info
            header = (
                f"=== {now.isoformat()} UTC  |  version {local_version} ({version_status})"
                f"  |  interval={interval}s  |  grace={grace}s"
                f"  |  warmed_up={int(elapsed_total)}s/180s ===\n"
            )
            print(header)

            # ── 1) Fetch logs for each container
            results = {}
            for cid in containers:
                try:
                    container = client.containers.get(cid)
                except docker.errors.NotFound:
                    continue
                except Exception as e:
                    print(f"[ERROR] Docker error retrieving '{cid}': {e}")
                    continue

                try:
                    raw_bytes = container.logs(tail=tail_lines, timestamps=False)
                    raw_text  = raw_bytes.decode("utf-8", errors="ignore")
                    log_lines = raw_text.splitlines()
                except Exception as e:
                    print(f"[ERROR] Failed to fetch logs for '{cid}': {e}")
                    continue

                recent_window = log_lines[-52:]
                ping_matches = sum(1 for ln in recent_window if ln.strip().startswith(PING_FAIL_PATTERN))
                if ping_matches >= 2:
                    ts_str = now.strftime("%Y%m%dT%H%M%S")
                    try:
                        pre_bytes = container.logs(tail=500, timestamps=False)
                        pre_text  = pre_bytes.decode("utf-8", errors="ignore")
                    except:
                        pre_text = "[ERROR capturing logs]\n"

                    outfile = log_dir / f"{cid}_pingfail_{ts_str}.log"
                    try:
                        with open(outfile, "w", encoding="utf-8") as wf:
                            wf.write(pre_text)
                    except Exception as e:
                        print(f"[{cid}] ✘ Could not write pingfail log: {e}")

                    entry = f"{now.isoformat()} UTC  Restarted '{cid}' (pingfail) → '{outfile.name}'\n"
                    try:
                        with open(master_log, "a", encoding="utf-8") as mf:
                            mf.write(entry)
                    except Exception as e:
                        print(f"[{cid}] ✘ Could not append to watcher.log: {e}")

                    try:
                        container.restart()
                        print(f"[{cid}] ✔ Restarted due to repeated “Pinging network…” (found {ping_matches} in last 52 lines)")
                    except Exception as e:
                        print(f"[{cid}] ✘ Restart failed: {e}")
                    continue

                last = None
                for ln in log_lines:
                    m = LOG_STATE_RE.search(ln)
                    if m:
                        last = (int(m.group(1)), int(m.group(2)))
                if last is not None:
                    results[cid] = (*last, raw_text, log_lines, container)

            # ── 2a) Majority logic
            if len(results) < 2:
                print("Not enough containers reporting. Sleeping…")
                time.sleep(interval)
                continue

            pair_list = [(v[0], v[1]) for v in results.values()]
            counts    = Counter(pair_list)
            majority_pair, majority_count = counts.most_common(1)[0]

            print(f"Majority (Latest ID, State) = {majority_pair}  →  {majority_count}/{len(results)} containers\n")

            # ── 2b) If majority state == 6, restart any inactive containers
            maj_state = majority_pair[1]
            if maj_state == 6:
                for cid in containers:
                    try:
                        c = client.containers.get(cid)
                    except docker.errors.NotFound:
                        try:
                            client.api.restart(cid)
                            note = f"{now.isoformat()} UTC  Restarted missing container '{cid}' (majority=6)\n"
                        except Exception as e:
                            note = f"{now.isoformat()} UTC  Failed restart missing '{cid}' (majority=6): {e}\n"
                        with open(master_log, "a", encoding="utf-8") as mf:
                            mf.write(note)
                        print(f"[{cid}] ✘ Not found → attempted restart")
                        continue

                    if c.status != "running":
                        try:
                            pre_bytes = c.logs(tail=500, timestamps=False)
                            pre_text  = pre_bytes.decode("utf-8", errors="ignore")
                        except:
                            pre_text = "[ERROR capturing logs]\n"
                        ts_str = now.strftime("%Y%m%dT%H%M%S")
                        outfile = log_dir / f"{cid}_inactive_{ts_str}.log"
                        try:
                            with open(outfile, "w", encoding="utf-8") as wf:
                                wf.write(pre_text)
                        except Exception as e:
                            print(f"[{cid}] ✘ Could not write inactive log: {e}")
                        try:
                            c.restart()
                            note = f"{now.isoformat()} UTC  Restarted '{cid}' (inactive, majority=6) → '{outfile.name}'\n"
                            with open(master_log, "a", encoding="utf-8") as mf:
                                mf.write(note)
                            print(f"[{cid}] ✔ Restarted inactive container (majority=6)")
                        except Exception as e:
                            print(f"[{cid}] ✘ Failed to restart inactive container: {e}")

            # ── 3) Display each container’s status line + last TX, and check for new session
            for cid in containers:
                if cid not in results:
                    print(f"[{cid}] (no data)")
                    print(f"    Last TX: {last_tx_message[cid]}")
                    continue

                id_val, state_val, raw_text, log_lines, container = results[cid]

                if prev_id_val[cid] is None:
                    prev_id_val[cid] = id_val
                elif prev_id_val[cid] != id_val:
                    print(f"[{cid}] → New session detected: ID changed {prev_id_val[cid]} → {id_val}")
                    print(f"    Last TX status: {last_tx_message[cid]}")
                    prev_id_val[cid] = id_val

                    tx_state[cid]           = 0
                    tx_seen[cid]["pre"]     = False
                    tx_seen[cid]["commit"]  = False
                    tx_deadline_pre[cid]    = None
                    tx_deadline_commit[cid] = None
                    pending_checks.pop(cid, None)

                this_pair = (id_val, state_val)
                if this_pair == majority_pair:
                    if cid in first_deviation:
                        del first_deviation[cid]
                    status_text = "✔ matches majority"
                else:
                    if cid not in first_deviation:
                        first_deviation[cid] = now
                        status_text = f"⚠ first deviation at {now.strftime('%H:%M:%S')}"
                    else:
                        elapsed = (now - first_deviation[cid]).total_seconds()
                        if elapsed >= grace:
                            if warmed_up[cid]:
                                status_text = f"↳ lagged {int(elapsed)}s ≥ {int(grace)}s → restarting"
                                try:
                                    pre_bytes = container.logs(tail=500, timestamps=False)
                                    pre_text  = pre_bytes.decode("utf-8", errors="ignore")
                                except:
                                    pre_text = "[ERROR capturing logs]\n"
                                ts_str = now.strftime("%Y%m%dT%H%M%S")
                                outfile = log_dir / f"{cid}_lag_{ts_str}.log"
                                try:
                                    with open(outfile, "w", encoding="utf-8") as wf:
                                        wf.write(pre_text)
                                except Exception as e:
                                    print(f"[{cid}] ✘ Could not write restart log: {e}")
                                entry = f"{now.isoformat()} UTC  Restarted '{cid}' (lag) → '{outfile.name}'\n"
                                try:
                                    with open(master_log, "a", encoding="utf-8") as mf:
                                        mf.write(entry)
                                except Exception as e:
                                    print(f"[{cid}] ✘ Could not append to watcher.log: {e}")
                                try:
                                    container.restart()
                                    print(f"[{cid}] ✔ Restarted due to majority lag")
                                except Exception as e:
                                    print(f"[{cid}] ✘ Restart failed: {e}")
                                del first_deviation[cid]
                            else:
                                status_text = f"⚠ lag detected but not warmed up → skipping restart"
                        else:
                            status_text = f"⚠ within grace: {int(elapsed)}s/<{int(grace)}s>"

                print(f"[{cid}] Latest ID: {id_val} / Latest State: {state_val}  [{status_text}]")
                print(f"    Last TX: {last_tx_message[cid]}")

            print()

            # ── 4) Assignment markers & detect TXs, queue pending checks
            for cid in containers:
                if cid not in results:
                    continue
                node_addr = node_addresses[cid].lower()
                id_val, state_val, raw_text, log_lines, container = results[cid]

                for idx, ln in enumerate(log_lines):
                    m_evt = LOG_EVENT_RE.search(ln)
                    if not m_evt:
                        continue
                    evt = m_evt.group(1)
                    if evt in ("NewSessionRequestAssigned", "SessionCreated") and state_val == 1:
                        for j in range(idx + 1, min(idx + 5, len(log_lines))):
                            ma = ASSIGNED_RE.search(log_lines[j])
                            if ma:
                                assigned = [a.strip().lower() for a in ma.group(1).split(",")]
                                if node_addr in assigned:
                                    msg = f"⭐ Assigned for Create"
                                    print(f"[{cid}] {msg}")
                                    last_tx_message[cid] = msg
                                break
                    elif evt == "SessionPrepareAssigned" and state_val == 1:
                        for j in range(idx + 1, min(idx + 5, len(log_lines))):
                            ma = ASSIGNED_RE.search(log_lines[j])
                            if ma:
                                assigned = [a.strip().lower() for a in ma.group(1).split(",")]
                                if node_addr in assigned:
                                    msg = f"⭐ Assigned for Prepare"
                                    print(f"[{cid}] {msg}")
                                    last_tx_message[cid] = msg
                                break

                if cid not in watch_tx_containers:
                    continue

                if tx_state[cid] == 0 and state_val == 3:
                    idx3 = find_latest_assigned_stage(log_lines, node_addr, 3)
                    if idx3 is not None:
                        tx1 = find_first_tx_after(log_lines, idx3)
                        if tx1:
                            tx_state[cid] = 1
                            tx_deadline_pre[cid] = now + timedelta(seconds=tx_timeout)
                            pending_checks[cid] = {
                                "tx": tx1,
                                "start": now,
                                "next_check": now + timedelta(seconds=5),
                                "type": "pre"
                            }
                            msg = f"⭐ Detected precommit TX {tx1}, will check in 5 s"
                            print(f"[{cid}] {msg}")
                            last_tx_message[cid] = msg

                if tx_state[cid] == 1:
                    if tx_deadline_pre[cid] and now > tx_deadline_pre[cid]:
                        if warmed_up[cid]:
                            msg = f"✘ Precommit TX timeout"
                            print(f"[{cid}] {msg}")
                            last_tx_message[cid] = msg
                            try:
                                pre_bytes = container.logs(tail=500, timestamps=False)
                                pre_text  = pre_bytes.decode("utf-8", errors="ignore")
                            except:
                                pre_text = "[ERROR capturing logs]\n"
                            ts_str = now.strftime("%Y%m%dT%H%M%S")
                            outfile = log_dir / f"{cid}_txtimeout_pre_{ts_str}.log"
                            try:
                                with open(outfile, "w", encoding="utf-8") as wf:
                                    wf.write(pre_text)
                            except Exception as e:
                                print(f"[{cid}] ✘ Could not write timeout log: {e}")
                            entry = f"{now.isoformat()} UTC  Restarted '{cid}' (pre_timeout) → '{outfile.name}'\n"
                            try:
                                with open(master_log, "a", encoding="utf-8") as mf:
                                    mf.write(entry)
                            except Exception as e:
                                print(f"[{cid}] ✘ Could not append to watcher.log: {e}")
                            try:
                                container.restart()
                                print(f"[{cid}] ✔ Restarted due to precommit timeout")
                            except Exception as e:
                                print(f"[{cid}] ✘ Restart failed: {e}")
                        else:
                            msg = f"⚠ Precommit TX timeout but not warmed up—skipped restart"
                            print(f"[{cid}] {msg}")
                            last_tx_message[cid] = msg
                        tx_state[cid] = 0
                        tx_seen[cid]["pre"] = False
                        tx_seen[cid]["commit"] = False
                        tx_deadline_pre[cid] = None
                        pending_checks.pop(cid, None)

                if tx_state[cid] == 2 and state_val == 4:
                    idx4 = find_latest_assigned_stage(log_lines, node_addr, 4)
                    if idx4 is not None:
                        tx2 = find_first_tx_after(log_lines, idx4)
                        if tx2:
                            tx_state[cid] = 3
                            tx_deadline_commit[cid] = now + timedelta(seconds=tx_timeout)
                            pending_checks[cid] = {
                                "tx": tx2,
                                "start": now,
                                "next_check": now + timedelta(seconds=5),
                                "type": "commit"
                            }
                            msg = f"⭐ Detected commit TX {tx2}, will check in 5 s"
                            print(f"[{cid}] {msg}")
                            last_tx_message[cid] = msg

                if tx_state[cid] == 3:
                    if tx_deadline_commit[cid] and now > tx_deadline_commit[cid]:
                        if warmed_up[cid]:
                            msg = f"✘ Commit TX timeout"
                            print(f"[{cid}] {msg}")
                            last_tx_message[cid] = msg
                            try:
                                pre_bytes = container.logs(tail=500, timestamps=False)
                                pre_text  = pre_bytes.decode("utf-8", errors="ignore")
                            except:
                                pre_text = "[ERROR capturing logs]\n"
                            ts_str = now.strftime("%Y%m%dT%H%M%S")
                            outfile = log_dir / f"{cid}_txtimeout_commit_{ts_str}.log"
                            try:
                                with open(outfile, "w", encoding="utf-8") as wf:
                                    wf.write(pre_text)
                            except Exception as e:
                                print(f"[{cid}] ✘ Could not write timeout log: {e}")
                            entry = f"{now.isoformat()} UTC  Restarted '{cid}' (commit_timeout) → '{outfile.name}'\n"
                            try:
                                with open(master_log, "a", encoding="utf-8") as mf:
                                    mf.write(entry)
                            except Exception as e:
                                print(f"[{cid}] ✘ Could not append to watcher.log: {e}")
                            try:
                                container.restart()
                                print(f"[{cid}] ✔ Restarted due to commit timeout")
                            except Exception as e:
                                print(f"[{cid}] ✘ Restart failed: {e}")
                        else:
                            msg = f"⚠ Commit TX timeout but not warmed up—skipped restart"
                            print(f"[{cid}] {msg}")
                            last_tx_message[cid] = msg
                        tx_state[cid] = 0
                        tx_seen[cid]["pre"] = False
                        tx_seen[cid]["commit"] = False
                        tx_deadline_commit[cid] = None
                        pending_checks.pop(cid, None)

                if tx_state[cid] == 4:
                    pass

            # ── 5) Process any pending_checks if their next_check time has passed
            for cid, info in list(pending_checks.items()):
                tx_hash   = info["tx"]
                next_chk  = info["next_check"]
                tx_type   = info["type"]

                if now < next_chk:
                    continue

                print(f"[{cid}] 🔄 Checking {tx_type}commit receipt for {tx_hash}…")
                last_tx_message[cid] = f"🔄 Checking {tx_type}commit receipt {tx_hash}"
                receipt = rpc_get_receipt(rpc_url, tx_hash)

                if receipt is None:
                    print(f"[{cid}] ⏳ Receipt still pending; will retry in 5 s")
                    info["next_check"] = now + timedelta(seconds=5)
                    continue

                status = receipt.get("status")
                if status == "0x1":
                    if tx_type == "pre":
                        tx_seen[cid]["pre"] = True
                        tx_state[cid] = 2
                        tx_deadline_pre[cid] = None
                        msg = f"✔ Precommit TX {tx_hash} status=0x1 (success)"
                    else:
                        tx_seen[cid]["commit"] = True
                        tx_state[cid] = 4
                        tx_deadline_commit[cid] = None
                        msg = f"✔ Commit TX {tx_hash} status=0x1 (success)"
                    print(f"[{cid}] {msg}")
                    last_tx_message[cid] = msg
                else:
                    if warmed_up[cid]:
                        reason = f"{tx_type}_revert"
                        msg = f"✘ {tx_type.capitalize()} TX {tx_hash} status=0x0 (failed) → restarting"
                        print(f"[{cid}] {msg}")
                        last_tx_message[cid] = msg

                        _, _, raw_text, _, container = results[cid]
                        try:
                            pre_bytes = container.logs(tail=500, timestamps=False)
                            pre_text  = pre_bytes.decode("utf-8", errors="ignore")
                        except:
                            pre_text = "[ERROR capturing logs]\n"
                        ts_str = now.strftime("%Y%m%dT%H%M%S")
                        suffix = f"txfail_{tx_type}_{tx_hash[:8]}_{ts_str}"
                        outfile = log_dir / f"{cid}_{suffix}.log"
                        try:
                            with open(outfile, "w", encoding="utf-8") as wf:
                                wf.write(pre_text)
                        except Exception as e:
                            print(f"[{cid}] ✘ Could not write failure log: {e}")
                        entry = f"{now.isoformat()} UTC  Restarted '{cid}' ({reason}) → '{outfile.name}'\n"
                        try:
                            with open(master_log, "a", encoding="utf-8") as mf:
                                mf.write(entry)
                        except Exception as e:
                            print(f"[{cid}] ✘ Could not append to watcher.log: {e}")
                        try:
                            container.restart()
                            print(f"[{cid}] ✔ Restarted due to {reason}")
                        except Exception as e:
                            print(f"[{cid}] ✘ Restart failed: {e}")

                        tx_state[cid] = 0
                        tx_seen[cid]["pre"] = False
                        tx_seen[cid]["commit"] = False
                        tx_deadline_pre[cid] = None
                        tx_deadline_commit[cid] = None
                    else:
                        msg = f"⚠ {tx_type.capitalize()} TX {tx_hash} status=0x0 (failed) but not warmed up—skipping restart"
                        print(f"[{cid}] {msg}")
                        last_tx_message[cid] = msg

                pending_checks.pop(cid, None)

            print()
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nWatcher interrupted by user. Exiting.")

if __name__ == "__main__":
    main()
