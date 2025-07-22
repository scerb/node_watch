# File: main.py
"""
main.py

Orchestrates modules, displays statuses, scans errors, handles TX logic,
monitors both Reputation and Session-Reputation API endpoints for precommit and commit timestamps,
and manages restarts on various error conditions.
Precommit, commit, sess-pre, and sess-com detections run every loop; TX-FSM flows through detect_pre → success_pre → detect_commit → success_commit.
Includes self-healing logic for Python tracebacks: nodes get 240s to recover before forced restart.
Supports per-restart toggles via config['restart_flags'].
"""
import time
from datetime import datetime, timezone, timedelta
import docker
import requests

from config_migrate import migrate_config
from log_feed import LogFeed
from tx_check import LOG_STATE_RE, TxChecker, rpc_get_receipt
from error_scan import saw_ping_fail, saw_traceback, saw_node_pool_stale
from restarter import RestartManager


def load_config(path='config.json'):
    return migrate_config(path)


def main():
    cfg = load_config()
    flags = cfg.get('restart_flags', {})

    # Version check initialization
    local_version = "v1.0.9"
    version_api = "https://api.github.com/repos/scerb/node_watch/releases/latest"
    last_version_check = datetime.min.replace(tzinfo=timezone.utc)
    remote_version = ""
    version_status = ""

    # Config values
    nodes = cfg['nodes']
    reputation_api_url = cfg['reputation_api_url']
    session_reputation_api_url = cfg['session_reputation_api_url']
    containers = [n['container'] for n in nodes]
    addresses = {n['container']: n['address'] for n in nodes}
    stats_url = cfg['stats_api_url']
    interval = float(cfg['check_interval_seconds'])
    rpc_url = cfg['rpc_url']
    tail = int(cfg['tail_lines'])

    # Initialize log feeds
    feeds = {cid: LogFeed(cid, tail_lines=tail) for cid in containers}
    for f in feeds.values(): f.start()

    # Initialize TX checker
    tx_checker = TxChecker(rpc_url, timeout_seconds=cfg['tx_timeout_seconds'])
    for cid in containers: tx_checker.init_container(cid, addresses[cid])

    # API polling setup
    API_INTERVAL = timedelta(seconds=60)
    last_pre = {cid: None for cid in containers}
    pending_pre = {}
    last_com = {cid: None for cid in containers}
    pending_com = {}
    last_sesspre = {cid: None for cid in containers}
    pending_sesspre = {}
    last_sesscom = {cid: None for cid in containers}
    pending_sesscom = {}
    next_api_check = {}
    now0 = datetime.now(timezone.utc)
    total = len(containers)
    for idx, cid in enumerate(containers):
        offset = timedelta(seconds=int((idx * API_INTERVAL.total_seconds()) / total))
        next_api_check[cid] = now0 + offset

    # Seed initial timestamps from APIs
    for cid in containers:
        addr = addresses[cid]
        try:
            rep = requests.get(f"{reputation_api_url}/{addr}", timeout=5).json()
            sess = requests.get(f"{session_reputation_api_url}/{addr}", timeout=5).json()
            pre_all = rep.get('precommit', {}).get('all_timestamps', [])
            com_all = rep.get('commit', {}).get('all_timestamps', [])
            sp_all = sess.get('precommit', {}).get('all_timestamps', [])
            sc_all = sess.get('commit', {}).get('all_timestamps', [])
            if pre_all: last_pre[cid] = max(pre_all)
            if com_all: last_com[cid] = max(com_all)
            if sp_all: last_sesspre[cid] = max(sp_all)
            if sc_all: last_sesscom[cid] = max(sc_all)
        except Exception:
            pass

    # Restarter with cooldown tracking and flags
    restarter = RestartManager(cooldown_minutes=2)
    last_restarted = {'cid': None, 'time': None}
    _orig = restarter.attempt_restart
    def _wrap(cid, obj, reason):
        if flags.get(reason, False):
            _orig(cid, obj, reason)
            last_restarted['cid'], last_restarted['time'] = cid, datetime.now(timezone.utc)
    restarter.attempt_restart = _wrap

    docker_client = docker.from_env()
    last_tx = {c: '(no TX)' for c in containers}
    fail_counts = {c: 0 for c in containers}
    stage = {c: 0 for c in containers}
    last_remote = None

    try:
        while True:
            now = datetime.now(timezone.utc)
            # Version refresh every 24h
            if (now - last_version_check).total_seconds() >= 86400:
                try:
                    r = requests.get(version_api, timeout=5)
                    r.raise_for_status()
                    remote_version = r.json().get('tag_name', '') or ''
                except Exception:
                    remote_version = ''
                version_status = (
                    'latest' if remote_version == local_version else
                    (f"please update ({remote_version})" if remote_version else 'unknown')
                )
                last_version_check = now

            # Clear and fetch session
            print('[H[2J', end='')
            try:
                rs = requests.get(stats_url, timeout=5).json()
                stats = rs.get('stats', rs.get('data', rs))
                maxk = max((int(k) for k in stats if k.isdigit()), default=None)
                remote = stats.get(str(maxk), {}).get('session_id') if maxk else None
            except Exception:
                remote = None

            # Reset TX-FSM on new session
            if last_remote is not None and remote != last_remote:
                for c in containers:
                    stage[c] = 0
                    tx_checker.on_new_session(c)
            last_remote = remote

            # Header
            active = sum(
                1 for cid in containers
                if docker_client.containers.get(cid).status == 'running'
            )
            rinfo = ''
            if last_restarted['cid']:
                ago = int((now - last_restarted['time']).total_seconds())
                rinfo = f" last_restart={last_restarted['cid']} ({ago}s ago)"
            print(
                f"=== {now.isoformat()} UTC  version={local_version} ({version_status})  "
                f"last_session={remote}  nodes={active}/{len(containers)}{rinfo} ==="
            )

            # Status lines
            for cid in containers:
                lines = feeds[cid].get_lines()
                idv = sd = None
                for ln in lines:
                    m = LOG_STATE_RE.search(ln)
                    if m:
                        idv, sd = m.group(1), m.group(2)
                idd = idv or 'USER'
                sdisp = f"State={sd}" if sd else "Mode=USER"
                def age_disp(ts, pend):
                    if ts:
                        a = int((now - datetime.fromtimestamp(ts, timezone.utc)).total_seconds())
                        mk = '✔' if cid not in pend else '…'
                        return f"{a}s{mk}"
                    return '-'
                pre = age_disp(last_pre[cid], pending_pre)
                com = age_disp(last_com[cid], pending_com)
                sp = age_disp(last_sesspre[cid], pending_sesspre)
                sc = age_disp(last_sesscom[cid], pending_sesscom)
                nxt = int(max((next_api_check[cid] - now).total_seconds(), 0))
                print(
                    f"[{cid}] {idd}/{sdisp} | TX:{last_tx[cid]} | "
                    f"Pre:{pre} Com:{com} | S-Pre:{sp} S-Com:{sc} | next:{nxt}s"
                )
            print()

            # Per-container processing
            for cid in containers:
                lines = feeds[cid].get_lines()

                # 1) Reputation & Session API checks
                if now >= next_api_check[cid]:
                    addr = addresses[cid]
                    try:
                        rep = requests.get(
                            f"{reputation_api_url}/{addr}", timeout=10
                        ).json()
                        sess = requests.get(
                            f"{session_reputation_api_url}/{addr}", timeout=10
                        ).json()
                        pa = rep.get('precommit', {}).get('all_timestamps', [])
                        ps = rep.get('precommit', {}).get('success_timestamps', [])
                        ca = rep.get('commit', {}).get('all_timestamps', [])
                        cs = rep.get('commit', {}).get('success_timestamps', [])
                        spa = sess.get('precommit', {}).get('all_timestamps', [])
                        sps = sess.get('precommit', {}).get('success_timestamps', [])
                        sca = sess.get('commit', {}).get('all_timestamps', [])
                        scs = sess.get('commit', {}).get('success_timestamps', [])
                    except Exception:
                        pa = ps = ca = cs = spa = sps = sca = scs = []
                    # -- rep pre
                    if pa:
                        mp = max(pa)
                        prev = last_pre[cid]
                        if prev is None:
                            last_pre[cid] = mp
                        elif mp > prev:
                            last_pre[cid] = mp
                            pending_pre[cid] = (mp, now + timedelta(seconds=180))
                            print(f"[{cid}] ⭐ Detected precommit {mp}")
                    if cid in pending_pre:
                        tstamp, dl = pending_pre[cid]
                        if tstamp in ps:
                            print(f"[{cid}] ✔ Confirmed precommit {tstamp}")
                            del pending_pre[cid]
                        elif now > dl:
                            restarter.attempt_restart(
                                cid, docker_client.containers.get(cid),
                                'precommit_timeout'
                            )
                            del pending_pre[cid]
                    # -- rep com
                    if ca:
                        mc = max(ca)
                        prevc = last_com[cid]
                        if prevc is None:
                            last_com[cid] = mc
                        elif mc > prevc:
                            last_com[cid] = mc
                            pending_com[cid] = (mc, now + timedelta(seconds=180))
                            print(f"[{cid}] 🔄 Detected commit {mc}")
                    if cid in pending_com:
                        tstamp, dl = pending_com[cid]
                        if tstamp in cs:
                            print(f"[{cid}] ✔ Confirmed commit {tstamp}")
                            del pending_com[cid]
                        elif now > dl:
                            restarter.attempt_restart(
                                cid, docker_client.containers.get(cid),
                                'commit_timeout'
                            )
                            del pending_com[cid]
                    # -- sess pre
                    if spa:
                        msp = max(spa)
                        prevsp = last_sesspre[cid]
                        if prevsp is None:
                            last_sesspre[cid] = msp
                        elif msp > prevsp:
                            last_sesspre[cid] = msp
                            pending_sesspre[cid] = (msp, now + timedelta(seconds=180))
                            print(f"[{cid}] 🕑 Detected sess-pre {msp}")
                    if cid in pending_sesspre:
                        tstamp, dl = pending_sesspre[cid]
                        if tstamp in sps:
                            print(f"[{cid}] ✔ Confirmed sess-pre {tstamp}")
                            del pending_sesspre[cid]
                        elif now > dl:
                            restarter.attempt_restart(
                                cid, docker_client.containers.get(cid),
                                'sesspre_timeout'
                            )
                            del pending_sesspre[cid]
                    # -- sess com
                    if sca:
                        msc = max(sca)
                        prevsc = last_sesscom[cid]
                        if prevsc is None:
                            last_sesscom[cid] = msc
                        elif msc > prevsc:
                            last_sesscom[cid] = msc
                            pending_sesscom[cid] = (msc, now + timedelta(seconds=180))
                            print(f"[{cid}] ⏳ Detected sess-com {msc}")
                    if cid in pending_sesscom:
                        tstamp, dl = pending_sesscom[cid]
                        if tstamp in scs:
                            print(f"[{cid}] ✔ Confirmed sess-com {tstamp}")
                            del pending_sesscom[cid]
                        elif now > dl:
                            restarter.attempt_restart(
                                cid, docker_client.containers.get(cid),
                                'sesscom_timeout'
                            )
                            del pending_sesscom[cid]
                    # schedule next API check
                    next_api_check[cid] = now + API_INTERVAL

                # 2) Error scans
                if saw_node_pool_stale(lines):
                    restarter.attempt_restart(
                        cid, docker_client.containers.get(cid), 'node_pool_stale'
                    )
                    stage[cid] = 0
                    continue
                if saw_traceback(lines):
                    restarter.attempt_restart(
                        cid, docker_client.containers.get(cid), 'traceback'
                    )
                    stage[cid] = 0
                    continue
                if saw_ping_fail(lines):
                    restarter.attempt_restart(
                        cid, docker_client.containers.get(cid), 'pingfail'
                    )
                    stage[cid] = 0
                    continue
                # 3) Lag-based restart
                idn = None
                for ln in lines:
                    m = LOG_STATE_RE.search(ln)
                    if m: idn = int(m.group(1))
                if idn is not None and remote is not None and idn < int(remote):
                    restarter.attempt_restart(
                        cid, docker_client.containers.get(cid), 'lag'
                    )
                    stage[cid] = 0
                    continue
                # 4) TX-FSM
                pr = f"{idn or '?'} "
                st = stage[cid]
                dec = tx_checker.process_logs(
                    cid, lines, now, lambda tx: rpc_get_receipt(rpc_url, tx)
                )
                if st == 0 and dec and dec[0] == 'detect_pre':
                    tx = dec[1]
                    short = f"{tx[:10]}..."
                    last_tx[cid] = f"{pr}⭐ Pre-TX {short}"
                    stage[cid] = 1
                elif st == 1 and dec:
                    kind, tx = dec
                    short = f"{tx[:10]}..."
                    if kind == 'success_pre':
                        last_tx[cid] = f"{pr}✔ Pre OK {short}"
                        stage[cid] = 2
                    elif kind == 'fail':
                        fail_counts[cid] += 1
                        last_tx[cid] = f"{pr}✘ Pre fail ({fail_counts[cid]})"
                        if fail_counts[cid] >= 2:
                            restarter.attempt_restart(
                                cid, docker_client.containers.get(cid), tx
                            )
                            stage[cid] = 0
                            fail_counts[cid] = 0
                elif st == 2 and dec:
                    kind, tx = dec
                    short = f"{tx[:10]}..."
                    if kind == 'detect_commit':
                        last_tx[cid] = f"{pr}⭐ Com-TX {short}"
                        stage[cid] = 3
                    elif kind == 'success_commit':
                        last_tx[cid] = f"{pr}✔ Com OK {short}"
                        tx_checker.on_new_session(cid)
                        stage[cid] = 3
                    elif kind == 'fail':
                        fail_counts[cid] += 1
                        last_tx[cid] = f"{pr}✘ Com fail ({fail_counts[cid]})"
                        if fail_counts[cid] >= 2:
                            restarter.attempt_restart(
                                cid, docker_client.containers.get(cid), tx
                            )
                            tx_checker.on_new_session(cid)
                            stage[cid] = 0
                            fail_counts[cid] = 0

            time.sleep(interval)
    except KeyboardInterrupt:
        print('Exiting...')
    finally:
        for f in feeds.values(): f.stop()


if __name__ == '__main__':
    main()
