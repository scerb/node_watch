To run, update config with your container and address details. 

cd to folder
chmod +x watch_majority_extended.py 
docker compose up --build


1. Monitors multiple Docker “miner” containers

Periodically fetches each container’s last N log lines.
Parses out the “Latest ID” and “Latest State” for each.

2. Majority‐state enforcement & auto‐restart

Computes the most common (ID, State) pair across all healthy containers.
If any warmed-up container lags behind that majority by more than your configured grace period, it:
Saves its last 500 log lines to restart_logs/…
Appends an entry to watcher.log
Restarts the container
If the majority State is 6 (SessionEnded), it also ensures all containers are running, restarting any that are stopped or missing.

3. “Ping‐fail” detection

Scans each container’s most recent logs for repeated “Pinging network…” failures.
If it sees three or more in the last ~80 lines, it dumps logs and restarts immediately.

4. Assignment markers

Watches for on-chain “Assigned Miners” events in State 1.
Logs a “⭐ Assigned for Create” or “⭐ Assigned for Prepare” beside the miner that got assigned.

5. On-chain TX tracking & retry

For containers you’ve opted into (watch_tx_for_containers):
Precommit TX: when State 3 + Assigned appears, captures the TX hash and polls its receipt every 5 s for success/failure (with a timeout).
Commit TX: similarly for State 4 + Assigned.
On failure or timeout (after warm-up), dumps logs, restarts, and logs to watcher.log.

6. Warm-up period

The first 3 minutes after the script starts are considered “warm-up.”
During warm-up it will still detect issues but won’t auto-restart on TX timeouts or majority lag until after 3 min have elapsed.