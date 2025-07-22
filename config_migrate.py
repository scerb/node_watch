    # File: config_migrate.py
"""
config_migrate.py

On startup, detects legacy config format and migrates to the new schema:
- If config.json is missing, writes a default template with placeholders and exits.
- For any config lacking 'nodes' or missing 'restart_flags', backs up old config.json to config_old.json (or config_old_N.json)
  and generates/updates a new config.json with the 'nodes' array, cleans up duplicated fields,
  and ensures keys for the reputation APIs and restart_flags are present.
"""
import json
from pathlib import Path
import sys

def migrate_config(path='config.json'):
    p = Path(path)
    # If no config exists, create a default template and exit
    if not p.exists():
        default = {
            'nodes': [
                {'container': 'cortensor-1', 'address': '0x...'},
                {'container': 'cortensor-2', 'address': '0x...'},
            ],
            'tail_lines': 300,
            'check_interval_seconds': 5,
            'grace_period_seconds': 30,
            'rpc_url': 'https://arbitrum-sepolia-rpc.publicnode.com',
            'stats_api_url': 'https://db-be-6.cortensor.network/network-stats-tasks',
            'reputation_api_url': 'https://db-be-6.cortensor.network/reputation',
            'session_reputation_api_url': 'https://db-be-6.cortensor.network/session-reputation',
            'tx_timeout_seconds': 30,
            'restart_flags': {
                'traceback': True,
                'traceback_unrecovered': True,
                'pingfail': True,
                'node_pool_stale': True,
                'lag': True,
                'precommit_timeout': True,
                'commit_timeout': True,
                'sesspre_timeout': True,
                'sesscom_timeout': True,
                'tx_failure': True
            }
        }
        p.write_text(json.dumps(default, indent=2) + '')
        print(f"Created default config at '{path}'. Please edit and re-run.")
        sys.exit(0)

    raw = json.loads(p.read_text())
    # Schema check: must have 'nodes'
    if 'nodes' in raw:
        updated = False
        # Ensure restart_flags present
        if 'restart_flags' not in raw:
            raw['restart_flags'] = {
                'traceback': True,
                'traceback_unrecovered': True,
                'pingfail': True,
                'node_pool_stale': True,
                'lag': True,
                'precommit_timeout': True,
                'commit_timeout': True,
                'sesspre_timeout': True,
                'sesscom_timeout': True,
                'tx_failure': True
            }
            updated = True
        # Ensure API URLs present
        if 'reputation_api_url' not in raw:
            raw['reputation_api_url'] = 'https://db-be-6.cortensor.network/reputation'
            updated = True
        if 'session_reputation_api_url' not in raw:
            raw['session_reputation_api_url'] = 'https://db-be-6.cortensor.network/session-reputation'
            updated = True
        if updated:
            p.write_text(json.dumps(raw, indent=2) + '')
            print(f"Updated config '{path}' with missing keys. Please review.")
        return raw

    # Legacy format: 'containers' + 'node_addresses'
    containers = raw.get('containers', [])
    addresses = raw.get('node_addresses', {})
    nodes = []
    for c in containers:
        addr = addresses.get(c)
        if addr:
            nodes.append({'container': c, 'address': addr})

    # Compose new config with defaults and migrate fields
    new_cfg = {
        'nodes': nodes,
        'tail_lines': raw.get('tail_lines', 300),
        'check_interval_seconds': raw.get('check_interval_seconds', 5),
        'grace_period_seconds': raw.get('grace_period_seconds', 30),
        'rpc_url': raw.get('rpc_url', 'https://arbitrum-sepolia-rpc.publicnode.com'),
        'stats_api_url': raw.get('stats_api_url', 'https://db-be-6.cortensor.network/network-stats-tasks'),
        'reputation_api_url': raw.get('reputation_api_url', 'https://db-be-6.cortensor.network/reputation'),
        'session_reputation_api_url': raw.get('session_reputation_api_url', 'https://db-be-6.cortensor.network/session-reputation'),
        'tx_timeout_seconds': raw.get('tx_timeout_seconds', 30),
        'restart_flags': raw.get('restart_flags', {
            'traceback': True,
            'traceback_unrecovered': True,
            'pingfail': True,
            'node_pool_stale': True,
            'lag': True,
            'precommit_timeout': True,
            'commit_timeout': True,
            'sesspre_timeout': True,
            'sesscom_timeout': True,
            'tx_failure': True
        })
    }

    # Backup old config
    backup = p.with_name('config_old.json')
    idx = 1
    while backup.exists():
        backup = p.with_name(f'config_old_{idx}.json')
        idx += 1
    p.rename(backup)

    # Write new config
    p.write_text(json.dumps(new_cfg, indent=2) + '')
    print(f"Migrated old config to {backup.name}, wrote new config.json")
    return new_cfg

if __name__ == '__main__':
    migrate_config()
