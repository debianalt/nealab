"""One-off: launch JRC Global Surface Water exports for the 5 v1.1 territories
that were missing flood data. The standard run_flood_update.py orchestrator
exports only S1 (current + historical recurrence); JRC occurrence/recurrence/
seasonality require launch_exports(jrc=True), which is not exposed via CLI.

This script kicks them off and exits. The user (or a follow-up session) downloads
the resulting tifs from gs://spatia-satellite/flood/ and re-runs
run_flood_update.py --skip-gee per territory to merge with the S1 outputs.
"""
from __future__ import annotations
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from gee_flood_detection import authenticate, launch_exports

TERRITORIES = ['chaco', 'formosa', 'parana_br', 'santa_catarina_br', 'rio_grande_sul_br']


def main() -> int:
    authenticate()
    print('GEE auth OK.')
    all_tasks = []
    for tid in TERRITORIES:
        print(f'\n--- Launching JRC export for {tid} ---')
        tasks = launch_exports(
            territory_id=tid,
            historical=False,
            current=False,
            jrc=True,
        )
        for t in tasks:
            print(f'  task: {t.status()["description"]} (id: {t.id})')
        all_tasks.extend(tasks)
    print(f'\nTotal JRC tasks launched: {len(all_tasks)}')
    print('Monitor at: https://code.earthengine.google.com/tasks')
    return 0


if __name__ == '__main__':
    sys.exit(main())
