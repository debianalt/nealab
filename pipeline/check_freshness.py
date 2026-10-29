"""
Weekly freshness check of what spatia.ar actually serves.

From June to October 2026 every CI upload was a silent no-op (upload_to_r2 ran
`npx` alone on Linux) while the workflows finished green: nothing compared R2
with the expected cadence. This does, without GEE or credentials — it only
reads Last-Modified from the public bucket and data/flood_meta.json — and exits
1 with a Markdown report when something is late; the workflow turns that into a
GitHub issue.

Only layers with an automatic refresh are checked. Layers refreshed by hand
(Paraguay, Brazil, agri_potential, land use, Overture) have no cadence to miss.

    python pipeline/check_freshness.py            # report to stdout, exit 1 if late
"""

import json
import sys
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

R2 = 'https://pub-580c676bec7f4eeb96d7d30559a3cab7.r2.dev'
NEA = {'misiones': '', 'corrientes': 'corrientes/', 'chaco': 'chaco/', 'formosa': 'formosa/'}


def flood_max_days(now: datetime) -> int:
    # Weekly Oct–Apr (+2 days of slack), monthly May–Sep (+5).
    return 9 if now.month in (10, 11, 12, 1, 2, 3, 4) else 36


# (R2 key, max age in days, what refreshes it)
STATIC_CHECKS = [
    ('data/sat_deforestation_dynamics.parquet', 200, 'deforestation-update.yml (1-abr / 1-oct)'),
    ('data/sat_pm25_drivers.parquet', 200, 'pm25-update.yml (15-may / 15-nov)'),
]


def last_modified(key: str) -> datetime | None:
    req = urllib.request.Request(f'{R2}/{key}?check={int(datetime.now().timestamp())}', method='HEAD',
                                 headers={'User-Agent': 'spatia-freshness/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return parsedate_to_datetime(r.headers['Last-Modified'])
    except Exception:
        return None


def main() -> int:
    now = datetime.now(timezone.utc)
    late, ok = [], []

    flood_days = flood_max_days(now)
    try:
        req = urllib.request.Request(f'{R2}/data/flood_meta.json?check={int(now.timestamp())}',
                                     headers={'User-Agent': 'spatia-freshness/1.0'})
        with urllib.request.urlopen(req, timeout=30) as r:
            meta = json.load(r)
    except Exception as e:
        meta = {}
        late.append(f'`data/flood_meta.json` no se pudo leer ({e}) — flood-update.yml')
    for tid, prefix in NEA.items():
        processed = meta.get(tid, {}).get('processed')
        age = (now - datetime.fromisoformat(processed).replace(tzinfo=timezone.utc)).days if processed else None
        parquet = last_modified(f'data/{prefix}hex_flood_risk.parquet')
        p_age = (now - parquet).days if parquet else None
        line = (f'inundación {tid}: procesado hace {age if age is not None else "?"} d, '
                f'parquet hace {p_age if p_age is not None else "?"} d (máx {flood_days} d) — flood-update.yml')
        (late if age is None or p_age is None or max(age, p_age) > flood_days else ok).append(line)

    for key, max_days, source in STATIC_CHECKS:
        lm = last_modified(key)
        age = (now - lm).days if lm else None
        line = f'`{key}`: hace {age if age is not None else "?"} d (máx {max_days} d) — {source}'
        (late if age is None or age > max_days else ok).append(line)

    print(f'## Frescura de datos — {now:%Y-%m-%d}\n')
    if late:
        print('### Atrasado\n')
        print('\n'.join(f'- {x}' for x in late))
        print()
    print('### Al día\n')
    print('\n'.join(f'- {x}' for x in ok) or '- (nada)')
    return 1 if late else 0


if __name__ == '__main__':
    sys.exit(main())
