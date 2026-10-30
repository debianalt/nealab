"""Edge cache for cdn.spatia.ar (R2 custom domain on the spatia.ar zone).

Cloudflare does not cache .pmtiles/.parquet by default, so without these rules every
range read travels to the bucket (region ENAM) and back. `setup` installs, idempotently:

  1. a Cache Rule that makes cdn.spatia.ar eligible for cache (edge TTL 1 day, browser
     TTL from origin, strong ETags respected);
  2. Smart Tiered Cache, so one upper tier pulls from R2 and the rest fetch from it.

CORS needs nothing here: R2 sends `Vary: Origin` and the edge keys its copies by Origin
(checked 30-oct-2026: www.spatia.ar after spatia.ar is a MISS with its own header), so
cors-rules.json on the bucket stays the only allow-list. An earlier version rewrote the
header with Transform Rules; `setup` removes them (HEADERS_PHASE maps to no rules).

Only rules whose `ref` starts with REF_PREFIX are touched; other rules in the same
phase are kept. Files over 512 MB (the BR building tiles) are never cached on the Free
plan: a cache-eligible request for one comes back as a full 200 instead of the 206 range
(seen 30-oct-2026 on parana_br_buildings, 854 MB), so they are excluded by path in
UNCACHEABLE and keep going to R2. Any new file over 512 MB must be added there.

Usage:
  python scripts/cdn_cache.py setup | status | disable
  python scripts/cdn_cache.py purge [data/key ...]   # no keys = whole cdn.spatia.ar

Token: the single Spatia account token (no expiry; R2/Pages/D1/Workers + spatia.ar zone
rules and purge). Env CLOUDFLARE_API_TOKEN (CI secret of the same name), else the
CLOUDFLARE_API_TOKEN line of ~/Documents/secrets/cloudfare1.env.
"""
import json
import os
import sys
import urllib.error
import urllib.request

ZONE = "9ae7ae8646d8347a86fa0844925513b5"
HOST = "cdn.spatia.ar"
API = f"https://api.cloudflare.com/client/v4/zones/{ZONE}"
REF_PREFIX = "spatia_cdn_"
EDGE_TTL = 86400
CACHE_PHASE = "http_request_cache_settings"
HEADERS_PHASE = "http_response_headers_transform"
SECRETS_FILE = "~/Documents/secrets/cloudfare1.env"
# Path fragments of objects over 512 MB (Free-plan cache limit); see module docstring.
UNCACHEABLE = ("_br_buildings",)


def token():
    env = os.environ.get("CLOUDFLARE_API_TOKEN")
    if env:
        return env.strip()
    path = os.path.expanduser(SECRETS_FILE)
    if os.path.exists(path):
        for line in open(path, encoding="utf-8-sig"):
            key, _, value = line.strip().partition("=")
            if key == "CLOUDFLARE_API_TOKEN" and value:
                return value.strip()
    sys.exit(f"No token: set CLOUDFLARE_API_TOKEN or add it to {SECRETS_FILE}")


def call(method, path, body=None, ok404=False):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method, headers={
        "Authorization": f"Bearer {token()}", "Content-Type": "application/json"})
    try:
        return json.load(urllib.request.urlopen(req, timeout=60))
    except urllib.error.HTTPError as e:
        if ok404 and e.code == 404:
            return None
        detail = e.read().decode(errors="replace")
        raise SystemExit(f"{method} {path} -> HTTP {e.code}: {detail}")


def our_rules():
    host = f'http.host eq "{HOST}"'
    big = " or ".join(f'http.request.uri.path contains "{f}"' for f in UNCACHEABLE)
    cache = [{
        "ref": REF_PREFIX + "cache",
        "description": "Spatia: cache cdn.spatia.ar (pmtiles/parquet) at the edge",
        "expression": f"{host} and not ({big})",
        "action": "set_cache_settings",
        "action_parameters": {
            "cache": True,
            "edge_ttl": {"mode": "override_origin", "default": EDGE_TTL},
            "browser_ttl": {"mode": "respect_origin"},
            "respect_strong_etags": True,
        },
        "enabled": True,
    }]
    return {CACHE_PHASE: cache, HEADERS_PHASE: []}


KEEP = ("id", "ref", "description", "expression", "action", "action_parameters", "enabled",
        "logging")


def phase_rules(phase):
    d = call("GET", f"/rulesets/phases/{phase}/entrypoint", ok404=True)
    return [] if d is None else d["result"].get("rules", [])


def put_phase(phase, rules):
    clean = [{k: v for k, v in r.items() if k in KEEP} for r in rules]
    call("PUT", f"/rulesets/phases/{phase}/entrypoint", {"rules": clean})


# The 1-jul-2026 rule (commit af52b6c) it replaces: edge-cache only objects uploaded with
# Cache-Control, bypass the rest. That left 87 % of objects, all PMTiles included, uncached.
# `disable` puts it back as it was.
LEGACY_REF = "1bf54fa5866f4a4f84d247efb6514088"
LEGACY_RULE = {
    "description": "R2 CDN immutable",
    "expression": f'(http.host eq "{HOST}")',
    "action": "set_cache_settings",
    "action_parameters": {"cache": True, "edge_ttl": {"mode": "bypass_by_default"},
                          "browser_ttl": {"mode": "respect_origin"}},
    "enabled": True,
}


def is_ours(rule):
    return rule.get("ref", "").startswith(REF_PREFIX) or rule.get("ref") == LEGACY_REF


def setup():
    for phase, mine in our_rules().items():
        rules = phase_rules(phase)
        others = [r for r in rules if not is_ours(r)]
        put_phase(phase, others + mine)
        dropped = sum(r.get("ref") == LEGACY_REF for r in rules)
        print(f"{phase}: {len(mine)} Spatia rule(s), {len(others)} other(s) kept"
              + (", 1-jul legacy rule replaced" if dropped else ""))
    for path in ("/argo/tiered_caching", "/cache/tiered_cache_smart_topology_enable"):
        call("PATCH", path, {"value": "on"})
    print("tiered cache: on (smart topology)")


def disable():
    for phase in our_rules():
        rules = phase_rules(phase)
        others = [r for r in rules if not is_ours(r)]
        removed = len(rules) - len(others)
        if phase == CACHE_PHASE:
            others.append(LEGACY_RULE)
        put_phase(phase, others)
        print(f"{phase}: removed {removed} Spatia rule(s)"
              + (", 1-jul legacy rule restored" if phase == CACHE_PHASE else ""))
    print("tiered cache left as is (harmless without the cache rule)")


def status():
    for phase in our_rules():
        for r in phase_rules(phase):
            mark = "*" if is_ours(r) else " "
            print(f"{mark} {phase}: {r.get('ref', r.get('id'))} enabled={r.get('enabled')}"
                  f" -> {r['expression']}")
    for path in ("/argo/tiered_caching", "/cache/tiered_cache_smart_topology_enable"):
        print(path, call("GET", path)["result"]["value"])


def purge(keys):
    if keys:
        # Prefixes, not URLs: also clears the ?v=N variants of each key.
        body = {"prefixes": [f"{HOST}/{k.lstrip('/')}" for k in keys]}
    else:
        body = {"hosts": [HOST]}
    try:
        call("POST", "/purge_cache", body)
    except SystemExit as e:
        print(f"targeted purge refused ({e}); purging everything", file=sys.stderr)
        call("POST", "/purge_cache", {"purge_everything": True})
    print("purged", ", ".join(body.get("prefixes", body.get("hosts", []))))


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("setup", "status", "disable", "purge"):
        sys.exit(__doc__)
    cmd = sys.argv[1]
    if cmd == "purge":
        purge(sys.argv[2:])
    else:
        {"setup": setup, "status": status, "disable": disable}[cmd]()


if __name__ == "__main__":
    main()
