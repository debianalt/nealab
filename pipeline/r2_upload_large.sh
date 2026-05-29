#!/usr/bin/env bash
# Wrangler 4.95 on Windows aborts with a libuv assertion on R2 uploads
# >~100MB. The Cloudflare REST API at /accounts/{acct}/r2/buckets/{bucket}
# /objects/{key} accepts a PUT with the wrangler OAuth bearer token (the
# scope set granted to wrangler includes implicit R2 write through
# workers_scripts:write) and handles the upload without the libuv path.
#
# This script wraps that API for the few big globals (carbon_stock,
# productive_activity for RS / PR) so the standard pipeline doesn't have
# to special-case them every time.
#
# Usage: bash pipeline/r2_upload_large.sh <r2-key> <local-file>
# Reads token + account from ~/.wrangler/config/default.toml.

set -e
KEY=$1
FILE=$2
if [ -z "$KEY" ] || [ -z "$FILE" ]; then
  echo "usage: $0 <r2-key> <local-file>"
  exit 1
fi
if [ ! -f "$FILE" ]; then
  echo "ERROR: $FILE not found"
  exit 1
fi
WCFG="$HOME/.wrangler/config/default.toml"
TOKEN=$(grep -E '^oauth_token =' "$WCFG" | sed 's/oauth_token = "\(.*\)"/\1/')
ACCT=$(npx wrangler whoami 2>&1 | grep -oE '[0-9a-f]{32}' | head -1)
if [ -z "$TOKEN" ] || [ -z "$ACCT" ]; then
  echo "ERROR: could not read oauth_token from $WCFG or account from wrangler whoami"
  exit 1
fi
echo "PUT https://api.cloudflare.com/client/v4/accounts/$ACCT/r2/buckets/neahub/objects/$KEY"
curl -s -X PUT "https://api.cloudflare.com/client/v4/accounts/$ACCT/r2/buckets/neahub/objects/$KEY" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/octet-stream" \
  --data-binary "@$FILE" \
  | python -c "import json,sys; r=json.load(sys.stdin); print('OK' if r.get('success') else r)"
