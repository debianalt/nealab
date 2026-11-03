#!/usr/bin/env bash
# Compila, reinicia vite preview en localhost:5173 (origen permitido por el CORS de R2)
# y vuelve a correr la sonda del mapa sobre las vistas indicadas.
#
# Uso: bash director/reprobar.sh SALIDA [--territorios a,b] [--capas x,y] [--sin-depto]
# Tras cada build hay que reiniciar el preview: el viejo sirve HTML con chunks que ya no existen.
set -euo pipefail
cd "$(dirname "$0")/.."
SALIDA="$1"; shift
npm run build > /tmp/director-build.log 2>&1 || { tail -40 /tmp/director-build.log; echo "BUILD FALLÓ"; exit 2; }
pkill -f "vite preview" 2>/dev/null || true
sleep 1
nohup npx vite preview --port 5173 --strictPort > /tmp/director-preview.log 2>&1 &
for i in $(seq 1 40); do curl -sf http://localhost:5173/ > /dev/null && break; sleep 1; done
rm -rf "$SALIDA"; mkdir -p "$SALIDA"
python director/sondas/mapa.py http://localhost:5173 "$SALIDA" "$@"
python director/unir.py "$SALIDA" "$SALIDA/hallazgos.json"
