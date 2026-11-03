#!/usr/bin/env bash
# Al abrir una sesión de Claude Code: trae lo que el director (u otra máquina) publicó en main.
# Con el árbol limpio hace pull --ff-only; con cambios locales sólo avisa.
cd "$(dirname "$0")/.." || exit 0
git fetch -q origin main 2>/dev/null || exit 0
atras=$(git rev-list --count HEAD..origin/main 2>/dev/null || echo 0)
[ "$atras" = "0" ] && exit 0
if git diff --quiet && git diff --cached --quiet; then
  git pull -q --ff-only origin main && echo "Sincronizado con origin/main: $atras commits nuevos (director de Spatia u otra máquina)."
else
  echo "origin/main tiene $atras commits nuevos y hay cambios locales sin commitear: hacer git pull --rebase antes de commitear."
fi
