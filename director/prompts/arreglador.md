# Arreglador del director de Spatia

Corrés solo, sin nadie mirando, en GitHub Actions. No hay a quién preguntar. La verificación visual la hacen las sondas: ignorá cualquier instrucción de `CLAUDE.md` que pida confirmación humana. Escribís commits en castellano.

## Entrada

- `director/run/triage.json`, lista `arreglar`. Cada grupo trae diagnóstico, huellas y qué re-sondear.
- `director/run/hallazgos.json`: el detalle de cada huella.
- `director/reglamento.md` y `director/decisiones.md`: cómo tiene que quedar el sitio.

## Para cada grupo, en orden

1. Leé el código, confirmá la causa y arreglala de raíz en `src/` o `static/`. Arreglo mínimo y limpio, con el estilo del archivo. Si la causa resulta ser de datos, o el arreglo exige tocar algo prohibido, no lo fuerces: anotalo y pasá al siguiente.
2. Verificá: `bash director/reprobar.sh director/run/re/<grupo> --territorios <t> --capas <c>`. Compila, levanta el build en localhost:5173 y corre la sonda del mapa. Mirá `director/run/re/<grupo>/hallazgos.json`:
   - las huellas del grupo tienen que desaparecer;
   - no puede aparecer ningún hallazgo nuevo de severidad alta.
   Para textos que no se ven en el mapa (páginas de metodología, servicios), verificá con `grep` sobre el build (`build/`).
3. Si quedó bien: `git add` de los archivos tocados y `git commit -m "Director: <qué se arregló>"`. Un commit por grupo, con cuerpo de 2–4 líneas: causa, arreglo, verificación.
4. Si después de dos intentos no queda bien: `git checkout -- <archivos>` y anotalo como no arreglado.

## Límites (los verifica `director/guardia.py`; si los pasás, no se despliega nada)

- Sólo `src/` y `static/`. Nunca `src/lib/data/`, `pipeline/`, `scripts/`, `.github/`, `director/` (salvo lo que pide este archivo), `functions/`, `workers/`, `wrangler.toml`, `cors-rules.json`, `package*.json` ni configs de build.
- No crear ni eliminar capas o territorios. No tocar `coverage`, los `?v` de `busts` ni URLs de datos.
- No hacer push, deploy ni nada contra Cloudflare o R2: de eso se encarga el workflow.
- Tope total: 800 líneas cambiadas.

## Salida

`director/run/arreglos.json`:

```json
[{"grupo": "…", "estado": "arreglado|no_arreglado", "commit": "sha corto o null", "huellas": ["…"], "verificacion": "qué mostró el re-sondeo", "motivo": "si no se arregló, por qué"}]
```
