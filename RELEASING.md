# Releases y DOI — leer antes de publicar

Este repositorio tiene una topología que ya causó confusión más de una vez. Está documentada acá
para no volver a deducirla desde cero.

## Identidad del repositorio

**`debianalt/nealab` y `debianalt/spatia` son el mismo repositorio** (id `1205897531`). Hubo un
rename y GitHub redirige el nombre viejo. No son dos proyectos: cualquier URL con `spatia` termina
en `nealab`. El nombre canónico hoy es **`nealab`**.

## Remotos de la copia de trabajo

La copia local tiene **dos** remotos, y no son intercambiables:

| Remoto | Apunta a | Para qué |
|---|---|---|
| `nealab` | `github.com/debianalt/nealab` | **Repo público, el que tiene el DOI de Zenodo.** Acá se publica. |
| `origin` | `github.com/debianalt/spatia-dev` | Desarrollo. **No** está conectado a Zenodo. |

⚠️ El branch local trackea `origin`, no `nealab`. Un `git push` a secas **no publica nada**: va a
`spatia-dev`. Ése fue exactamente el motivo por el que la corrección del `CITATION.cff` del
18-jul-2026 nunca llegó a Zenodo — estaba commiteada y pusheada, pero al remoto equivocado.

## Branch por defecto

El default del repo público es **`master`**, no `main`. Todos los releases (v1.0.0 … v2.3.1)
cuelgan de `master`.

Existió una rama `main` abandonada, con historia propia no relacionada con `master` y un
`CITATION.cff` en v1.1.0 que declaraba un DOI viejo (`10.5281/zenodo.19543818`) sin correspondencia
con el registro vigente. Se eliminó el 21-jul-2026 tras archivar su historia en el tag
**`archive/main-2026-04`**, que la deja alcanzable de forma permanente. Si alguna vez hace falta
recuperar algo de ahí: `git show archive/main-2026-04`.

## Cómo cortar un release que actualice Zenodo

```sh
# 1. Actualizar la version y la fecha en CITATION.cff, y commitear
#    (el 'doi:' de CITATION.cff debe ser el CONCEPT DOI, ver abajo)

# 2. Publicar al remoto correcto — NO alcanza con 'git push'
git push nealab HEAD:master

# 3. Tag anotado con la misma version que CITATION.cff
git tag -a v2.3.1 -m "v2.3.1 — descripcion breve"
git push nealab v2.3.1

# 4. Crear el release en GitHub; esto es lo que dispara el webhook de Zenodo
gh release create v2.3.1 --repo debianalt/nealab \
  --title "v2.3.1 — ..." --notes "..."

# 5. Verificar que Zenodo lo tomo (suele tardar menos de un minuto)
curl -s https://zenodo.org/api/records/19483040 | grep -o '"version":"[^"]*"'
```

Zenodo se dispara con el **release de GitHub**, no con el tag ni con el push. Un tag sin release no
genera registro nuevo.

## Qué DOI citar

| DOI | Qué es | Usar |
|---|---|---|
| **`10.5281/zenodo.19483040`** | **Concept DOI.** Resuelve siempre a la última versión publicada. | ✅ **Éste.** En papers, postulaciones, `CITATION.cff`, sitio web. |
| `10.5281/zenodo.19483041` | Version DOI de **v1.0.0** (abril 2026, `is_last: false`). | Sólo si hace falta pinnear esa versión exacta. |
| `10.5281/zenodo.21478702` | Version DOI de **v2.3.1** (jul 2026). | Sólo para pinnear v2.3.1. |
| `10.5281/zenodo.19543818` | Aparecía en la rama `main` borrada. **Obsoleto.** | ❌ Nunca. |

Regla práctica: **salvo que necesites reproducibilidad exacta de una versión, citá el concept DOI.**
Un version DOI en una postulación congela la plataforma en el estado de esa fecha, que suele ser lo
contrario de lo que se quiere mostrar.

## Verificación rápida del estado

```sh
git remote -v                                   # confirmar los dos remotos
git rev-parse --abbrev-ref --symbolic-full-name @{u}   # a que remoto trackea el branch
gh release list --repo debianalt/nealab --limit 3
grep -E '^(version|doi):' CITATION.cff
```
