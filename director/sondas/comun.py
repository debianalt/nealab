"""Piezas comunes de las sondas: reglas, hallazgos con huella estable, chequeos de texto."""
import hashlib, json, re
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
DIRECTOR = RAIZ / 'director'

# Errores de consola que no son de Spatia o que produce el propio runner.
RUIDO_CONSOLA = [
    r'spatia\.ar/_app/version\.json',          # el apex desafía al runner; www sirve igual
    r'favicon\.ico',
    r'challenges\.cloudflare\.com',
    r'Failed to load resource: the server responded with a status of 403 \(\)$',
    r'net::ERR_ABORTED',                        # navegación que corta descargas en curso
]


def reglas():
    import yaml
    return yaml.safe_load((DIRECTOR / 'reglas.yaml').read_text(encoding='utf-8'))


_R = None


def _reglas():
    global _R
    if _R is None:
        _R = reglas()
    return _R


def revisar_texto(texto, contexto_extra=''):
    """Frases prohibidas y basura visible en un texto. Devuelve [(tipo, severidad, detalle)]."""
    R = _reglas(); out = []
    if not texto: return out
    for fr in R.get('frases_prohibidas', []):
        for m in re.finditer(fr['re'], texto, re.I):
            frag = texto[max(0, m.start() - 40): m.end() + 40].replace('\n', ' ')
            if fr.get('excepto_re') and re.search(fr['excepto_re'], frag): continue
            out.append(('frase_prohibida', 'media', f"«{m.group(0)}» ({fr['motivo']}): …{frag}…"))
    for b in R.get('basura_visible', []):
        for m in re.finditer(b['re'], texto):
            frag = texto[max(0, m.start() - 40): m.end() + 40].replace('\n', ' ')
            if b.get('excepto_re') and re.search(b['excepto_re'], m.group(0) if b['motivo'] != 'clave i18n sin traducir' else frag): continue
            out.append(('texto_basura', 'media', f"{b['motivo']}: «{m.group(0)}» en …{frag}…"))
    return out


_NUM = re.compile(r'[−\-+]?\d[\d.,]*')


def numeros(texto):
    """Números de un texto en castellano o inglés: '1.234,5' '12,5' '12.5' '−3' → floats."""
    out = []
    for tok in _NUM.findall(texto or ''):
        s = tok.replace('−', '-').rstrip('.,')
        if not re.search(r'\d', s): continue
        if ',' in s and '.' in s:
            if s.rfind(',') > s.rfind('.'): s = s.replace('.', '').replace(',', '.')
            else: s = s.replace(',', '')
        elif ',' in s:
            s = s.replace(',', '.') if re.fullmatch(r'[-+]?\d+,\d{1,2}', s) or not re.fullmatch(r'[-+]?\d{1,3}(,\d{3})+', s) else s.replace(',', '')
        elif '.' in s and re.fullmatch(r'[-+]?\d{1,3}(\.\d{3})+', s):
            s = s.replace('.', '')
        try: out.append(float(s))
        except ValueError: pass
    return out


def huella(tipo, ctx, detalle_clave):
    base = '|'.join(str(x) for x in (tipo, ctx.get('territorio'), ctx.get('capa'), ctx.get('vista'),
                                     ctx.get('pestana'), ctx.get('grafico'), ctx.get('pagina'), ctx.get('idioma'), detalle_clave))
    return hashlib.sha1(base.encode('utf-8')).hexdigest()[:12]


class Hallazgos:
    def __init__(self, out, nombre):
        self.out = Path(out); self.nombre = nombre; self.items = {}; self._pend_captura = None
        (self.out / 'capturas').mkdir(parents=True, exist_ok=True)

    def add(self, tipo, severidad, ctx, detalle, clave=None):
        detalle = re.sub(r'\s*\n\s*', ' | ', detalle).strip()
        # La huella no lleva cifras: el mismo defecto con otro valor es el mismo hallazgo.
        h = huella(tipo, ctx, clave if clave is not None else re.sub(r'[\d.,]+', '#', detalle)[:80])
        if h in self.items: return
        self.items[h] = {'huella': h, 'tipo': tipo, 'severidad': severidad, 'sonda': self.nombre,
                         **{k: v for k, v in ctx.items() if v is not None}, 'detalle': detalle}
        self._pend_captura = h

    def revisar(self, lista, ctx, donde):
        for tipo, sev, det in lista:
            self.add(tipo, sev, {**ctx, 'donde': donde}, det, clave=det[:60])

    def consola(self, errores, ctx):
        vistos = set()
        for e in errores:
            if any(re.search(p, e) for p in RUIDO_CONSOLA): continue
            k = re.sub(r'\?.*', '', e)[:160]
            if k in vistos: continue
            vistos.add(k)
            sev = 'alta' if e.startswith(('PAGEERROR', 'MAP ERROR', 'HTTP 5', 'HTTP 404')) else 'media'
            self.add('consola', sev, ctx, e[:400], clave=re.sub(r'[0-9a-f]{8,}', '#', k))
        errores.clear()

    async def captura(self, page, ctx):
        nombre = re.sub(r'[^a-z0-9_]+', '_', f"{ctx.get('territorio')}_{ctx.get('capa')}_{ctx.get('vista')}".lower())
        ruta = self.out / 'capturas' / f'{nombre}.png'
        try:
            await page.screenshot(path=str(ruta), timeout=60000, animations='disabled')
        except Exception:
            return None
        for it in self.items.values():
            if it.get('territorio') == ctx.get('territorio') and it.get('capa') == ctx.get('capa') and it.get('vista') == ctx.get('vista') and 'captura' not in it:
                it['captura'] = f'capturas/{ruta.name}'
        return ruta

    async def captura_si(self, page, ctx):
        """Captura de la vista sólo si dejó hallazgos sin captura."""
        if any(it.get('territorio') == ctx.get('territorio') and it.get('capa') == ctx.get('capa')
               and it.get('vista') == ctx.get('vista') and 'captura' not in it for it in self.items.values()):
            await self.captura(page, ctx)

    def total(self):
        return len(self.items)

    def guardar(self):
        ruta = self.out / f'hallazgos_{self.nombre}.json'
        ruta.write_text(json.dumps(list(self.items.values()), ensure_ascii=False, indent=1), encoding='utf-8')
        return ruta
