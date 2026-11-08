"""Piezas comunes del archivo de prensa: limpieza del texto (sin menús del HTML) y nombres de los medios para citar.

limpiar_texto() corta los bloques de navegación que los sitios meten adentro del cuerpo de la nota. Sólo corta cuando
la frase abre una línea u oración y con la mayúscula del menú («Lee también», «Te puede interesar»), para no tocar
texto legítimo («también te puede guiar», «mirá también que feo»). El crudo bajado queda intacto: es el respaldo.

  Misiones Online, Misiones Opina, Noticias del 6   «Lee también: <título>», «(Tal vez) Te puede interesar: <título>»
  El Territorio                                     varios «Lee también: …» en la misma línea; «Notas relacionadas:» al final
  MisionesCuatro                                    pie entero: «Dejá un comentario ->», «Seguí leyendo», «Compartir», etiquetas
  Primera Edición, Noticias del 6                   «Te puede interesar» + título en la línea siguiente; «(Ver Notas Relacionadas)»
"""
from __future__ import annotations

import re

NOMBRES = {
    'misionesonline': 'Misiones Online', 'primeraedicion': 'Primera Edición', 'canal12': 'Canal 12 Misiones',
    'noticiasdel6': 'Noticias del 6', 'economis': 'Economis', 'misionesopina': 'Misiones Opina',
    'elterritorio': 'El Territorio', 'lavozdecataratas': 'La Voz de Cataratas', 'misionescuatro': 'MisionesCuatro',
    'gobmisiones': 'Gobierno de Misiones (misiones.gob.ar)',
}
OFICIALES = {'gobmisiones'}

_INICIO = r'(?:(?<=\n)|(?<=^)|(?<=[.!?”"»:])\s*|(?<=\s)|(?<=/))'   # «//Lee también» en Misiones Online
# desde acá hasta el final de la nota no hay más texto propio
_PIE = re.compile(r'(?s)(?:Dejá un comentario\s*->|Suscribite a nuestro canal de YouTube|Seguí leyendo\n|'
                  r'Notas relacionadas:|\(Ver Notas Relacionadas\)\s*\n(?:[^\n.]{5,200}\n?){0,4}$).*$')
# un título de nota relacionada (hasta fin de línea, o hasta el próximo «Lee también», como máximo 220 caracteres)
_TITULO = r'[^\n]{0,220}?(?=\s*(?:Lee también|LEE TAMBIÉN|Leé también)|\n|$)'
_MENU = re.compile(
    rf'{_INICIO}(?:Lee también|LEE TAMBIÉN|Leé también|LEÉ TAMBIÉN)\s*:?\s*{_TITULO}'
    rf'|{_INICIO}(?:Tal vez te puede interesar|Te puede interesar|TE PUEDE INTERESAR|También te puede interesar)'
    rf'\s*:?[ \t]*(?:\n[ \t]*)?[^\n]{{0,220}}'
    rf'|{_INICIO}Mirá también\s*:?\s*[^\n]{{0,220}}'
    rf'|{_INICIO}Leé las últimas noticias sobre el coronavirus[^\n]*')
_RENGLON_SUELTO = re.compile(r'(?m)^[ \t]*(?:Compartir|Twittear|ver en vivo|Compartí esta nota|AMPLIAREMOS|'
                             r'Seguinos en Google (?:Noticias|News)[^\n]*)[ \t]*$\n?')


def actualizar_fuentes(cur) -> None:
    """Nombre para citar y marca de fuente oficial en noticias.fuentes (columnas creadas si faltan)."""
    cur.execute('ALTER TABLE noticias.fuentes ADD COLUMN IF NOT EXISTS nombre text')
    cur.execute('ALTER TABLE noticias.fuentes ADD COLUMN IF NOT EXISTS fuente_oficial boolean DEFAULT false')
    for m, n in NOMBRES.items():
        cur.execute('UPDATE noticias.fuentes SET nombre = %s, fuente_oficial = %s WHERE medio = %s', (n, m in OFICIALES, m))


def limpiar_texto(t: str | None) -> str:
    if not t:
        return t or ''
    t = _PIE.sub('', t)
    t = re.sub(r'(?<=[a-záéíóúñ0-9])(?=(?:Lee también|LEE TAMBIÉN|Leé también))', ' ', t)   # «…MisionesLee también»
    for _ in range(6):   # varios menús seguidos: hasta que no cambie
        u = _MENU.sub(' ', t)
        if u == t:
            break
        t = u
    t = _RENGLON_SUELTO.sub('', t)
    t = re.sub(r'[ \t]{2,}', ' ', t)
    return re.sub(r'\n\s*\n+', '\n', t).strip()


def aplicar() -> None:
    """Una sola vez: limpia noticias.notas.texto en la base (el crudo queda como estaba)."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from build_hexoma import DB, connect
    conn = connect(DB)
    cur = conn.cursor()
    cur.execute('SELECT nota_id, texto FROM noticias.notas')
    cambios = [(limpiar_texto(t), i) for i, t in cur.fetchall() if limpiar_texto(t) != t]
    cur.executemany('UPDATE noticias.notas SET texto = %s WHERE nota_id = %s', cambios)
    conn.commit()
    conn.close()
    print(f'notas limpiadas: {len(cambios):,}')


def probar() -> None:   # casos reales del 7-nov-2026
    casos = [
        ('Moconá (El Soberbio): permanece cerrado. Lee también: La crecida del río Paraná inundó el Club de Pesca '
         'Lee también: Ituzaingó: Prefectura activó el plan\nSigue el texto.', 'Lee también'),
        ('restitución de la menor.Notas relacionadas:Caso Antonella: Defensoría pidió…Caso Antonella: jueza', 'Antonella'),
        ('las vías públicas.\nDejá un comentario ->\nSuscribite a nuestro canal de YouTube\nSeguí leyendo\nDesborde del '
         'A° Yacutinga\nCompartir\nTwittear', 'Yacutinga'),
        ('aclaró el ministro. \n Lee también: La Anmat aprobó el uso de cuatro autotest \n Pase sanitario en Misiones', 'Anmat'),
        ('WhatsApp sigue desarrollando.\n Te puede interesar\n Conocé como sumarte al Canal de Telegram', 'Telegram'),
    ]
    legitimos = ['Cuando eres una persona espiritual te puede guiar el Espíritu Santo y también te puede guiar el diablo',
                 'Pero digo: mirá también que feo puede ser.']
    for t, no_debe in casos:
        r = limpiar_texto(t)
        assert no_debe not in r, (no_debe, r)
        print('ok:', repr(r))
    for t in legitimos:
        assert limpiar_texto(t) == t, limpiar_texto(t)
    print("legítimos intactos")


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--aplicar', action='store_true', help='limpiar noticias.notas.texto en la base (una vez)')
    probar()
    if ap.parse_args().aplicar:
        aplicar()
