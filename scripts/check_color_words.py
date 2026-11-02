"""Fail if a «Cómo leer» text names a map colour.

The map has two themes with opposite conventions (Oscuro: more = brighter;
Claro, the default: more = darker, print convention), and the palette is
tuned over time (sello.ts). A text that says «rojo = alta» is wrong on one
theme at least and goes stale on the next palette change. Texts name the
classes and point to the legend, which draws the active theme's colours.
Decided with the author on 2-nov-2026.

Scope: the howToRead texts in content/methodology.ts and config.ts, and the
*howToRead* / guide*Body keys in i18n (es, en, pt; gn is checked against its
own colour words).

Usage: python scripts/check_color_words.py   (exit 1 on any hit)
"""
import io, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FILES = ['src/lib/content/methodology.ts', 'src/lib/config.ts', 'src/lib/stores/i18n.svelte.ts']
KEY = re.compile(r"^\s*['\"]?([\w.]+)['\"]?\s*:\s*\{")
SCOPE = re.compile(r'howToRead|guide\w*Body', re.I)
STR = re.compile(r"\b(es|en|pt|gn)\s*:\s*(['\"`])((?:\\.|(?!\2).)*)\2")
WORDS = {
	'es': r'oscur[oa]s?|azul(es)?|amarill[oa]s?|roj[oa]s?|verdes?|naranjas?|violetas?|púrpuras?|morad[oa]s?|gris(es)?|blanc[oa]s?|negr[oa]s?|dorad[oa]s?|celestes?|cyan|marr[oó]n|pard[oa]s?|ámbar|cálid[oa]s?',
	'en': r'dark|blue|yellow|red|green(er)?|orange|purple|gr[ae]y|white|black|golden|brown|amber|cyan|warm(er)?|cool(er)?',
	'pt': r'escur[oa]s?|azu(l|is)|amarel[oa]s?|vermelh[oa]s?|verdes?|laranjas?|rox[oa]s?|cinzas?|branc[oa]s?|pret[oa]s?|dourad[oa]s?|ciano|marrom|quentes?|frias?',
	'gn': r"hũ|hovy|sa'yju|pytã|hovyũ|morotĩ|sovy|pyta",
}
WORDS = {k: re.compile(rf'\b({v})\b', re.I) for k, v in WORDS.items()}

def main() -> int:
	hits = []
	for f in FILES:
		key = None
		for n, line in enumerate(io.open(ROOT / f, encoding='utf-8'), 1):
			m = KEY.match(line)
			if m:
				key = m.group(1)
			if not key or not SCOPE.search(key):
				continue
			for lang, _, text in STR.findall(line):
				found = sorted({w[0].lower() for w in WORDS[lang].findall(text)})
				if found:
					hits.append(f'{f}:{n} [{key} {lang}] {", ".join(found)}')
	for h in hits:
		print(h)
	print(f'{len(hits)} texto(s) «Cómo leer» nombran colores del mapa' if hits else 'ok: ningún «Cómo leer» nombra colores')
	return 1 if hits else 0

if __name__ == '__main__':
	sys.exit(main())
