"""Consulta de sólo lectura a los parquets publicados, para verificar cifras de los textos.

Uso: python director/consulta.py "SELECT avg(low_bld_pct) FROM p('hex_flood_risk')"
     python director/consulta.py "DESCRIBE SELECT * FROM p('corrientes/hex_flood_risk')"

p('<nombre>') se expande a la URL pública de R2 (r2.dev, sin la protección de
Cloudflare de cdn.spatia.ar): data/<nombre>.parquet. Sólo SELECT/DESCRIBE/SUMMARIZE.
Imprime a lo sumo 50 filas.
"""
import re, sys
import duckdb

R2 = 'https://pub-580c676bec7f4eeb96d7d30559a3cab7.r2.dev/data'


def main():
    sql = ' '.join(sys.argv[1:]).strip()
    if not re.match(r'^(select|with|describe|summarize)\b', sql, re.I) or re.search(r'\b(copy|attach|install|load|create|insert|update|delete|drop|export|pragma|set)\b', sql, re.I):
        sys.exit('Sólo consultas de lectura (SELECT/WITH/DESCRIBE/SUMMARIZE).')
    sql = re.sub(r"p\('([A-Za-z0-9_/.-]+)'\)", lambda m: f"read_parquet('{R2}/{m.group(1).removesuffix('.parquet')}.parquet')", sql)
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    print(con.sql(sql).limit(50))


if __name__ == '__main__':
    main()
