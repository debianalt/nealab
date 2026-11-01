"""Microdatos REDATAM (exportados a CSV) → conteos por radio censal, en el formato de Source Cooperative.

Fuente: las bases REDATAM del INDEC exportadas a CSV por Open Redatam
(github.com/pachadotdev/redatam-microdata, release 2.0.2; ARG1991.zip). Cada entidad es un CSV
separado por «;» con su clave `<entidad>_ref_id` y la clave de su padre; las etiquetas de cada
variable están en `<entidad>_labels_<variable>.csv`.

Sale lo mismo que `census-data.parquet` y `metadata.parquet` de
source.coop/nlebovits/censo-argentino, para que build_hexoma_censo.py trate todos los años igual:
    census_nea.parquet   id_geo, prov_code, valor_departamento, etiqueta_departamento, valor_fraccion,
                         valor_radio, codigo_variable, valor_categoria, etiqueta_categoria, conteo
    metadata.parquet     valor_categoria, etiqueta_categoria, codigo_variable, nombre_variable,
                         etiqueta_variable, entidad

Los conteos NO están expandidos: en los radios donde el cuestionario fue una muestra son casos de
la muestra. La calibración por radio la hace build_hexoma_censo.py con los totales oficiales.

Variables:
    PERSONA_<v>   personas, por categoría de cada variable de persona
    HOGAR_<v>     hogares, por categoría de cada variable de hogar y de su vivienda (cada hogar hereda
                  los atributos de la vivienda donde vive: materiales, agua, NBI de la vivienda…)
    VIVIENDA_<v>  viviendas, por categoría de cada variable de vivienda
Se omiten las claves y las variables con más de MAX_CATS categorías en el ámbito (códigos de
localidad, de ocupación…).

Uso (desde neahub/):
    python pipeline/hexoma/redatam_a_radio.py --anio 1991 --csv pipeline/output/hexoma/redatam1991/csv
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / 'pipeline' / 'output' / 'hexoma'
MAX_CATS = 200
PROVS = ['54', '18', '22', '34']  # Misiones, Corrientes, Chaco, Formosa

# Jerarquía de cada base: (archivo, clave, clave del padre, columna del código, columna del nombre)
HIER = {
    '1991': {
        'provincia': ('provin', 'provin_ref_id', None, 'provin', 'nombprov'),
        'departamento': ('depto', 'depto_ref_id', 'provin_ref_id', 'depto', 'nombdepto'),
        'fraccion': ('fraccion', 'fraccion_ref_id', 'depto_ref_id', 'fraccion', None),
        'radio': ('radio', 'radio_ref_id', 'fraccion_ref_id', 'radio', None),
        'segmento': ('segmento', 'segmento_ref_id', 'radio_ref_id', None, None),
        'vivienda': ('vivienda', 'vivienda_ref_id', 'segmento_ref_id', None, None),
        'hogar': ('hogar', 'hogar_ref_id', 'vivienda_ref_id', None, None),
        'persona': ('persona', 'persona_ref_id', 'hogar_ref_id', None, None),
    },
}
# Columnas que no son variables (identificadores dentro de la entidad)
NO_VAR = {'vivienda', 'manzana', 'idhogar', 'idpers', 'segmento'}

# Rótulos de las variables (las bases exportadas no los traen). Los de 1991 salen del
# cuestionario del CNPV 1991; las que no están acá quedan con su nombre.
ETIQUETAS = {
    '1991': {
        'condocup': 'Condición de ocupación de la vivienda', 'vivcolect': 'Tipo de vivienda colectiva',
        'tipoviv': 'Tipo de vivienda', 'canthogar': 'Cantidad de hogares en la vivienda',
        'paredes': 'Material de las paredes', 'techo': 'Material del techo', 'pisos': 'Material de los pisos',
        'electric': 'Electricidad', 'agua': 'Tenencia de agua', 'obtagua': 'Procedencia del agua',
        'cmv23nbi': 'Cantidad de indicadores NBI de la vivienda', 'cmv2ahaci': 'NBI hacinamiento',
        'cmv2btivi': 'NBI tipo de vivienda', 'cmv2cretr': 'NBI condiciones sanitarias (retrete)',
        'cmv2dedes': 'NBI asistencia escolar', 'cmv2ecasu': 'NBI capacidad de subsistencia',
        'cmv08pers': 'Personas en la vivienda', 'catcuarth': 'Cuartos del hogar',
        'combustih': 'Combustible para cocinar', 'inodoroh': 'Inodoro o retrete',
        'desaginh': 'Desagüe del inodoro', 'propvivh': 'Régimen de tenencia', 'cantperh': 'Personas en el hogar',
        'viemergh': 'Vivienda en villa de emergencia', 'cmh12pcto': 'Personas por cuarto',
        'parentes': 'Relación de parentesco', 'sexo': 'Sexo', 'edad': 'Edad', 'jubilacio': 'Percibe jubilación',
        'nacioaca': 'Lugar de nacimiento', 'vivia5aca': 'Residencia hace 5 años', 'obrasocial': 'Obra social',
        'planmedico': 'Plan médico o mutual', 'leescribe': 'Sabe leer y escribir',
        'asistencia': 'Asistencia escolar', 'nivelasist': 'Nivel que cursa', 'nivelcurso': 'Nivel más alto que cursó',
        'complcurso': 'Completó el nivel', 'condactiv': 'Condición de actividad', 'relacilabo': 'Categoría ocupacional',
        'descjubil': 'Descuento jubilatorio', 'ecivil': 'Estado civil', 'hijnacviv': 'Hijos nacidos vivos',
        'cobsalud': 'Cobertura de salud', 'pesoperson': 'Peso de la persona (muestra)',
        'areaponde': 'Área de ponderación (muestra)',
    },
}


def lab(csv: Path, ent: str, var: str) -> Path:
    return csv / f'{ent}_labels_{var}.csv'


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--anio', required=True, choices=sorted(HIER))
    ap.add_argument('--csv', required=True, type=Path, help='carpeta con los CSV de la base')
    args = ap.parse_args()
    t0 = time.time()
    h, csv = HIER[args.anio], args.csv
    out = OUTPUT / f'censo{args.anio}'
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    def src(ent: str) -> str:
        return f"read_csv('{(csv / (h[ent][0] + '.csv')).as_posix()}', delim=';', all_varchar=true, header=true)"

    pv, dp, fr, ra, sg = (h[k] for k in ('provincia', 'departamento', 'fraccion', 'radio', 'segmento'))
    con.execute(f"""
        CREATE TABLE geo AS
        SELECT s.{sg[1]} AS seg, p.{pv[3]} AS prov, p.{pv[4]} AS prov_nombre,
               d.{dp[3]} AS depto, d.{dp[4]} AS depto_nombre, f.{fr[3]} AS fraccion, r.{ra[3]} AS radio,
               p.{pv[3]} || d.{dp[3]} || f.{fr[3]} || r.{ra[3]} AS id_geo
        FROM {src('segmento')} s
        JOIN {src('radio')} r ON r.{ra[1]} = s.{sg[2]}
        JOIN {src('fraccion')} f ON f.{fr[1]} = r.{ra[2]}
        JOIN {src('departamento')} d ON d.{dp[1]} = f.{fr[2]}
        JOIN {src('provincia')} p ON p.{pv[1]} = d.{dp[2]}
        WHERE p.{pv[3]} IN ({', '.join(repr(p) for p in PROVS)})
    """)
    vi, ho, pe = h['vivienda'], h['hogar'], h['persona']
    con.execute(f"CREATE TABLE viv AS SELECT g.id_geo, v.* FROM {src('vivienda')} v JOIN geo g ON g.seg = v.{vi[2]}")
    con.execute(f"CREATE TABLE hog AS SELECT v.id_geo, h.* FROM {src('hogar')} h JOIN viv v ON v.{vi[1]} = h.{ho[2]}")
    # cada hogar hereda las variables de su vivienda
    vcols = [c for c in con.execute('SELECT * FROM viv LIMIT 0').fetchdf().columns
             if c not in ('id_geo',) and not c.endswith('_ref_id') and c not in NO_VAR]
    con.execute(f"CREATE TABLE hogv AS SELECT h.id_geo, {', '.join('v.' + c for c in vcols)} "
                f"FROM hog h JOIN viv v ON v.{vi[1]} = h.{ho[2]}")
    con.execute(f"CREATE TABLE per AS SELECT h.id_geo, p.* FROM {src('persona')} p JOIN hog h ON h.{ho[1]} = p.{pe[2]}")
    for t in ('geo', 'viv', 'hog', 'per'):
        print(f'  {t}: {con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]:,}', flush=True)

    con.execute("""CREATE TABLE conteos (id_geo VARCHAR, codigo_variable VARCHAR, entidad VARCHAR,
                                         var VARCHAR, valor_categoria VARCHAR, conteo BIGINT)""")
    plan = [('VIVIENDA', 'vivienda', 'viv'), ('HOGAR', 'vivienda', 'hogv'), ('HOGAR', 'hogar', 'hog'),
            ('PERSONA', 'persona', 'per')]
    omitidas = []
    for pref, ent, table in plan:
        cols = [c for c in con.execute(f'SELECT * FROM {table} LIMIT 0').fetchdf().columns
                if c != 'id_geo' and not c.endswith('_ref_id') and c not in NO_VAR]
        for c in cols:
            n = con.execute(f'SELECT count(DISTINCT "{c}") FROM {table}').fetchone()[0]
            if n > MAX_CATS:
                omitidas.append(f'{pref}_{c.upper()} ({n})')
                continue
            con.execute(f"""INSERT INTO conteos
                SELECT id_geo, '{pref}_{c.upper()}', '{ent}', '{c}', coalesce("{c}", ''), count(*)
                FROM {table} GROUP BY ALL""")
    print('  omitidas por tener más de', MAX_CATS, 'categorías:', ', '.join(sorted(set(omitidas))), flush=True)

    # rótulos de categoría: <entidad>_labels_<var>.csv (valor;descripción)
    con.execute('CREATE TABLE etiquetas (entidad VARCHAR, var VARCHAR, valor_categoria VARCHAR, etiqueta VARCHAR)')
    for ent, var in con.execute('SELECT DISTINCT entidad, var FROM conteos').fetchall():
        f = lab(csv, ent, var)
        if f.exists():
            con.execute(f"""INSERT INTO etiquetas
                SELECT '{ent}', '{var}', column0, column1
                FROM read_csv('{f.as_posix()}', delim=';', all_varchar=true, header=false, skip=1)""")
    et = ETIQUETAS.get(args.anio, {})
    con.execute('CREATE TABLE etiq_var (var VARCHAR, etiqueta VARCHAR)')
    if et:
        con.executemany('INSERT INTO etiq_var VALUES (?, ?)', list(et.items()))

    con.execute(f"""COPY (
        SELECT c.id_geo, g.prov::BIGINT AS prov_code, any_value(g.depto) AS valor_departamento,
               any_value(g.depto_nombre) AS etiqueta_departamento, any_value(g.fraccion) AS valor_fraccion,
               any_value(g.radio) AS valor_radio, c.codigo_variable, c.valor_categoria,
               any_value(e.etiqueta) AS etiqueta_categoria, sum(c.conteo)::BIGINT AS conteo
        FROM conteos c
        JOIN (SELECT DISTINCT id_geo, prov, depto, depto_nombre, fraccion, radio FROM geo) g USING (id_geo)
        LEFT JOIN etiquetas e ON e.entidad = c.entidad AND e.var = c.var AND e.valor_categoria = c.valor_categoria
        GROUP BY c.id_geo, g.prov, c.codigo_variable, c.valor_categoria
    ) TO '{(out / 'census_nea.parquet').as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
    con.execute(f"""COPY (
        SELECT DISTINCT c.valor_categoria, e.etiqueta AS etiqueta_categoria, c.codigo_variable,
               c.var AS nombre_variable, coalesce(ev.etiqueta, c.var) AS etiqueta_variable,
               split_part(c.codigo_variable, '_', 1) AS entidad
        FROM conteos c
        LEFT JOIN etiquetas e ON e.entidad = c.entidad AND e.var = c.var AND e.valor_categoria = c.valor_categoria
        LEFT JOIN etiq_var ev ON ev.var = c.var
    ) TO '{(out / 'metadata.parquet').as_posix()}' (FORMAT parquet)""")
    n = con.execute(f"SELECT count(*), count(DISTINCT id_geo), count(DISTINCT codigo_variable) "
                    f"FROM '{(out / 'census_nea.parquet').as_posix()}'").fetchone()
    print(f'  {out / "census_nea.parquet"}: {n[0]:,} filas, {n[1]:,} radios, {n[2]} variables', flush=True)
    print(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
