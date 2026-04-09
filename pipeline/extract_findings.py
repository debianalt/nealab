"""
Extract 5 citable findings from Spatia data for publications and social media.
Queries parquets in pipeline/output/ using DuckDB.
"""

import duckdb
import os

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")


def main():
    conn = duckdb.connect()

    print("=" * 70)
    print("  SPATIA — 5 HALLAZGOS CITEABLES PARA MISIONES")
    print("=" * 70)

    # ── Finding 1: Flood risk ────────────────────────────────────────────
    print("\n\n### FINDING 1: RIESGO DE INUNDACION")
    print("-" * 50)

    flood = conn.execute(f"""
        SELECT
            type_label,
            COUNT(*) AS n_hex,
            ROUND(COUNT(*) * 5.16, 0) AS area_km2,
            ROUND(AVG(flood_risk_score), 1) AS avg_score,
            ROUND(AVG(jrc_occurrence), 1) AS avg_occurrence,
            ROUND(AVG(jrc_recurrence), 1) AS avg_recurrence
        FROM read_parquet('{OUTPUT_DIR}/hex_flood_risk.parquet')
        WHERE type_label IS NOT NULL AND type_label != 'Sin riesgo detectado'
        GROUP BY type_label
        ORDER BY avg_score DESC
    """).fetchdf()
    print(flood.to_string(index=False))

    total_risk = conn.execute(f"""
        SELECT
            COUNT(*) AS total_hex,
            COUNT(*) FILTER (WHERE flood_risk_score > 50) AS high_risk_hex,
            ROUND(COUNT(*) FILTER (WHERE flood_risk_score > 50) * 5.16, 0) AS high_risk_km2,
            (SELECT COUNT(*) FROM read_parquet('{OUTPUT_DIR}/hex_flood_risk.parquet')) AS total_all,
            ROUND(COUNT(*) FILTER (WHERE flood_risk_score > 50) * 100.0 /
                  (SELECT COUNT(*) FROM read_parquet('{OUTPUT_DIR}/hex_flood_risk.parquet')), 1) AS pct_high_risk
        FROM read_parquet('{OUTPUT_DIR}/hex_flood_risk.parquet')
        WHERE type_label IS NOT NULL AND type_label != 'Sin riesgo detectado'
    """).fetchone()
    print(f"\nTotal hexagonos con riesgo detectado: {total_risk[0]:,}")
    print(f"Hexagonos con riesgo alto (score > 50): {total_risk[1]:,} ({total_risk[4]}% del territorio)")
    print(f"Area con riesgo alto: {total_risk[2]:,.0f} km2")

    # ── Finding 2: Health access gap ─────────────────────────────────────
    print("\n\n### FINDING 2: BRECHA DE ACCESO A SALUD")
    print("-" * 50)

    health = conn.execute(f"""
        WITH dept AS (
            SELECT
                SUBSTRING(redcode, 1, 5) AS deptcode,
                CASE SUBSTRING(redcode, 1, 5)
                    WHEN '54007' THEN 'Capital'
                    WHEN '54014' THEN 'Apostoles'
                    WHEN '54021' THEN 'Cainguas'
                    WHEN '54028' THEN 'Candelaria'
                    WHEN '54035' THEN 'Concepcion'
                    WHEN '54042' THEN 'Eldorado'
                    WHEN '54049' THEN 'G.M.Belgrano'
                    WHEN '54056' THEN 'Guarani'
                    WHEN '54063' THEN 'Iguazu'
                    WHEN '54070' THEN 'L.N.Alem'
                    WHEN '54077' THEN 'L.G.San Martin'
                    WHEN '54084' THEN 'Montecarlo'
                    WHEN '54091' THEN 'Obera'
                    WHEN '54098' THEN '25 de Mayo'
                    WHEN '54105' THEN 'San Ignacio'
                    WHEN '54112' THEN 'San Javier'
                    WHEN '54119' THEN 'San Pedro'
                    ELSE 'Otro'
                END AS dept_name,
                dist_nearest_hospital_km,
                total_personas
            FROM read_parquet('{OUTPUT_DIR}/radio_stats_master.parquet')
        )
        SELECT
            dept_name,
            COUNT(*) AS n_radios,
            SUM(total_personas) AS poblacion,
            ROUND(AVG(dist_nearest_hospital_km), 1) AS avg_dist_hospital_km,
            ROUND(MAX(dist_nearest_hospital_km), 1) AS max_dist_hospital_km
        FROM dept
        GROUP BY dept_name
        ORDER BY avg_dist_hospital_km DESC
    """).fetchdf()
    print(health.to_string(index=False))

    capital_avg = health[health['dept_name'] == 'Capital']['avg_dist_hospital_km'].values[0]
    rural_top3 = health.head(3)['avg_dist_hospital_km'].mean()
    ratio = round(rural_top3 / max(capital_avg, 0.1), 1)
    print(f"\nDistancia promedio en Capital: {capital_avg} km")
    print(f"Distancia promedio en los 3 deptos mas alejados: {round(rural_top3, 1)} km")
    print(f"Ratio rural/urbano: {ratio}x")

    # ── Finding 3: Service deprivation ───────────────────────────────────
    print("\n\n### FINDING 3: PRIVACION MULTIDIMENSIONAL DE SERVICIOS")
    print("-" * 50)

    depriv = conn.execute(f"""
        SELECT
            CASE
                WHEN score >= 70 THEN 'Severa (>=70)'
                WHEN score >= 50 THEN 'Alta (50-70)'
                WHEN score >= 30 THEN 'Moderada (30-50)'
                ELSE 'Baja (<30)'
            END AS categoria,
            COUNT(*) AS n_hex,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM read_parquet('{OUTPUT_DIR}/sat_service_deprivation.parquet')), 1) AS pct,
            ROUND(AVG(c_nbi), 1) AS avg_nbi,
            ROUND(AVG(c_hacinamiento), 1) AS avg_hacinamiento,
            ROUND(AVG(c_combustible), 1) AS avg_combustible,
            ROUND(AVG(c_sin_techo), 1) AS avg_sin_techo
        FROM read_parquet('{OUTPUT_DIR}/sat_service_deprivation.parquet')
        GROUP BY categoria
        ORDER BY categoria DESC
    """).fetchdf()
    print(depriv.to_string(index=False))

    severe = depriv[depriv['categoria'] == 'Severa (>=70)']
    if len(severe) > 0:
        print(f"\nHexagonos con privacion severa: {severe['n_hex'].values[0]:,} ({severe['pct'].values[0]}%)")
        print(f"En estas zonas: NBI promedio {severe['avg_nbi'].values[0]}%, hacinamiento {severe['avg_hacinamiento'].values[0]}%, combustible precario {severe['avg_combustible'].values[0]}%")

    # ── Finding 4: Deforestation vs agricultural potential ───────────────
    print("\n\n### FINDING 4: CONFLICTO DEFORESTACION - POTENCIAL AGRICOLA")
    print("-" * 50)

    conflict = conn.execute(f"""
        WITH joined AS (
            SELECT
                a.h3index,
                a.score AS agri_score,
                e.score AS envr_score,
                e.c_fire,
                e.c_deforest
            FROM read_parquet('{OUTPUT_DIR}/sat_agri_potential.parquet') a
            JOIN read_parquet('{OUTPUT_DIR}/sat_environmental_risk.parquet') e
                ON a.h3index = e.h3index
        )
        SELECT
            COUNT(*) AS total_hex,
            COUNT(*) FILTER (WHERE agri_score >= 60) AS high_agri,
            COUNT(*) FILTER (WHERE envr_score >= 60) AS high_risk,
            COUNT(*) FILTER (WHERE agri_score >= 60 AND envr_score >= 60) AS conflict_zone,
            ROUND(COUNT(*) FILTER (WHERE agri_score >= 60 AND envr_score >= 60) * 100.0 /
                  NULLIF(COUNT(*) FILTER (WHERE agri_score >= 60), 0), 1) AS pct_conflict,
            ROUND(COUNT(*) FILTER (WHERE agri_score >= 60 AND envr_score >= 60) * 0.153, 0) AS conflict_km2,
            ROUND(AVG(c_deforest) FILTER (WHERE agri_score >= 60 AND envr_score >= 60), 3) AS avg_deforest_conflict,
            ROUND(AVG(c_fire) FILTER (WHERE agri_score >= 60 AND envr_score >= 60), 3) AS avg_fire_conflict
        FROM joined
    """).fetchone()
    print(f"Total hexagonos analizados: {conflict[0]:,}")
    print(f"Alto potencial agricola (score >= 60): {conflict[1]:,}")
    print(f"Alto riesgo ambiental (score >= 60): {conflict[2]:,}")
    print(f"ZONA DE CONFLICTO (ambos >= 60): {conflict[3]:,} hexagonos ({conflict[4]}% del alto potencial)")
    print(f"Area en conflicto: ~{conflict[5]:,.0f} km2")
    print(f"Deforestacion promedio en zona de conflicto: {conflict[6]}")
    print(f"Incidencia de fuego promedio en zona de conflicto: {conflict[7]}")

    # Also get department-level deforestation from radio_stats_master
    defor_dept = conn.execute(f"""
        WITH dept AS (
            SELECT
                CASE SUBSTRING(redcode, 1, 5)
                    WHEN '54007' THEN 'Capital'
                    WHEN '54014' THEN 'Apostoles'
                    WHEN '54021' THEN 'Cainguas'
                    WHEN '54028' THEN 'Candelaria'
                    WHEN '54035' THEN 'Concepcion'
                    WHEN '54042' THEN 'Eldorado'
                    WHEN '54049' THEN 'G.M.Belgrano'
                    WHEN '54056' THEN 'Guarani'
                    WHEN '54063' THEN 'Iguazu'
                    WHEN '54070' THEN 'L.N.Alem'
                    WHEN '54077' THEN 'L.G.San Martin'
                    WHEN '54084' THEN 'Montecarlo'
                    WHEN '54091' THEN 'Obera'
                    WHEN '54098' THEN '25 de Mayo'
                    WHEN '54105' THEN 'San Ignacio'
                    WHEN '54112' THEN 'San Javier'
                    WHEN '54119' THEN 'San Pedro'
                    ELSE 'Otro'
                END AS dept_name,
                ha_deforestadas_total,
                frac_bosque_nativo
            FROM read_parquet('{OUTPUT_DIR}/radio_stats_master.parquet')
        )
        SELECT
            dept_name,
            ROUND(SUM(ha_deforestadas_total), 0) AS total_ha_deforestadas,
            ROUND(AVG(frac_bosque_nativo) * 100, 1) AS avg_pct_bosque_nativo
        FROM dept
        GROUP BY dept_name
        ORDER BY total_ha_deforestadas DESC
        LIMIT 5
    """).fetchdf()
    print(f"\nTop 5 departamentos por deforestacion total (ha):")
    print(defor_dept.to_string(index=False))

    # ── Finding 5: Digital divide ────────────────────────────────────────
    print("\n\n### FINDING 5: BRECHA DIGITAL Y EDUCATIVA")
    print("-" * 50)

    digital = conn.execute(f"""
        SELECT
            ROUND(AVG(pct_computadora), 1) AS avg_computadora_prov,
            ROUND(AVG(pct_computadora) FILTER (WHERE pct_nbi <= 10), 1) AS avg_comp_low_nbi,
            ROUND(AVG(pct_computadora) FILTER (WHERE pct_nbi > 10 AND pct_nbi <= 30), 1) AS avg_comp_med_nbi,
            ROUND(AVG(pct_computadora) FILTER (WHERE pct_nbi > 30), 1) AS avg_comp_high_nbi,
            COUNT(*) FILTER (WHERE pct_nbi > 30) AS n_radios_high_nbi,
            ROUND(AVG(pct_sin_instruccion) FILTER (WHERE pct_nbi > 30), 1) AS avg_sin_instr_high_nbi,
            ROUND(AVG(pct_secundario_comp), 1) AS avg_sec_comp,
            ROUND(AVG(pct_secundario_comp) FILTER (WHERE pct_nbi > 30), 1) AS avg_sec_comp_high_nbi,
            ROUND(CORR(pct_computadora, pct_nbi), 3) AS corr_comp_nbi
        FROM read_parquet('{OUTPUT_DIR}/radio_data/censo2022_variables.parquet')
    """).fetchone()
    print(f"Acceso a computadora provincial: {digital[0]}%")
    print(f"  En radios con NBI <= 10%: {digital[1]}%")
    print(f"  En radios con NBI 10-30%: {digital[2]}%")
    print(f"  En radios con NBI > 30%: {digital[3]}%")
    print(f"Radios con NBI > 30%: {digital[4]}")
    print(f"Sin instruccion promedio en radios NBI > 30%: {digital[5]}%")
    print(f"Secundario completo provincial: {digital[6]}%")
    print(f"Secundario completo en radios NBI > 30%: {digital[7]}%")
    print(f"Correlacion computadora-NBI: r = {digital[8]}")

    conn.close()
    print("\n" + "=" * 70)
    print("  DONE — 5 findings extracted")
    print("=" * 70)


if __name__ == "__main__":
    main()
