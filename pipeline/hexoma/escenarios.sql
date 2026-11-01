-- Hexoma — escenarios de investigación (NEA argentino: Misiones, Corrientes, Chaco, Formosa)
-- Base local `hexoma` (ver README.md). Correr con:
--     psql -U postgres -d hexoma -f pipeline/hexoma/escenarios.sql
-- o pegar cada consulta en psql / DBeaver. Tiempos medidos el 31-oct-2026 en la base cargada ese día.
--
-- Recordatorios que valen para todas:
--   * Sin dato = NULL, nunca cero: avg() y percentile_cont() ya los saltean; count(*) NO.
--   * v_hexoma muestra los valores como la ficha de spatia.ar ((rawCol ?? col) × scale).
--   * v_hexoma_rangos: <capa>__<variable>__pr_<dep|prov|reg> = % de celdas con valor
--     estrictamente menor en ese ámbito (la ficha: «más alto que N de cada 100»).
--   * Las censales (INDEC) son comparables entre provincias argentinas, nunca con PY/BR.
--   * Área de cada celda: h3_cell_area(h3, 'km^2') (res 9 ≈ 0,09–0,11 km² en el NEA).


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Frontera de deforestación aislada
-- ¿Dónde se pierde bosque más rápido (top 10 % de la región) en las celdas más
-- lejanas de la capital de su propio departamento (por encima del P75 del depto),
-- y cuánto potencial agrícola tienen?
-- Ámbitos: región para la tasa (capa satelital comparable entre territorios),
-- departamento para el aislamiento (lejos «para ese departamento»).
-- Medido: 30 departamentos (de 76), 14 s. Encabezan Patiño y Bermejo (Formosa).
-- ─────────────────────────────────────────────────────────────────────────────
WITH p75 AS (
    SELECT ambito_id::smallint AS depto_id, p75
    FROM distribuciones
    WHERE capa = 'accessibility' AND variable = 'travel_min_capital' AND ambito = 'departamento'
),
celdas AS (
    SELECT v.h3, v.territorio, v.departamento,
           v.deforestation_dynamics__c_loss_rate AS perdida,
           v.accessibility__travel_min_capital   AS minutos,
           ag.score                               AS potencial_agricola
    FROM v_hexoma v
    JOIN v_hexoma_rangos r USING (h3, territorio)
    JOIN territorios t ON t.territorio = v.territorio
    JOIN capa_accessibility a ON a.h3 = v.h3 AND a.territorio_id = t.territorio_id AND a.principal
    JOIN p75 ON p75.depto_id = a.depto_id
    LEFT JOIN capa_agri_potential ag ON ag.h3 = v.h3 AND ag.territorio_id = t.territorio_id AND ag.principal
    WHERE r.deforestation_dynamics__c_loss_rate__pr_reg >= 90
      AND v.accessibility__travel_min_capital > p75.p75
)
SELECT territorio, departamento,
       count(*)                                          AS hexagonos,
       round(sum(h3_cell_area(h3, 'km^2'))::numeric, 1)  AS km2,
       round(avg(perdida)::numeric, 2)                   AS perdida_media_pct_anio,
       round(avg(minutos)::numeric)                      AS minutos_a_capital,
       round(avg(potencial_agricola)::numeric, 1)        AS potencial_agricola_100
FROM celdas
GROUP BY 1, 2
ORDER BY km2 DESC
LIMIT 30;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Inundación y carencias (justicia ambiental)
-- ¿Cuánta gente vive en celdas con inundación histórica (JRC, presencia > 0) Y con
-- hogares con NBI en el cuartil más alto de su provincia? ¿Qué parte de la población
-- censada del departamento es?
-- Ámbito: provincia para el NBI (capa censal: comparable entre provincias AR).
-- Población: censo 2022 por celda (capa_censo_temporal.pob_cnt_2022, dasimétrico).
-- Las celdas sin edificios no tienen NBI (NULL): no entran como «sin carencia».
-- Medido: 11 s. Muestra: Capital (Misiones) 6.067 personas (1,6 %); Saladas y Concepción
-- (Corrientes) ~15 % de su población con dato.
-- ─────────────────────────────────────────────────────────────────────────────
WITH celdas AS (
    SELECT v.territorio, v.departamento, ct.pob_cnt_2022 AS pob,
           (v.flood_risk__jrc_occurrence > 0
            AND r.service_deprivation__c_nbi__pr_prov >= 75) AS expuesta
    FROM v_hexoma v
    JOIN v_hexoma_rangos r USING (h3, territorio)
    JOIN territorios t ON t.territorio = v.territorio
    JOIN capa_censo_temporal ct ON ct.h3 = v.h3 AND ct.territorio_id = t.territorio_id
    WHERE v.service_deprivation__c_nbi IS NOT NULL
)
SELECT territorio, departamento,
       round(sum(pob) FILTER (WHERE expuesta))                           AS poblacion_expuesta,
       round(sum(pob))                                                   AS poblacion_con_dato_nbi,
       round((100 * sum(pob) FILTER (WHERE expuesta) / nullif(sum(pob), 0))::numeric, 1) AS pct_expuesta
FROM celdas
GROUP BY 1, 2
HAVING sum(pob) FILTER (WHERE expuesta) > 0
ORDER BY poblacion_expuesta DESC
LIMIT 25;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. EUDR: ¿qué hay hoy donde se perdió bosque después de 2020?
-- En las celdas con pérdida post-2020 (> 0 %), el uso del suelo dominante actual
-- (Dynamic World: la fracción mayor) y cuánto de esa pérdida coincide con fuego
-- sobre vegetación nativa. Por provincia y uso dominante.
-- Lectura: «cultivos/pasturas» sugiere conversión; «arbustos» puede ser
-- regeneración o degradación; «árboles» (Dynamic World trees: selva paranaense en Misiones,
-- bosque chaqueño al oeste) = la celda perdió una parte y conserva cobertura arbórea.
-- loss_post_2020_pct es % del área de la celda (como el summary del sitio): km² = área × %/100.
-- Medido: 14 s. Muestra: Chaco 2.036 km² perdidos en celdas que siguen arboladas y 764 en
-- celdas hoy de cultivos (44 % con fuego sobre nativa).
-- ─────────────────────────────────────────────────────────────────────────────
WITH celdas AS (
    SELECT v.territorio, v.h3, v.eudr__loss_post_2020_pct AS perdida,
           v.eudr__fire_native_post_2020_pct AS fuego_nativa,
           (ARRAY['árboles', 'cultivos', 'pasturas', 'arbustos', 'construido', 'agua', 'veg. inundada', 'suelo desnudo'])[
               array_position(
                   ARRAY[land_use__frac_trees, land_use__frac_crops, land_use__frac_grass, land_use__frac_shrub,
                         land_use__frac_built, land_use__frac_water, land_use__frac_flooded, land_use__frac_bare],
                   greatest(land_use__frac_trees, land_use__frac_crops, land_use__frac_grass, land_use__frac_shrub,
                            land_use__frac_built, land_use__frac_water, land_use__frac_flooded, land_use__frac_bare))
           ] AS uso_dominante
    FROM v_hexoma v
    WHERE v.eudr__loss_post_2020_pct > 0
)
SELECT territorio, coalesce(uso_dominante, 'sin dato de uso') AS uso_dominante,
       count(*)                                                AS hexagonos,
       round(sum(h3_cell_area(h3, 'km^2') * perdida / 100)::numeric, 1) AS km2_perdidos,
       round(avg(perdida)::numeric, 1)                         AS perdida_media_pct,
       round(100.0 * count(*) FILTER (WHERE fuego_nativa > 0) / count(*), 1) AS pct_celdas_con_fuego_nativa
FROM celdas
GROUP BY 1, 2
ORDER BY 1, km2_perdidos DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Población 1991–2022 según aislamiento
-- ¿Las zonas más aisladas pierden población? Cuatro censos por celda (capa
-- censo_temporal, columnas que no están en v_hexoma: se baja a la tabla capa_*)
-- agrupados por clase de minutos a la capital provincial.
-- censo_temporal es territorial (no se parte por departamento): basta (h3, territorio).
-- Medido: 0,5 s. Control: la suma por provincia reproduce los censos (Chaco 1991: 839.660,
-- INDEC 839.677; Misiones 2022: 1,27 M).
-- ─────────────────────────────────────────────────────────────────────────────
WITH celdas AS (
    SELECT t.territorio, ct.pob_cnt_1991, ct.pob_cnt_2001, ct.pob_cnt_2010, ct.pob_cnt_2022,
           CASE WHEN a.travel_min_capital_raw < 30  THEN '1 · < 30 min'
                WHEN a.travel_min_capital_raw < 60  THEN '2 · 30–60 min'
                WHEN a.travel_min_capital_raw < 120 THEN '3 · 60–120 min'
                WHEN a.travel_min_capital_raw >= 120 THEN '4 · > 120 min'
                ELSE '5 · sin dato' END AS aislamiento
    FROM capa_censo_temporal ct
    JOIN territorios t USING (territorio_id)
    LEFT JOIN capa_accessibility a ON a.h3 = ct.h3 AND a.territorio_id = ct.territorio_id AND a.principal
)
SELECT territorio, aislamiento,
       round(sum(pob_cnt_1991)) AS pob_1991,
       round(sum(pob_cnt_2001)) AS pob_2001,
       round(sum(pob_cnt_2010)) AS pob_2010,
       round(sum(pob_cnt_2022)) AS pob_2022,
       round((100 * (sum(pob_cnt_2022) / nullif(sum(pob_cnt_1991), 0) - 1))::numeric, 1) AS var_pct_1991_2022
FROM celdas
GROUP BY 1, 2
ORDER BY 1, 2;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Leña o carbón para cocinar donde el PM2.5 es alto (esquema censo)
-- ¿Cuántos hogares cocinan con leña o carbón en celdas con PM2.5 en el cuartil más
-- alto de la región, por departamento? Junta el censo por radio (censo_2022.v_h3: hogares
-- repartidos por el peso de edificios del cruce) con la capa de PM2.5 de v_hexoma.
-- Ámbito región para el PM2.5 (capa satelital comparable entre territorios).
-- Tasas de un conjunto de celdas: sumar _num y _den y dividir al final, nunca promediar.
-- Medido: 14 s. Muestra: Patiño (Formosa) 5.095 hogares con leña en el cuartil alto;
-- Ramón Lista 72 % de hogares con leña. Por provincia: Formosa y Misiones 9,2 %,
-- Chaco 4,9 %, Corrientes 4,8 %.
-- ─────────────────────────────────────────────────────────────────────────────
WITH celdas AS (
    SELECT v.territorio, v.departamento,
           h.hogares, h.cocina_lena_carbon_num AS lena,
           v.pm25_drivers__c_pm25_mean AS pm25,
           r.pm25_drivers__c_pm25_mean__pr_reg AS pm25_pr
    FROM censo_2022.v_h3 h
    JOIN v_hexoma v USING (h3, territorio)
    JOIN v_hexoma_rangos r USING (h3, territorio)
)
SELECT territorio, departamento,
       round(sum(hogares))                                              AS hogares,
       round(sum(lena))                                                 AS hogares_lena,
       round((100 * sum(lena) / nullif(sum(hogares), 0))::numeric, 1)   AS pct_lena,
       round(sum(lena) FILTER (WHERE pm25_pr >= 75))                    AS lena_pm25_cuartil_alto,
       round((sum(pm25 * hogares) / nullif(sum(hogares) FILTER (WHERE pm25 IS NOT NULL), 0))::numeric, 1) AS pm25_medio_hogares
FROM celdas
GROUP BY 1, 2
ORDER BY lena_pm25_cuartil_alto DESC NULLS LAST
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. La campaña 2022/23 frente a su normal, por departamento (esquema clima)
-- ¿Cuánto llovió en la campaña de la sequía 2022/23 frente a la normal 1991–2020 de
-- cada celda, y en qué parte del departamento fue la campaña más seca de las 30 de
-- referencia? Lluvia CHIRPS v3; balance = lluvia − ET0 FAO-56 de ERA5-Land.
-- Cada celda lee el píxel que contiene su centro (CHIRPS ~70 celdas por píxel): el
-- promedio por departamento pondera los píxeles por cuántas celdas del depto cubren.
-- Medido el 1-nov-2026: 6 s. Encabeza Corrientes: Goya −48 % de la normal, balance
-- −873 mm, y en 99 % de sus celdas fue la campaña más seca de 1991–2020.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT l.nombre AS departamento, t.etiqueta AS provincia, count(*) AS celdas,
       round(avg(a.anom_pct)::numeric, 0)                          AS lluvia_pct_normal,
       round(avg(b.anom)::numeric, 0)                               AS balance_anom_mm,
       round(100.0 * avg((a.pct_ref = 0)::int)::numeric, 0)        AS pct_celdas_la_mas_seca
FROM hexagonos x
JOIN territorios t USING (territorio_id)
JOIN limites l ON l.redcode = x.redcode
JOIN clima.hex_pixel h ON h.h3 = x.h3
JOIN clima.anomalia a ON a.tabla = 'chirps_campania' AND a.unidad_id = h.chirps_pixel
                     AND a.variable = 'p_mm' AND a.periodo = 2022
JOIN clima.anomalia b ON b.tabla = 'balance_campania' AND b.unidad_id = h.par_id
                     AND b.variable = 'p_menos_et0' AND b.periodo = 2022
GROUP BY 1, 2
HAVING count(*) >= 100   -- fuera las celdas de borde cuyo centro cae en un depto vecino
ORDER BY lluvia_pct_normal
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. Heladas agrometeorológicas y altitud en Misiones (esquemas clima y suelo)
-- Normal 1991–2020 de días con Tmin ≤ 3 °C por franja de elevación (FABDEM).
-- OJO: ERA5-Land SUBESTIMA estos días (~12 por año menos que las estaciones, ver
-- validacion_clima.md): sirve el gradiente, no el número. Y su píxel de ~11 km no
-- ve las heladas de valle: la elevación de la celda no es la del píxel (clima.pixel.z_m).
-- Medido el 1-nov-2026: 9 s. De 2,2 días (100–200 m) a 4,1 (800 m): un gradiente
-- chato que delata el sesgo; la estación de Posadas sola ya supera esos valores.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT (floor(s.elev_media_m / 100) * 100)::int AS elev_desde_m, count(*) AS celdas,
       round(avg(n.media)::numeric, 1) AS dias_tmin_le3_normal,
       round(avg(p.z_m)::numeric, 0)   AS altitud_media_del_pixel_era5
FROM hexagonos x
JOIN territorios t USING (territorio_id)
JOIN suelo.h3 s ON s.h3 = x.h3
JOIN clima.hex_pixel h ON h.h3 = x.h3
JOIN clima.pixel p ON p.fuente = 'era5' AND p.pixel_id = h.era5_pixel
JOIN clima.normal_1991_2020 n ON n.tabla = 'era5_helada' AND n.unidad_id = h.era5_pixel
                             AND n.variable = 'dias_tmin_le3' AND n.estacion IS NULL
WHERE t.territorio = 'misiones'
GROUP BY 1 ORDER BY 1;
