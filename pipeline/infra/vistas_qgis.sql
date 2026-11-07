-- Tablas derivadas para ver en QGIS lo que en el Hexoma no tiene geometría propia (proyecto_qgis.py).
-- Son tablas comunes y no vistas materializadas: PostgreSQL 17+ arma las vistas materializadas con search_path
-- restringido y las funciones de h3/PostGIS sin esquema fallan. Rearmar después de recargar las tablas de origen:
--   psql -U postgres -d hexoma -f pipeline/infra/vistas_qgis.sql
-- Cada tabla lleva una clave entera `id` (QGIS la necesita) e índice espacial.

DROP TABLE IF EXISTS infra.mv_hand_h3;
CREATE TABLE infra.mv_hand_h3 AS
SELECT row_number() OVER ()::int AS id, h.h3::text AS h3, h.hand_min, h.hand_mediana, h.pct_lt_1m, h.pct_lt_2m,
       h.pct_lt_5m, h.area_cauce_km2, h3_cell_to_boundary_geometry(h.h3) AS geom
FROM infra.hand_h3 h;
CREATE UNIQUE INDEX ON infra.mv_hand_h3 (id);
CREATE INDEX ON infra.mv_hand_h3 USING gist (geom);
COMMENT ON TABLE infra.mv_hand_h3 IS 'infra.hand_h3 con el polígono del hexágono (para QGIS)';

DROP TABLE IF EXISTS infra.mv_acceso_h3;
CREATE TABLE infra.mv_acceso_h3 AS
SELECT row_number() OVER ()::int AS id, s.h3::text AS h3, s.personas,
       s.min_hospital AS min_seco, l.min_hospital AS min_lluvia, f.min_hospital AS min_lluvia_fuerte,
       f.sin_ruta AS sin_ruta_lluvia_fuerte, s.km_tierra_hospital AS km_tierra,
       h3_cell_to_boundary_geometry(s.h3) AS geom
FROM infra.acceso_lluvia s
JOIN infra.acceso_lluvia l ON l.h3 = s.h3 AND l.cota = s.cota AND l.escenario = 'lluvia'
JOIN infra.acceso_lluvia f ON f.h3 = s.h3 AND f.cota = s.cota AND f.escenario = 'lluvia_fuerte'
WHERE s.escenario = 'seco' AND s.cota = 'pesimista';
CREATE UNIQUE INDEX ON infra.mv_acceso_h3 (id);
CREATE INDEX ON infra.mv_acceso_h3 USING gist (geom);
COMMENT ON TABLE infra.mv_acceso_h3 IS 'Minutos al hospital por celda poblada en seco, lluvia y lluvia fuerte (cota pesimista), con el polígono (para QGIS)';

DROP TABLE IF EXISTS infra.mv_ookla_ultimo;
CREATE TABLE infra.mv_ookla_ultimo AS
WITH u AS (SELECT tipo, max(anio * 10 + trimestre) AS at FROM infra.ookla GROUP BY tipo)
SELECT row_number() OVER ()::int AS id, o.tipo, o.anio, o.trimestre, o.quadkey,
       round(o.avg_d_kbps / 1000.0, 1) AS bajada_mbps, round(o.avg_u_kbps / 1000.0, 1) AS subida_mbps,
       o.avg_lat_ms AS latencia_ms, o.tests AS pruebas, o.devices AS dispositivos,
       -- tesela de zoom 16: 40075016,686 / 2^16 = 611,5 m de lado en web-mercator
       ST_Transform(ST_Expand(ST_Transform(o.geom, 3857), 305.75), 4326) AS geom
FROM infra.ookla o JOIN u ON u.tipo = o.tipo AND o.anio * 10 + o.trimestre = u.at;
CREATE UNIQUE INDEX ON infra.mv_ookla_ultimo (id);
CREATE INDEX ON infra.mv_ookla_ultimo USING gist (geom);
COMMENT ON TABLE infra.mv_ookla_ultimo IS 'Último trimestre de Ookla (móvil y fija) como tesela (para QGIS)';

DROP TABLE IF EXISTS infra.mv_luz_pixel;
CREATE TABLE infra.mv_luz_pixel AS
SELECT p.pixel_id AS id, p.mediana_20130101 AS mediana_2013, p.mediana_20250101 AS mediana_2025,
       p.asentamiento_gid, a.nombre AS asentamiento,
       ST_Expand(p.geom, 1 / 480.0) AS geom          -- píxel de 15" (1/240°)
FROM infra.luz_pixel p LEFT JOIN infra.asentamientos a ON a.gid = p.asentamiento_gid
WHERE p.iluminado AND p.en_misiones;
CREATE UNIQUE INDEX ON infra.mv_luz_pixel (id);
CREATE INDEX ON infra.mv_luz_pixel USING gist (geom);
COMMENT ON TABLE infra.mv_luz_pixel IS 'Píxeles VIIRS iluminados de Misiones como cuadrado de 15", medianas anuales 2013 y 2025 (para QGIS)';

DROP TABLE IF EXISTS infra.mv_caminos_misiones;
CREATE TABLE infra.mv_caminos_misiones AS
SELECT id, clase, superficie, nombre, ref, fuente, round(largo_m) AS largo_m, geom
FROM vial.caminos WHERE provincia = 'misiones';
CREATE UNIQUE INDEX ON infra.mv_caminos_misiones (id);
CREATE INDEX ON infra.mv_caminos_misiones USING gist (geom);
COMMENT ON TABLE infra.mv_caminos_misiones IS 'vial.caminos de Misiones (para QGIS)';

-- Tramos de la red derivada que caen sobre ríos anchos, embalses o espejos de agua del IGN: sobre el agua plana el
-- D8 traza manojos de líneas paralelas que no son arroyos. Se marcan (no se borran) y QGIS no los dibuja.
-- (red_arroyos.py rearma infra.arroyos sin esta columna: correr este archivo después.)
ALTER TABLE infra.arroyos ADD COLUMN IF NOT EXISTS en_agua_ancha boolean;
CREATE TEMP TABLE _agua AS
  SELECT geom FROM infra.ign_corrientes_area UNION ALL SELECT geom FROM infra.ign_embalses
  UNION ALL SELECT geom FROM infra.ign_espejos_perennes;
CREATE INDEX ON _agua USING gist (geom);
UPDATE infra.arroyos a SET en_agua_ancha = EXISTS (
  SELECT 1 FROM _agua w WHERE ST_Intersects(w.geom, ST_LineInterpolatePoint(a.geom, 0.5)));
