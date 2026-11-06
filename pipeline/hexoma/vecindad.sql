-- Hexoma — vecindad con SQL (esquema vec). Lo aplica build_hexoma_meta.py al final de cada recarga.
-- Una celda H3 res-9 tiene 6 vecinas a distancia 1 (anillo de ~350 m entre centros), 18 más a distancia 2, etc.
-- Las funciones toman cualquier tabla o vista con una columna de celda (h3 por defecto; las capa_* usan h3index).
-- Las celdas sin fila o con NULL no cuentan: n_vecinos dice cuántas entraron.

CREATE SCHEMA IF NOT EXISTS vec;
COMMENT ON SCHEMA vec IS 'Hexoma: funciones de vecindad sobre celdas H3 (pipeline/hexoma/vecindad.sql)';

-- Celdas a distancia exacta k (k = 0 devuelve la propia celda).
CREATE OR REPLACE FUNCTION vec.anillo(celda h3index, k int DEFAULT 1)
RETURNS SETOF h3index LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT h3_grid_disk(celda, k)
    EXCEPT
    SELECT h3_grid_disk(celda, k - 1) WHERE k > 0
$$;
COMMENT ON FUNCTION vec.anillo(h3index, int) IS 'Celdas a distancia exacta k de la celda (anillo); k = 0: la celda';

-- Celdas a distancia ≤ k, sin la propia.
CREATE OR REPLACE FUNCTION vec.vecinas(celda h3index, k int DEFAULT 1)
RETURNS SETOF h3index LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT d FROM h3_grid_disk(celda, k) d WHERE d <> celda
$$;
COMMENT ON FUNCTION vec.vecinas(h3index, int) IS 'Celdas a distancia ≤ k, sin la propia (k = 1: las 6 vecinas)';

-- Media de una columna en las vecinas de UNA celda.
CREATE OR REPLACE FUNCTION vec.media_vecinos(objeto regclass, columna text, celda h3index, k int DEFAULT 1,
                                             col_h3 text DEFAULT 'h3')
RETURNS TABLE (media double precision, n_vecinos bigint) LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'SELECT avg(%2$I::double precision), count(%2$I) FROM %1$s WHERE %3$I::h3index IN (SELECT vec.vecinas($1, $2))',
        objeto, columna, col_h3) USING celda, k;
END $$;
COMMENT ON FUNCTION vec.media_vecinos(regclass, text, h3index, int, text) IS
    'Media de la columna en las celdas a distancia ≤ k de una celda (sin ella) y cuántas tenían valor';

-- Valor de cada celda y media de sus vecinas, para un conjunto (filtro = cláusula WHERE sobre el objeto).
CREATE OR REPLACE FUNCTION vec.suavizar(objeto regclass, columna text, k int DEFAULT 1, filtro text DEFAULT 'true',
                                        col_h3 text DEFAULT 'h3')
RETURNS TABLE (h3 h3index, valor double precision, media_vecinos double precision, n_vecinos bigint)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'WITH b AS (SELECT DISTINCT ON (%3$I) %3$I::h3index AS h3, %2$I::double precision AS v FROM %1$s WHERE %4$s),
              t AS (SELECT %3$I::h3index AS h3, %2$I::double precision AS v FROM %1$s)
         SELECT b.h3, b.v, avg(t.v), count(t.v)
         FROM b CROSS JOIN LATERAL vec.vecinas(b.h3, $1) AS d(h) LEFT JOIN t ON t.h3 = d.h
         GROUP BY b.h3, b.v', objeto, columna, col_h3, filtro) USING k;
END $$;
COMMENT ON FUNCTION vec.suavizar(regclass, text, int, text, text) IS
    'Por celda del filtro: su valor, la media de las vecinas a distancia ≤ k y cuántas tenían valor. '
    'Sirve para contraste celda–entorno (valor − media_vecinos) y para suavizar ruido de píxel';

-- Frontera: celdas con valor ≥ umbral que tienen al menos una vecina (≤ k) por debajo del umbral.
CREATE OR REPLACE FUNCTION vec.frontera(objeto regclass, columna text, umbral double precision, k int DEFAULT 1,
                                        filtro text DEFAULT 'true', col_h3 text DEFAULT 'h3')
RETURNS TABLE (h3 h3index, valor double precision, n_bajo bigint, n_vecinos bigint)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'WITH b AS (SELECT DISTINCT ON (%3$I) %3$I::h3index AS h3, %2$I::double precision AS v FROM %1$s
                    WHERE (%4$s) AND %2$I >= $2),
              t AS (SELECT %3$I::h3index AS h3, %2$I::double precision AS v FROM %1$s)
         SELECT b.h3, b.v, count(*) FILTER (WHERE t.v < $2), count(t.v)
         FROM b CROSS JOIN LATERAL vec.vecinas(b.h3, $1) AS d(h) JOIN t ON t.h3 = d.h
         GROUP BY b.h3, b.v HAVING count(*) FILTER (WHERE t.v < $2) > 0',
        objeto, columna, col_h3, filtro) USING k, umbral;
END $$;
COMMENT ON FUNCTION vec.frontera(regclass, text, double precision, int, text, text) IS
    'Celdas con valor ≥ umbral con al menos una vecina (distancia ≤ k) por debajo: borde de un fenómeno. '
    'Las vecinas sin fila no cuentan como «bajo»';
