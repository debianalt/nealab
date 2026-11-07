"""Genera el proyecto QGIS para consultar el Hexoma en esta compu: C:/Users/ant/OneDrive/nealab/hexoma.qgz.

Corre con el Python de QGIS (no con el Python del sistema):
    "C:\\Program Files\\QGIS 3.40.13\\bin\\python-qgis-ltr.bat" pipeline/infra/proyecto_qgis.py

Antes: psql -U postgres -d hexoma -f pipeline/infra/vistas_qgis.sql (tablas mv_* con geometría para QGIS).
Conexión: localhost:5432, base hexoma, usuario postgres, sin contraseña (pg_hba local = trust).
Al final verifica: cada capa válida y con elementos, e imágenes de control (El Soberbio y Posadas).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
from osgeo import gdal, osr
from qgis.core import (QgsApplication, QgsCategorizedSymbolRenderer, QgsColorRampShader, QgsCoordinateReferenceSystem,
                       QgsCoordinateTransform, QgsDataSourceUri, QgsFillSymbol, QgsLineSymbol, QgsMapRendererParallelJob,
                       QgsMapSettings, QgsMarkerSymbol, QgsPalLayerSettings, QgsProject, QgsProperty, QgsRasterLayer,
                       QgsRasterShader, QgsRectangle, QgsReferencedRectangle, QgsRendererCategory, QgsRuleBasedRenderer,
                       QgsSingleBandPseudoColorRenderer, QgsTextFormat, QgsVectorLayer,
                       QgsVectorLayerSimpleLabeling, QgsWkbTypes)
from qgis.PyQt.QtCore import QSize
from qgis.PyQt.QtGui import QColor

SALIDA = Path(r'C:\Users\ant\OneDrive\nealab\hexoma.qgz')
FUENTES = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'infra'
CONTROL = Path(os.environ.get('QGIS_CONTROL', str(FUENTES)))
W = QgsWkbTypes


def raster_luz_2025() -> Path:
    dest = FUENTES / 'luz_mediana_2025.tif'
    z = np.load(FUENTES / 'blackmarble' / 'compuestos' / 'anual_v22_20250101.npz')
    x0, dx, y0, dy, ancho, alto = z['grilla']
    a = np.where(z['median_masked'] < -900, -1, z['median_masked']).astype(np.float32)
    ds = gdal.GetDriverByName('GTiff').Create(str(dest), int(ancho), int(alto), 1, gdal.GDT_Float32,
                                              ['COMPRESS=DEFLATE', 'TILED=YES'])
    ds.SetGeoTransform((float(x0), float(dx), 0, float(y0), 0, float(dy)))
    s = osr.SpatialReference()
    s.ImportFromEPSG(4326)
    ds.SetProjection(s.ExportToWkt())
    b = ds.GetRasterBand(1)
    b.WriteArray(a)
    b.SetNoDataValue(-1)
    ds = None
    return dest


def pg(nombre, schema, tabla, key, wkb, sql='', geom='geom'):
    uri = QgsDataSourceUri()
    uri.setConnection('localhost', '5432', 'hexoma', 'postgres', '')
    uri.setDataSource(schema, tabla, geom, sql, key)
    uri.setWkbType(wkb)
    uri.setSrid('4326')
    return QgsVectorLayer(uri.uri(False), nombre, 'postgres')


def punto(color, tam=2.0, forma='circle', borde='#222222'):
    return QgsMarkerSymbol.createSimple({'name': forma, 'color': color, 'size': str(tam), 'outline_color': borde,
                                         'outline_width': '0.2'})


def linea(color, ancho=0.4):
    return QgsLineSymbol.createSimple({'line_color': color, 'line_width': str(ancho)})


def relleno(color, borde='no'):
    return QgsFillSymbol.createSimple({'color': color, 'outline_style': borde, 'outline_color': '#555555',
                                       'outline_width': '0.1'})


def simple(lyr, sym):
    from qgis.core import QgsSingleSymbolRenderer
    lyr.setRenderer(QgsSingleSymbolRenderer(sym))
    return lyr


def categorias(lyr, campo, cats):
    lyr.setRenderer(QgsCategorizedSymbolRenderer(campo, [QgsRendererCategory(v, s, e) for v, s, e in cats]))
    return lyr


def reglas(lyr, lista):
    """lista de (etiqueta, expresión, símbolo, escala_mín) — escala_mín = denominador más alejado en que se ve (0 = siempre)."""
    raiz = QgsRuleBasedRenderer.Rule(None)
    for etq, expr, sym, esc in lista:
        r = QgsRuleBasedRenderer.Rule(sym, int(esc or 0), 0, expr, etq)
        raiz.appendChild(r)
    lyr.setRenderer(QgsRuleBasedRenderer(raiz))
    return lyr


def etiquetas(lyr, expr, tam=8):
    s = QgsPalLayerSettings()
    s.fieldName = expr
    s.isExpression = True
    f = QgsTextFormat()
    f.setSize(tam)
    s.setFormat(f)
    lyr.setLabeling(QgsVectorLayerSimpleLabeling(s))
    lyr.setLabelsEnabled(True)


def pseudocolor(lyr, pares, clip=True):
    sh = QgsColorRampShader()
    sh.setColorRampType(QgsColorRampShader.Interpolated)
    sh.setColorRampItemList([QgsColorRampShader.ColorRampItem(v, QColor(c), e) for v, c, e in pares])
    sh.setClip(clip)
    rs = QgsRasterShader()
    rs.setRasterShaderFunction(sh)
    lyr.setRenderer(QgsSingleBandPseudoColorRenderer(lyr.dataProvider(), 1, rs))
    return lyr


def clases_minutos(campo):
    return [('Sin ruta al hospital', f'"{campo}" IS NULL', relleno('#5b0f0f'), 0),
            ('< 15 min', f'"{campo}" < 15', relleno('#1a9850'), 0),
            ('15–30 min', f'"{campo}" >= 15 AND "{campo}" < 30', relleno('#fee08b'), 0),
            ('30–60 min', f'"{campo}" >= 30 AND "{campo}" < 60', relleno('#f46d43'), 0),
            ('≥ 60 min', f'"{campo}" >= 60', relleno('#a50026'), 0)]


def construir():
    proj = QgsProject.instance()
    proj.clear()
    proj.setCrs(QgsCoordinateReferenceSystem('EPSG:3857'))
    proj.setTitle('Hexoma — Misiones')
    raiz = proj.layerTreeRoot()

    def grupo(nombre, capas, apagadas=()):
        g = raiz.addGroup(nombre)
        for c in capas:
            proj.addMapLayer(c, False)
            n = g.addLayer(c)
            if c.name() in apagadas:
                n.setItemVisibilityChecked(False)
        return g

    # Prensa (fase 6): menciones de cortes con lugar del nomenclador; varias menciones en el mismo lugar se superponen
    pre = categorias(pg('Menciones en prensa (cortes y crecidas)', 'noticias', 'menciones', 'mencion_id', W.Point,
                        'geom IS NOT NULL'), 'tipo', [
        ('corte_puente', punto('#1f6feb', 2.6, 'square'), 'Corte de puente'),
        ('corte_ruta_camino', punto('#bf8700', 2.4, 'square'), 'Corte de ruta o camino'),
        ('corte_luz', punto('#f1c40f', 2.4, 'star'), 'Corte de luz'),
        ('crecida', punto('#00b4d8', 2.0), 'Crecida'),
        ('evacuados_aislados', punto('#cf222e', 2.0, 'triangle'), 'Evacuados o aislados')])
    grupo('Prensa', [pre], apagadas=(pre.name(),))

    # Ríos y puentes
    rp2 = simple(pg('Puentes RP2 (cota de seguridad)', 'infra', 'visor_puentes_rp2', 'gid', W.Point),
                 punto('#1f6feb', 3.2, 'diamond'))
    etiquetas(rp2, "coalesce(\"nombre\", '') || if(\"cota_seguridad_msnm\" IS NULL, '', ' — ' || "
                   "format_number(\"cota_seguridad_msnm\", 1) || ' msnm')")
    hid = simple(pg('Hidrómetros (INA / Prefectura)', 'infra', 'visor_hidrometros', 'gid', W.Point),
                 punto('#00b4d8', 3.0, 'triangle'))
    etiquetas(hid, '"nombre"', 8)
    cru = categorias(pg('Cruces camino × cauce', 'infra', 'cruces', 'id', W.Point), 'estructura', [
        ('puente', punto('#1f6feb', 1.8), 'Puente'), ('alcantarilla', punto('#2da44e', 1.6), 'Alcantarilla'),
        ('vado', punto('#bf8700', 1.6), 'Vado'), ('sin_dato', punto('#cf222e', 1.2), 'Sin dato de estructura')])
    for cat in cru.renderer().categories():
        cat.symbol().setDataDefinedSize(QgsProperty.fromExpression(
            'coalesce(scale_linear("personas_aisladas_lluvia_fuerte", 0, 2000, 1.2, 7), 1.2)'))
    cru.setScaleBasedVisibility(True)
    cru.setMinimumScale(400000)
    grupo('Ríos y puentes', [rp2, hid, cru])

    # Arroyos
    arr = reglas(pg('Arroyos (red FABDEM, orden de Strahler)', 'infra', 'arroyos', 'arroyo_id', W.LineString,
                    'NOT coalesce(en_agua_ancha, false)'), [
        ('Orden ≥ 6', '"orden" >= 6', linea('#08519c', 1.2), 0),
        ('Orden 4–5', '"orden" BETWEEN 4 AND 5', linea('#3182bd', 0.6), 600000),
        ('Orden 3', '"orden" = 3', linea('#6baed6', 0.4), 200000),
        ('Orden 1–2', '"orden" <= 2', linea('#9ecae1', 0.25), 50000)])
    ignp = simple(pg('Cursos de agua perennes (IGN)', 'infra', 'ign_cursos_perennes', 'gid', W.MultiLineString),
                  linea('#2c7fb8', 0.35))
    igni = simple(pg('Cursos de agua intermitentes (IGN)', 'infra', 'ign_cursos_intermitentes', 'gid', W.MultiLineString),
                  linea('#7fcdbb', 0.35))
    hand = QgsRasterLayer(str(FUENTES / 'hand_misiones.tif'), 'Altura sobre el cauce (HAND, 0–10 m)')
    pseudocolor(hand, [(0, '#08306b', '0 m'), (10, '#2171b5', '1 m'), (20, '#6baed6', '2 m'), (50, '#c6dbef', '5 m'),
                       (100, '#f7fbff', '10 m')])
    hand.renderer().setOpacity(0.7)
    hh3 = reglas(pg('HAND por hexágono (% < 2 m)', 'infra', 'mv_hand_h3', 'id', W.Polygon), [
        ('Nada < 2 m', '"pct_lt_2m" = 0', relleno('#ffffff00'), 0),
        ('< 10 %', '"pct_lt_2m" > 0 AND "pct_lt_2m" < 10', relleno('#c6dbef'), 0),
        ('10–30 %', '"pct_lt_2m" >= 10 AND "pct_lt_2m" < 30', relleno('#6baed6'), 0),
        ('≥ 30 %', '"pct_lt_2m" >= 30', relleno('#08519c'), 0)])
    grupo('Arroyos', [arr, ignp, igni, hand, hh3], apagadas=(hh3.name(), hand.name()))

    # Caminos
    cam = reglas(pg('Caminos de Misiones (superficie)', 'infra', 'mv_caminos_misiones', 'id', W.MultiLineString), [
        ('Pavimento', "\"superficie\" = 'pavimento'", linea('#252525', 0.7), 0),
        ('Ripio / consolidado', "\"superficie\" = 'ripio/consolidado'", linea('#8c6d31', 0.5), 400000),
        ('Tierra (no huella)', "\"superficie\" = 'tierra' AND \"clase\" <> 'huella'", linea('#d95f0e', 0.4), 300000),
        ('Huella de tierra', "\"superficie\" = 'tierra' AND \"clase\" = 'huella'", linea('#fdae6b', 0.3), 100000),
        ('Superficie desconocida', "\"superficie\" = 'desconocida'", linea('#9e9ac8', 0.3), 100000)])
    grupo('Caminos', [cam])

    # Acceso y lluvia
    acc = [reglas(pg(f'Minutos al hospital — {etq}', 'infra', 'mv_acceso_h3', 'id', W.Polygon), clases_minutos(campo))
           for etq, campo in [('seco', 'min_seco'), ('lluvia', 'min_lluvia'), ('lluvia fuerte', 'min_lluvia_fuerte')]]
    grupo('Acceso al hospital (escenarios)', acc, apagadas=(acc[0].name(), acc[1].name(), acc[2].name()))

    # Salud, educación y seguridad
    sal = [simple(pg('Hospitales', 'infra', 'visor_hospitales', 'gid', W.Point), punto('#d7191c', 3.4, 'cross2')),
           simple(pg('CAPS', 'infra', 'visor_caps', 'gid', W.Point), punto('#fdae61', 2.2, 'cross2')),
           simple(pg('Salud (IGN)', 'infra', 'salud_ign', 'gid', W.Point), punto('#f46d43', 1.8)),
           simple(pg('Escuelas', 'infra', 'escuelas', 'gid', W.Point), punto('#7b3294', 1.6, 'square')),
           simple(pg('Policía', 'infra', 'visor_policia', 'gid', W.Point), punto('#2b83ba', 1.8, 'square')),
           simple(pg('Bomberos voluntarios', 'infra', 'visor_bomberos_voluntarios', 'gid', W.Point), punto('#e66101', 2.4, 'star')),
           simple(pg('Bomberos de la Policía', 'infra', 'visor_bomberos_policia', 'gid', W.Point), punto('#b2182b', 2.4, 'star')),
           simple(pg('Ambulancias', 'infra', 'visor_ambulancias', 'gid', W.Point), punto('#e7298a', 2.0, 'triangle'))]
    grupo('Salud, educación y seguridad', sal, apagadas=('Salud (IGN)', 'Escuelas', 'Policía'))

    # Agua potable
    agua = [simple(pg('Perforaciones', 'infra', 'visor_perforaciones', 'gid', W.Point), punto('#0571b0', 1.6)),
            simple(pg('Tanques elevados', 'infra', 'visor_tanques', 'gid', W.Point), punto('#92c5de', 1.8, 'square')),
            simple(pg('Cisternas', 'infra', 'visor_cisternas', 'gid', W.Point), punto('#4393c3', 1.8, 'square')),
            simple(pg('Hidrantes', 'infra', 'visor_hidrantes', 'gid', W.Point), punto('#d6604d', 1.2))]
    grupo('Agua potable', agua, apagadas=tuple(c.name() for c in agua))

    # Electricidad y telecomunicaciones
    lat = simple(pg('Líneas de alta tensión (visor)', 'infra', 'visor_lineas_at', 'gid', W.LineString), linea('#e08214', 0.6))
    lat.renderer().symbol().setDataDefinedWidth(QgsProperty.fromExpression(
        'CASE WHEN "tension_kv" >= 500 THEN 1.4 WHEN "tension_kv" >= 132 THEN 1.0 ELSE 0.5 END'))
    ele = [lat,
           simple(pg('Transformadoras (visor)', 'infra', 'visor_transformadoras', 'gid', W.Point), punto('#b35806', 2.6, 'square')),
           simple(pg('Red eléctrica OSM (líneas)', 'infra', 'osm_electrica', 'gid', W.LineString,
                     "GeometryType(geom) = 'LINESTRING'"), linea('#fdb863', 0.4)),
           simple(pg('Red eléctrica OSM (subestaciones)', 'infra', 'osm_electrica', 'gid', W.Point,
                     "GeometryType(geom) = 'POINT'"), punto('#8c510a', 2.0, 'square')),
           simple(pg('Plantas solares', 'infra', 'visor_parque_solar', 'gid', W.Point), punto('#ffd92f', 3.0, 'star')),
           simple(pg('Antenas (visor)', 'infra', 'visor_antenas', 'gid', W.Point), punto('#542788', 1.8, 'triangle')),
           simple(pg('Antenas (OSM)', 'infra', 'osm_telecom', 'gid', W.Point), punto('#8073ac', 1.4, 'triangle'))]
    ook = []
    for tipo, etq in [('mobile', 'móvil'), ('fixed', 'fija')]:
        o = reglas(pg(f'Internet {etq} — último trimestre (Mbps de bajada)', 'infra', 'mv_ookla_ultimo', 'id', W.Polygon,
                      f"tipo = '{tipo}'"), [
            ('< 10 Mbps', '"bajada_mbps" < 10', relleno('#d7191c'), 0),
            ('10–50', '"bajada_mbps" >= 10 AND "bajada_mbps" < 50', relleno('#fdae61'), 0),
            ('50–100', '"bajada_mbps" >= 50 AND "bajada_mbps" < 100', relleno('#a6d96a'), 0),
            ('≥ 100', '"bajada_mbps" >= 100', relleno('#1a9641'), 0)])
        ook.append(o)
    luz = QgsRasterLayer(str(raster_luz_2025()), 'Luz nocturna 2025 (VIIRS, mediana anual)')
    pseudocolor(luz, [(0.5, '#000004', '0,5'), (2, '#51127c', '2'), (8, '#b73779', '8'), (30, '#fc8961', '30'),
                      (80, '#fcfdbf', '≥ 80 nW·cm⁻²·sr⁻¹')], clip=False)
    lpx = simple(pg('Píxeles iluminados (con asentamiento)', 'infra', 'mv_luz_pixel', 'id', W.Polygon),
                 relleno('#ffff0040', 'solid'))
    grupo('Electricidad y telecomunicaciones', ele + ook + [luz, lpx],
          apagadas=('Red eléctrica OSM (líneas)', 'Red eléctrica OSM (subestaciones)', 'Antenas (OSM)', ook[0].name(),
                    ook[1].name(), luz.name(), lpx.name()))

    # Límites
    lim = [simple(pg('Municipios', 'infra', 'visor_municipios', 'gid', W.Polygon), relleno('#00000000', 'solid')),
           simple(pg('Departamentos (radios INDEC disueltos)', 'public', 'limites', 'redcode', W.MultiPolygon,
                     'territorio_id = 1'), relleno('#00000000', 'dash')),
           simple(pg('Asentamientos BAHRA', 'infra', 'asentamientos', 'gid', W.Point), punto('#000000', 1.4))]
    etiquetas(lim[2], '"nombre"', 7)
    lim[2].setScaleBasedVisibility(True)
    lim[2].setMinimumScale(300000)
    grupo('Límites', lim, apagadas=(lim[1].name(),))

    # Fondo
    osm = QgsRasterLayer('type=xyz&url=https://tile.openstreetmap.org/{z}/{x}/{y}.png&zmax=19&zmin=0',
                         'OpenStreetMap', 'wms')
    sat = QgsRasterLayer('type=xyz&url=https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/'
                         '{z}/{y}/{x}&zmax=19&zmin=0', 'Satélite (Esri)', 'wms')
    grupo('Fondo', [osm, sat], apagadas=(sat.name(),))

    # vista inicial: Misiones
    tr = QgsCoordinateTransform(QgsCoordinateReferenceSystem('EPSG:4326'), proj.crs(), proj)
    ext = tr.transformBoundingBox(QgsRectangle(-56.1, -28.2, -53.6, -25.45))
    proj.viewSettings().setDefaultViewExtent(QgsReferencedRectangle(ext, proj.crs()))
    proj.write(str(SALIDA))
    return proj


def verificar() -> int:
    proj = QgsProject.instance()
    proj.clear()
    proj.read(str(SALIDA))
    malas = 0
    for lyr in proj.mapLayers().values():
        if isinstance(lyr, QgsVectorLayer):
            n = lyr.featureCount()
            ok = lyr.isValid() and n > 0
        else:
            n = '-'
            ok = lyr.isValid()
        malas += not ok
        print(f'  {"ok " if ok else "MAL"} {lyr.name()}: {n}', flush=True)
    capas = [n.layer() for n in proj.layerTreeRoot().findLayers() if n.isVisible() and n.layer().providerType() != 'wms']
    tr = QgsCoordinateTransform(QgsCoordinateReferenceSystem('EPSG:4326'), proj.crs(), proj)
    for nombre, caja in [('el_soberbio', (-54.35, -27.42, -54.05, -27.20)), ('posadas', (-56.00, -27.50, -55.80, -27.33))]:
        ms = QgsMapSettings()
        ms.setDestinationCrs(proj.crs())
        ms.setLayers(capas)
        ms.setExtent(tr.transformBoundingBox(QgsRectangle(*caja)))
        ms.setOutputSize(QSize(1400, 1000))
        ms.setBackgroundColor(QColor('white'))
        job = QgsMapRendererParallelJob(ms)
        job.start()
        job.waitForFinished()
        p = CONTROL / f'qgis_control_{nombre}.png'
        job.renderedImage().save(str(p))
        print(f'  imagen de control: {p}', flush=True)
    print(f'capas con problemas: {malas}', flush=True)
    return 1 if malas else 0


def main() -> int:
    app = QgsApplication([], False)
    app.initQgis()
    try:
        construir()
        print(f'proyecto: {SALIDA}', flush=True)
        return verificar()
    finally:
        app.exitQgis()


if __name__ == '__main__':
    sys.exit(main())
