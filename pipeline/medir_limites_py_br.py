"""Usage: python pipeline/medir_limites_py_br.py itapua_py,parana_br [land_use,flood_risk]
(from neahub/; needs pipeline/output/hexoma/registry.json).
Per PY/BR territory and per-department layer: share of each file's cells whose centre is in
the polygon the web draws for that department (findDeptFeature rule), names without polygon,
and how much the layers' partitions disagree among themselves."""
import json, sys, re, unicodedata, urllib.request, time
import duckdb, h3, numpy as np, pandas as pd, geopandas as gpd, shapely
REG = json.load(open('pipeline/output/hexoma/registry.json', encoding='utf-8'))
AL = {'docedeoctubre':'12deoctubre','nuevedejulio':'9dejulio','primerodemayo':'1demayo','veinticincodemayo':'25demayo'}
def norm(s):
    k = unicodedata.normalize('NFD', (s or '').lower()); k = re.sub(r'[̀-ͯ]', '', k)
    k = re.sub(r'[^a-z0-9]', '', k); return AL.get(k, k)
def polys(tid):
    if tid.endswith('_py'):
        fc = json.load(open(f'static/data/{tid[:-3]}_districts.geojson', encoding='utf-8'))
    else:
        sys.path.insert(0, 'pipeline'); from repartir_deptos_indec import s3_client
        fc = json.loads(s3_client().get_object(Bucket='neahub', Key=f'data/br_districts/{tid}_districts.geojson')['Body'].read())
    g = gpd.GeoDataFrame.from_features(fc['features'], crs=4326)
    g['key'] = g['district'].map(norm) if tid.endswith('_py') else g['district']
    g['geometry'] = shapely.make_valid(g.geometry.values)
    return g[['key', 'geometry']]
con = duckdb.connect(); con.execute('INSTALL httpfs; LOAD httpfs; SET threads=16')
fresh = int(time.time())
ONLY = set(sys.argv[2].split(',')) if len(sys.argv) > 2 else set()
out = []
for tid in sys.argv[1].split(','):
    g = polys(tid); pk = set(g.key)
    part = {}
    for lid, spec in REG['loads'][tid].items():
        if spec['scope'] != 'dept' or (ONLY and lid not in ONLY): continue
        files = spec['files']
        name = {f['url'].split('?')[0]: f['dept'] for f in files}
        urls = [f"{u}?fresh={fresh}" for u in name]
        df = con.execute(f"SELECT h3index, split_part(filename,'?',1) f FROM read_parquet({urls}, filename=true)").df()
        df['dept'] = df.f.map(name)
        df['key'] = df.dept.map(norm) if tid.endswith('_py') else df.dept
        nopoly = sorted(set(df.dept[~df.key.isin(pk)]))
        ll = np.array([h3.cell_to_latlng(c) for c in df.h3index])
        pts = gpd.GeoDataFrame(df[['h3index', 'key']], geometry=gpd.points_from_xy(ll[:, 1], ll[:, 0]), crs=4326)
        j = gpd.sjoin(pts, g.rename(columns={'key': 'pkey'}), how='left', predicate='within')
        j = j[~j.index.duplicated()]
        inside = (j.pkey == j.key).mean(); outside_all = j.pkey.isna().mean()
        part[lid] = df.drop_duplicates('h3index').set_index('h3index')['key']
        dup = int(df.h3index.duplicated().sum())
        out.append((tid, lid, len(files), len(g), len(df), round(100*inside, 2), round(100*outside_all, 2), dup, nopoly[:5], len(nopoly)))
        print(*out[-1], flush=True)
    # cross-layer agreement vs land_use
    if 'land_use' in part:
        for lid, s in part.items():
            if lid == 'land_use': continue
            c = s.index.intersection(part['land_use'].index)
            print('   ', tid, lid, 'vs land_use: same dept', round(100*(s[c] == part['land_use'][c]).mean(), 2), '% of', len(c), flush=True)
