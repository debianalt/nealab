"""
gs://spatia-satellite as a staging area for GEE exports: download, verify, delete.

GEE can only export to GCS, Drive or Assets, so exports land here; keeping them
after the pipeline has its local copy only fills the bucket (Always Free = 5 GB
Standard in us-central1). Decided with the author on 3-nov-2026.

Never deleted: persistent inputs that CI reads without exporting them first.
  static/  radio_stats_master, catastro, overture… (catastro/land-use/overture/pm25 CI)
  eudr/    eudr_deforestation_br.tif (EUDR CI)

SPATIA_KEEP_GCS=1 in the environment keeps everything (e.g. to re-run a step).
"""
import os

KEEP_PREFIXES = ('static/', 'eudr/')


def download_and_delete(blob, local_path: str) -> str:
    """Download a GCS blob, check its size, then delete it unless it is a kept input."""
    os.makedirs(os.path.dirname(local_path) or '.', exist_ok=True)
    blob.download_to_filename(local_path)
    if blob.size is None:
        blob.reload()
    got = os.path.getsize(local_path)
    if blob.size is not None and got != blob.size:
        raise IOError(f'gs://{blob.bucket.name}/{blob.name}: downloaded {got} bytes, expected {blob.size}')
    if os.environ.get('SPATIA_KEEP_GCS') or blob.name.startswith(KEEP_PREFIXES):
        return local_path
    blob.delete()
    print(f'  gcs: deleted gs://{blob.bucket.name}/{blob.name} (local copy {got / 1e6:.1f} MB)', flush=True)
    return local_path


def fetch(gcs_name: str, local_path: str) -> str:
    """Download gs://<bucket>/<gcs_name> to local_path and delete it from the bucket."""
    from config import GCS_BUCKET
    from download_gcs import get_gcs_client
    blob = get_gcs_client().bucket(GCS_BUCKET).blob(gcs_name)
    blob.reload()   # fails loudly if the object does not exist
    return download_and_delete(blob, local_path)


if __name__ == '__main__':
    import sys
    fetch(sys.argv[1], sys.argv[2])
