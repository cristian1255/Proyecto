import logging
import sys
import os

# Agrega el directorio dags al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etls.etl_viales_estrella import extract_viales, load_viales

logger = logging.getLogger("airflow.task")


# =========================
# PIPELINE VIALES
# =========================
def run_viales_pipeline(**kwargs):
    url = kwargs.get("url")
    year = kwargs.get("year")

    logger.info(f"Procesando VIALES: {url}")

    data = extract_viales(url)

    if not data:
        logger.warning("Sin datos viales")
        return "Sin datos viales"

    load_viales(data, year)

    return f"{len(data)} registros viales cargados"
