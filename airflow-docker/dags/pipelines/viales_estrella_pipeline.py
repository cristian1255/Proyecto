import logging

from dags.etls.etl_viales_estrella import extract_viales, load_viales

logger = logging.getLogger("airflow.task")


# =========================
# PIPELINE VIALES
# =========================
def run_viales_pipeline(**kwargs):
    url = kwargs.get("url")
    year = kwargs.get("year")

    logger.info(f"Procesando VIALES: {url}")

    # Extraer datos
    data = extract_viales(url)

    if not data:
        logger.warning("Sin datos viales")
        return "Sin datos viales"

    # Cargar datos
    load_viales(data, year)

    logger.info(f"{len(data)} registros viales cargados")

    return f"{len(data)} registros viales cargados"
