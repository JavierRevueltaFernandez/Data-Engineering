# Propuesta para Puntos Extra: Implementar Idempotencia y Limpieza Automática
# Anteriormente, el módulo results mostró 243 archivos porque no limpiamos el estado previo. 
# Aunque el orquestador tiene una bandera --init, una solución más elegante es asegurar que todos
# los directorios de salida se limpien automáticamente al inicio de la ejecución del orquestador,
# incluyendo el catálogo antiguo, para garantizar que el pipeline sea totalmente idempotente
# (ejecutarlo varias veces da el mismo resultado).


import logging as log
import shutil
from pathlib import Path

# Directorios principales que deben ser limpiados al inicio del pipeline
DIRECTORIES_TO_CLEAN = [
    Path("./files/songs"),
    Path("./files/cleaned"),
    Path("./files/validations"),
    Path("./files/lyrics_only"),
    Path("./files/insights"),
]

# Archivos de metadatos que deben ser eliminados para forzar el re-scrape
FILES_TO_DELETE = [
    Path("./files/catalog.json"),
]

def cleanup_pipeline_outputs():
    """
    Elimina todos los directorios de salida y archivos de metadatos del pipeline
    para asegurar una ejecución idempotente (partiendo de un estado limpio).
    """
    logger = log.getLogger('CLEANUP')
    logger.info("Starting comprehensive cleanup of pipeline output directories...")
    cleaned_count = 0
    
    # 1. Eliminar directorios
    for directory in DIRECTORIES_TO_CLEAN:
        if directory.exists():
            shutil.rmtree(directory, ignore_errors=True)
            logger.info(f"Directory removed: {directory}")
            cleaned_count += 1

    # 2. Eliminar archivos clave
    for file_path in FILES_TO_DELETE:
        if file_path.exists():
            file_path.unlink()
            logger.info(f"File deleted: {file_path}")
            cleaned_count += 1
            
    if cleaned_count > 0:
        logger.info(f"Cleanup finished. {cleaned_count} items removed.")
    else:
        logger.info("No pipeline outputs found. Starting clean.")
    
    return cleaned_count

if __name__ == "__main__":
    # Script de prueba simple para asegurar que el módulo funciona.
    log.basicConfig(level=log.INFO, format="%(levelname)s: %(message)s")
    cleanup_pipeline_outputs()