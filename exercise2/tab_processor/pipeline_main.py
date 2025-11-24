import logging as log
from pathlib import Path
import datetime
import subprocess
import sys

# --- Configuration ---
LOGS_DIRECTORY = Path("./logs/")
PIPELINE_LOG_FILE = LOGS_DIRECTORY / "pipeline.log"

# Lista de módulos a ejecutar en orden.
MODULE_SEQUENCE = [
    
    # 1. Scrapping y catalogación
    {"name": "SCRAPPER", "command": ["python", "scrapper/main.py", "-sc", "a", "-ec", "a"], "required": True},
    # 2. Limpieza de archivos
    {"name": "CLEANER", "command": ["python", "tab_cleaner/main.py"], "required": True},
    # 3. Validación y clasificación final (OK/KO)
    {"name": "VALIDATOR", "command": ["python", "tab_validator/main.py"], "required": True},
    # 4. Generación de conteo de resultados
    {"name": "RESULTS", "command": ["python", "results/main.py"], "required": True},
    # 5. Extracción de letras limpias (preparación para Insights)
    {"name": "LYRICS EXTRACTOR", "command": ["python", "lyrics/main.py"], "required": True},
    # 6. Análisis de insights (NLP)
    {"name": "INSIGHTS ANALYZER", "command": ["python", "insights/main.py"], "required": True},
]

# --- Logging Setup ---
def setup_pipeline_logging():
    """Configura el logger específico para el orquestador."""
    LOGS_DIRECTORY.mkdir(exist_ok=True)
    
    log.basicConfig(
        filename=PIPELINE_LOG_FILE,
        filemode="w",
        encoding="utf-8",
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=log.INFO,
    )
    console = log.StreamHandler()
    console.setLevel(log.INFO)
    formatter = log.Formatter("%(levelname)-8s %(message)s")
    console.setFormatter(formatter)
    log.getLogger('').addHandler(console)
    log.getLogger('PIPELINE').setLevel(log.INFO)


def run_pipeline():
    """Ejecuta todos los módulos secuencialmente y maneja los fallos."""
    logger = log.getLogger('PIPELINE')
    start_time = datetime.datetime.now()
    
    logger.info("-" * 50)
    logger.info("STARTING DATA ENGINEERING PIPELINE")
    logger.info(f"Start Time: {start_time}")
    logger.info("-" * 50)

    # Nota: Eliminamos el bloque de Cleanup para evitar borrar archivos
    
    for step in MODULE_SEQUENCE:
        step_name = step["name"]
        command = step["command"]
        required = step["required"]
        
        logger.info(f"🚀 Running Step: {step_name} | Command: {' '.join(command)}")

        try:
            # Ejecutar el comando del sistema
            result = subprocess.run(
                command, 
                capture_output=True, 
                text=True, 
                check=required 
            )

            logger.info(f"✅ Step Succeeded: {step_name}")
            if result.stdout:
                 # Muestra una línea del output para confirmar que el módulo no se saltó
                 logger.debug(f"Module output preview: {result.stdout.splitlines()[-1]}")
            
        except subprocess.CalledProcessError as e:
            # Captura y registra cualquier error que rompa el pipeline
            logger.error(f"❌ Step Failed: {step_name}")
            logger.error(f"Command: {e.cmd}")
            logger.error(f"Return Code: {e.returncode}")
            logger.error(f"Stderr: {e.stderr}")
            
            if required:
                logger.critical("Pipeline halted due to critical failure.")
                sys.exit(1)
            else:
                logger.warning("Step was not mandatory, continuing pipeline...")
        except FileNotFoundError:
            logger.error(f"❌ ERROR: Command not found. Ensure Python and all module paths are correct.")
            sys.exit(1)
        except Exception as e:
            logger.critical(f"❌ UNEXPECTED CRITICAL ERROR during step {step_name}: {e}")
            sys.exit(1)

    # Fin del Pipeline
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    
    logger.info("-" * 50)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info(f"Total Duration: {duration}")
    logger.info("-" * 50)


if __name__ == "__main__":
    setup_pipeline_logging()
    run_pipeline()