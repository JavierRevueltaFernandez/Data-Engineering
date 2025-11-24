import logging as log
from pathlib import Path
import datetime

# --- Configuration ---
# Utilizamos Path, siguiendo la mejora de código propuesta (Punto 6)
INPUT_DIRECTORY = Path("./files/")
OUTPUT_DIRECTORY_OK = INPUT_DIRECTORY / "validations" / "ok"
OUTPUT_DIRECTORY_KO = INPUT_DIRECTORY / "validations" / "ko"
LOGS_DIRECTORY = Path("./logs/")

# --- Logging Setup ---
LOGS_DIRECTORY.mkdir(exist_ok=True)
log.basicConfig(
    filename=LOGS_DIRECTORY / "results.log",
    filemode="w",
    encoding="utf-8",
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=log.INFO,
)


def count_files_recursively(directory: Path) -> int:
    """
    Cuenta el número de archivos .txt de forma recursiva dentro de un directorio.
    Args:
        directory (Path): La ruta del directorio a inspeccionar.
    """
    if not directory.exists():
        log.warning(f"Directory not found: {directory}")
        return 0

    count = sum(1 for item in directory.rglob("*.txt") if item.is_file())
    return count


def main():
    """Ejecuta el módulo de resultados para contar archivos OK y KO."""
    start_time = datetime.datetime.now()
    log.info(f"Results module started at {start_time}")
    print("Starting Results Module...")

    ok_count = count_files_recursively(OUTPUT_DIRECTORY_OK)
    ko_count = count_files_recursively(OUTPUT_DIRECTORY_KO)
    total_count = ok_count + ko_count

    print("-" * 40)
    # CORRECCIÓN: Eliminamos los emojis por caracteres ASCII seguros
    print(f"OK Total Validated Files: {ok_count}")
    print(f"KO Total Invalid Files: {ko_count}")
    print(f"Total Processed (OK + KO): {total_count}")
    print("-" * 40)

    log.info(f"Results: OK={ok_count}, KO={ko_count}, Total={total_count}")

    end_time = datetime.datetime.now()
    log.info(f"Results module ended at {end_time}")
    duration = end_time - start_time
    log.info(f"Total duration: {duration}")

    print(f"Results module finished in: {duration.total_seconds():.2f} seconds.")

    # Devolvemos los resultados por si el orquestador los necesita
    return {"ok": ok_count, "ko": ko_count, "total": total_count}


if __name__ == "__main__":
    main()