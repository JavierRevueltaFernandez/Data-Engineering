# Importamos las bibliotecas necesarias
import os
import re
import logging as log
import datetime
# Importamos Path de pathlib para manejar rutas de forma robusta
from pathlib import Path 
# Asumimos que MAPPING está disponible, si no, causará un ImportError
from utils.string_mapping import MAPPING 

# -- Configuration ---
# Usamos Path para definir directorios base
INPUT_DIRECTORY = Path("./files/")
CATALOG_DIRECTORY = INPUT_DIRECTORY / "catalogs" 
LOGS_DIRECTORY = Path("./logs/")
OUTPUT_DIRECTORY = INPUT_DIRECTORY / "cleaned" # Nueva forma, usando pathlib

ROOT = "https://acordes.lacuerda.net"
URL_ARTIST_INDEX = "https://acordes.lacuerda.net/tabs/"
MIN_LINES = 5
SONG_VERSION = 0
INDEX = "abcdefghijklmnopqrstuvwxyz#"

# No usamos una lista global, ya que list_files_recursive se usará solo una vez en main()
# dir_list = list() 

# --- Logging config---
logger = log.getLogger(__name__)

# Aseguramos que el directorio de logs exista antes de configurar el logging
LOGS_DIRECTORY.mkdir(exist_ok=True)

log.basicConfig(
    # Usamos Path para construir la ruta del archivo de log
    filename=LOGS_DIRECTORY / "cleaner.log",
    filemode="w",
    encoding="utf-8",
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=log.INFO,
)

# --- Logic---

# Función list_files_recursive modificada para devolver objetos Path
def list_files_recursive(path: Path = INPUT_DIRECTORY) -> list[Path]:
    """
    Lista recursivamente todos los archivos de un directorio, devolviendo una lista de objetos Path.
    """
    found_files = []
    for entry in os.listdir(path):
        full_path = path / entry # Usamos el operador / de Path para unir la ruta
        if full_path.is_dir():
            # Extendemos la lista con el resultado de la llamada recursiva
            found_files.extend(list_files_recursive(full_path))
        else:
            found_files.append(full_path)
    return found_files


def remove_email_sentences(text: str):

    email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    sentence_pattern = r"[\n^.!?]*" + email_pattern + r"[^.!?]*[.!?\n]"

    return re.sub(sentence_pattern, "", text)


def apply_format_rules(text: str):
    formatted_text = text

    formatted_text = remove_email_sentences(formatted_text)

    for key, value in MAPPING.items():
        # Reemplaza todas las ocurrencias de las reglas definidas en MAPPING
        formatted_text = re.sub(
            key, value, formatted_text, flags=re.DOTALL | re.IGNORECASE
        )
    return formatted_text


def main():

    # Start time tracking
    start_time = datetime.datetime.now()
    log.info(f"Cleaner started at {start_time}")
    print("Starting cleaner...")

    # Aseguramos que el directorio raíz de salida exista
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)
    print(f"INFO {OUTPUT_DIRECTORY} CREATED!!")

    cleaned = 0

    # list_files_recursive ahora devuelve objetos Path. Usamos un bucle para iterar.
    for file_path in list_files_recursive(INPUT_DIRECTORY):
        
        # Tarea 3: Ignoramos los archivos que no sean .txt (archivos de catálogo)
        # Usamos name para obtener el nombre completo del archivo
        if file_path.name == INPUT_DIRECTORY.name or not str(file_path).endswith(".txt"):
            log.info(f"Skipping non-lyric file (catalog/auxiliary): {file_path}")
            continue

        log.info(f"Processing files... -> {file_path}")
        text = str()
        
        try:
            with open(file_path, "r") as file:
                text = file.read()
        except Exception as e:
            log.error(f"Error reading file {file_path}: {e}")
            continue

        if text.count("\n") < MIN_LINES:
            log.info("Empty or too small tab. Skipping.............................")
            continue

        formatted_text = apply_format_rules(text)

        # Tarea 4: SOLUCIÓN ROBUSTA DE RUTAS CON PATHLIB
        
        # 1. Obtenemos la ruta relativa del archivo dentro de INPUT_DIRECTORY.
        #    Ejemplo: 'files/songs/abel_pintos/3.txt' -> 'songs/abel_pintos/3.txt'
        relative_path = file_path.relative_to(INPUT_DIRECTORY)
        
        # 2. Construimos la ruta de salida completa.
        #    Ejemplo: 'files/cleaned' / 'songs/abel_pintos/3.txt'
        output_path = OUTPUT_DIRECTORY / relative_path

        # 3. Creamos los directorios padres (recursivamente) de la ruta de salida.
        #    output_path.parent es el directorio donde debe estar el archivo.
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # Esto asegura que se creen todos los subdirectorios intermedios si no existen.
        
        cleaned += 1
        # 4. Escribimos al path correcto.
        with open(output_path, "w") as file: 
            file.write(formatted_text)
            print(f"{cleaned} -- {output_path.name} CREATED!!")

    end_time = datetime.datetime.now()
    log.info(f"Cleaner ended at {end_time}")
    duration = end_time - start_time
    log.info(f"Total duration: {duration}")
    print(
        f"Cleaner finished. Duration in seconds: {duration.total_seconds()}, that is {duration.total_seconds() / 60} minutes."
    )


if __name__ == "__main__":
    main()