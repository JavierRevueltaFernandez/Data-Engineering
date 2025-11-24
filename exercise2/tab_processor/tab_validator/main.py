# Importamos las bibliotecas necesarias
import os
import click
import re
import logging as log
import datetime
import shutil
from pathlib import Path # Importamos Path para manejo de rutas

# Definimos los directorios como objetos Path
INPUT_DIRECTORY = Path("./files/")
CLEANED_DIRECTORY = INPUT_DIRECTORY / "cleaned"
OUTPUT_DIRECTORY_OK = INPUT_DIRECTORY / "validations" / "ok"
OUTPUT_DIRECTORY_KO = INPUT_DIRECTORY / "validations" / "ko"
ROOT = "https://acordes.lacuerda.net"
URL_ARTIST_INDEX = "https://acordes.lacuerda.net/tabs/"
SONG_VERSION = 0
INDEX = "abcdefghijklmnopqrstuvwxyz#"


# dir_list = list()
# output_file = str()
# dir = str()
# file_name = str()


def validate_song_format(song):
    """Valida si la canción contiene la mezcla esperada de acordes y letras (Calidad de Contenido)"""

    # Busca un acorde simple.
    chord_pattern = r"[A-G][#b]?(m|maj|min|sus|add|aug|dim)?[0-9]?"
    
    # Comprobar si hay al menos un acorde en el archivo.
    has_chords = re.search(chord_pattern, song)

    # Comprobar si hay letras (al menos una palabra de minúsculas).
    has_lyrics = re.search(r"[a-z]+", song) 

    # La validación pasa si hay acordes Y letras.
    if has_chords and has_lyrics:
        return True
    else:
        log.debug("Validation KO: Missing chords or lyrics content.")
        return False


def list_files_recursive(path: str = "."):
    """Lists all files in a directory recursively, returning Path objects."""
    found_files = []
    path = Path(path) 
    
    for entry in os.listdir(path):
        full_path = path / entry
        if full_path.is_dir():
            found_files.extend(list_files_recursive(full_path))
        else:
            # Filtramos aquí también, aunque la entrada debería ser solo .txt
            if full_path.suffix == ".txt":
                found_files.append(full_path)

    # La lista global 'dir_list' original se ha eliminado para usar 'found_files'.
    return found_files


@click.command()
@click.option(
    "--init",
    "-i",
    is_flag=True,
    default=False,
    help=(
        "If flag is present, drops all files and validates from the clean directory. "
    ),
)
def main(init):
    # Start time tracking
    start_time = datetime.datetime.now()
    log.info(f"Validator started at {start_time}")
    print("Starting validator...")

    if init:
        if OUTPUT_DIRECTORY_OK.exists():
            shutil.rmtree(OUTPUT_DIRECTORY_OK)
        if OUTPUT_DIRECTORY_KO.exists():
            shutil.rmtree(OUTPUT_DIRECTORY_KO)
        log.info("Validation directories removed")

    OK = 0
    KO = 0

    # Obtenemos la lista de archivos limpios
    files_to_validate = list_files_recursive(CLEANED_DIRECTORY)

    for file_path in files_to_validate:

        text = str()
        try:
            with open(file_path, "r") as file:
                text = file.read()
        except Exception as e:
            log.error(f"Error reading file {file_path}: {e}")
            continue

        # Formatting of the text goes in that function call
        validated = validate_song_format(text)

        # Obtenemos la ruta relativa: 'cleaned/songs/artist/song.txt' -> 'songs/artist/song.txt'
        relative_path = file_path.relative_to(CLEANED_DIRECTORY)

        # Elegimos el directorio de salida base
        output_base_dir = OUTPUT_DIRECTORY_OK if validated else OUTPUT_DIRECTORY_KO
        
        # Construimos la ruta final: 'validations/ok' / 'songs/artist/song.txt'
        output_path = output_base_dir / relative_path

        # Creamos los directorios padres de forma recursiva (output_path.parent)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if validated:
            OK += 1
        else:
            KO += 1

        try:
            # Escribimos el contenido al nuevo path
            with open(output_path, "w") as file:
                file.write(text)    
                print(f"File {output_path.name} -> {'OK' if validated else 'KO'}. OKs: {OK}, KOs: {KO}")

        except Exception as e:
            log.error(f"Error writing validated file to {output_path}: {e}")
            continue

    log.info(f"OKs = {OK}, -- KOs = {KO}, --")
    end_time = datetime.datetime.now()
    log.info(f"Validator ended at {end_time}")
    duration = end_time - start_time
    log.info(f"Total duration: {duration}")
    print(
        f"Validator finished. Duration in seconds: {duration.total_seconds()}, that is {duration.total_seconds() / 60} minutes."
    )


if __name__ == "__main__":
    main()
