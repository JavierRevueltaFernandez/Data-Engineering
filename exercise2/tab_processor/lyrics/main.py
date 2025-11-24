import logging as log
from pathlib import Path
import datetime
import re

# --- Configuration ---
INPUT_DIRECTORY_OK = Path("./files/") / "validations" / "ok"
OUTPUT_DIRECTORY_LYRICS = Path("./files/") / "lyrics_only"
LOGS_DIRECTORY = Path("./logs/")

# --- Logging Setup ---
LOGS_DIRECTORY.mkdir(exist_ok=True)
log.basicConfig(
    filename=LOGS_DIRECTORY / "lyrics.log",
    filemode="w",
    encoding="utf-8",
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=log.INFO,
)


def remove_chords(text: str) -> str:
    """
    Elimina los acordes de un texto, dejando solo la letra.
    
    Estrategia:
    1. Eliminar líneas que solo contienen patrones de acorde y espacios.
    2. Limpiar saltos de línea extra resultantes.
    """
    
    # Patrón de Acorde Robusto: [Letra A-G][# o b]? (modificador m/maj/sus/etc)? [número]? [barra y bajo]?
    chord_pattern = r"[A-G][#b]?(m|maj|min|sus|add|aug|dim)?[0-9]*(\/[A-G][#b]?)?"
    
    # Patrón de líneas que contienen solo acordes
    lines_only_chords_pattern = re.compile(
        rf"^\s*(?:{chord_pattern}\s*)+$", 
        re.MULTILINE
    )
    lyrics_only = lines_only_chords_pattern.sub("", text)
    
    # Limpiar líneas extra en blanco dejadas por la eliminación, dejando solo un salto de línea
    lyrics_only = re.sub(r"\n\s*\n", "\n", lyrics_only).strip()
    
    return lyrics_only


def list_files_recursive(path: Path) -> list[Path]:
    """Lista recursivamente todos los archivos .txt de un directorio."""
    if not path.exists():
        return []
    # rgload("*.txt") encuentra archivos .txt en cualquier subdirectorio
    return [item for item in path.rglob("*.txt") if item.is_file()]


def main():
    """Procesa los archivos OK, elimina los acordes y los guarda en un nuevo directorio."""
    start_time = datetime.datetime.now()
    log.info(f"Lyrics module started at {start_time}")
    print("Starting Lyrics-Only Module...")

    # Aseguramos que el directorio de salida exista
    OUTPUT_DIRECTORY_LYRICS.mkdir(parents=True, exist_ok=True)
    log.info(f"Output directory created: {OUTPUT_DIRECTORY_LYRICS}")

    processed_count = 0
    files_to_process = list_files_recursive(INPUT_DIRECTORY_OK)

    if not files_to_process:
        print("No OK files found to process. Run the Validator first.")
        return

    for file_path in files_to_process:
        try:
            # Leer el archivo (ya validado)
            with open(file_path, "r", encoding="utf-8") as file:
                text_with_chords = file.read()

            # Eliminar los acordes
            lyrics_only = remove_chords(text_with_chords)

            # Construir la ruta de salida
            relative_path_to_ok = file_path.relative_to(INPUT_DIRECTORY_OK)
            
            # Construimos la ruta de salida en 'files/lyrics_only'
            output_path = OUTPUT_DIRECTORY_LYRICS / relative_path_to_ok
            
            # Aseguramos la creación de subdirectorios (ej: files/lyrics_only/songs/abel_pintos)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Escribir el nuevo archivo
            with open(output_path, "w", encoding="utf-8") as file:
                file.write(lyrics_only)

            processed_count += 1
            print(f"✅ Extracted lyrics from: {file_path.name} -> Saved to {output_path.name}")

        except Exception as e:
            log.error(f"Error processing file {file_path}: {e}")
            continue

    print("-" * 40)
    print(f"Total files processed (Lyrics only): {processed_count}")
    print("-" * 40)

    end_time = datetime.datetime.now()
    log.info(f"Lyrics module ended at {end_time}. Processed: {processed_count}")
    duration = end_time - start_time
    log.info(f"Total duration: {duration}")

    print(f"Lyrics module finished in: {duration.total_seconds():.2f} seconds.")
    return processed_count


if __name__ == "__main__":
    main()