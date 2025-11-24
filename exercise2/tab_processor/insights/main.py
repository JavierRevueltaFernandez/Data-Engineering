import logging as log
from pathlib import Path
import datetime
import re
from collections import defaultdict, Counter
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import json
nltk.download('punkt_tab')
nltk.download('averaged_perceptron_tagger_eng')

# ------------------------------------------
# EXPLICACIÓN DEL PROCESAMIENTO DE INSIGHTS 
# ------------------------------------------
# 1. Agrupa todas las letras de cada artista en un único texto.
#
# 2. Separación de palabras (Tokenización): El texto se divide en palabras individuales.
#
# 3. Filtrado inicial: Se eliminan palabras comunes que no aportan significado (Stopwords),
#    como 'la', 'el', 'un', 'y', 'que', etc.
#
# 4. Clasificación gramatical (POS Tagging): Cada palabra restante se clasifica según
#    su función: Sustantivo, Verbo, Adjetivo, etc.
#
# 5. Filtro: Solo se mantienen las palabras clasificadas como Sustantivos, Verbos
#    o Adjetivos, ya que son las que contienen el significado temático o la acción de la canción.
#
# 6. Normalización (Lematización): Las palabras se reducen a su forma raíz. Por ejemplo,
#    "amando" y "amé" se convierten en "amar" para que se cuenten como una sola palabra.
#
# 7. Conteo (Reduce): Se cuentan las ocurrencias de cada palabra clave final.
#
# 8. Resultados: Se generan los top 10 por artista y el top 20 global, guardados en JSON.
#
# Este proceso asegura que el análisis se centre en el vocabulario con significado real.
# =================================================================================


# --- Configuración ---
INPUT_DIRECTORY_LYRICS = Path("./files/") / "lyrics_only"
OUTPUT_DIRECTORY_INSIGHTS = Path("./files/") / "insights"
LOGS_DIRECTORY = Path("./logs/")

# Lista de etiquetas POS que corresponden a Sustantivos (NN), Verbos (VB) y Adjetivos (JJ)
TARGET_POS_TAGS = ['NN', 'NNS', 'NNP', 'NNPS',  # Nouns
                   'VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ',  # Verbs
                   'JJ', 'JJR', 'JJS']  # Adjectives

# --- Logging Setup ---
LOGS_DIRECTORY.mkdir(exist_ok=True)
log.basicConfig(
    filename=LOGS_DIRECTORY / "insights.log",
    filemode="w",
    encoding="utf-8",
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=log.INFO,
)

# --- Funciones de Utilidad NLP ---

def setup_nltk():
    """
    Descarga los recursos necesarios de NLTK y maneja LookupErrors.
    """
    try:
        stopwords.words('spanish')
    except LookupError:
        print("NLTK resources missing. Attempting to download standard models...")
        # Aseguramos todas las dependencias necesarias que han fallado
        nltk.download([
            'punkt', 
            'stopwords', 
            'averaged_perceptron_tagger', 
            'wordnet', 
            'omw-1.4', 
            'taggers/averaged_perceptron_tagger_eng', # El modelo específico que falló
        ], quiet=True)
        print("NLTK setup complete.")
    
    # Después de la descarga, configuramos las stopwords
    spanish_stopwords = set(stopwords.words('spanish'))
    english_stopwords = set(stopwords.words('english'))
    global_stopwords = spanish_stopwords.union(english_stopwords)
    
    return global_stopwords

def process_lyrics(text: str, stopwords_set: set) -> list[str]:
    """
    Tokeniza, filtra por POS Tag y lematiza el texto para generar una lista de palabras clave.
    """
    # 1. Tokenización
    words = nltk.word_tokenize(text.lower())
    
    # 2. Filtrado y Limpieza
    filtered_words = [
        re.sub(r'[^a-záéíóúüñ\s]', '', word) for word in words 
        if word.isalpha() and word not in stopwords_set
    ]
    filtered_words = [word for word in filtered_words if word]

    # 3. POS Tagging
    # CORRECCIÓN: NLTK no soporta 'es' directamente en pos_tag.
    # Usaremos el tagger en inglés, que es el más robusto para Nouns/Verbs/Adjectives.
    # Intentamos el tagger por defecto (inglés)
    tagged_words = nltk.pos_tag(filtered_words) 

    # 4. Lematización y Filtrado Final por POS Tag (Sustantivos, Verbos, Adjetivos)
    lemmatizer = WordNetLemmatizer()
    final_words = []
    
    for word, tag in tagged_words:
        # Lematización basada en el tag (reduce formas plurales, tiempos verbales a su raíz)
        simple_tag = tag[0].lower()
        if simple_tag in ['v', 'a', 'n']:
            # Usar 'spanish' para la lematización no es una opción estándar en NLTK, 
            # pero WordNetLemmatizer usa el contexto de la etiqueta (v, a, n) para lematizar, 
            # que es lo que hacemos aquí.
            lemma = lemmatizer.lemmatize(word, simple_tag)
        else:
            lemma = word
            
        # Filtrar por las etiquetas POS deseadas
        if tag in TARGET_POS_TAGS:
            final_words.append(lemma)
            
    return final_words

# --- Lógica Principal (El resto de la función main se mantiene) ---

def main():
    """
    Ejecuta el módulo de insights: fusiona letras, procesa POS Tagging y genera Top N.
    """
    start_time = datetime.datetime.now()
    log.info(f"Insights module started at {start_time}")
    print("Starting Insights Module (NLP processing)...")

    # [1] Inicializar NLTK y obtener stopwords
    try:
        stopwords_set = setup_nltk()
    except Exception as e:
        log.error(f"FATAL ERROR during NLTK setup. Check dependencies: {e}")
        print("FATAL ERROR: Could not set up NLTK. Please ensure 'nltk' is installed.")
        return

    # [2] Asegurar directorio de salida
    OUTPUT_DIRECTORY_INSIGHTS.mkdir(parents=True, exist_ok=True)

    artist_lyrics = defaultdict(str)
    
    # [3] Obtener archivos del módulo 'lyrics'
    files_to_process = [f for f in INPUT_DIRECTORY_LYRICS.rglob("*.txt") if f.is_file()]
    
    if not files_to_process:
        print("No cleaned lyrics found to process. Run the Lyrics module first.")
        return

    global_word_counter = Counter()

    for file_path in files_to_process:
        
        # El nombre del artista es la carpeta inmediatamente superior a la canción
        artist_name = file_path.parent.name
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lyrics = f.read()
            
            # Acumular letras para la fusión por artista
            artist_lyrics[artist_name] += " " + lyrics
            
        except Exception as e:
            log.error(f"Error reading lyric file {file_path}: {e}")
            continue

    # [4] Procesamiento y Conteo Final
    artist_results = {}
    
    for artist, lyrics in artist_lyrics.items():
        print(f"Processing insights for artist: {artist}")
        
        # Procesar tokenización, POS tag y lematización
        processed_words = process_lyrics(lyrics, stopwords_set)
        
        # Contar palabras para este artista
        artist_counter = Counter(processed_words)
        
        # Top 10 por artista (Requisito)
        top_10 = artist_counter.most_common(10)
        
        artist_results[artist] = top_10
        
        # Acumular al contador global
        global_word_counter.update(artist_counter)

    # [5] Generar Salidas (Merge File, JSON Artista, JSON Global)

    # Salida 1: Fusión de todas las letras por artista (Requisito)
    for artist, lyrics in artist_lyrics.items():
        output_file = OUTPUT_DIRECTORY_INSIGHTS / f"{artist}_merged.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(lyrics.strip())
        log.info(f"Merged lyrics saved for: {artist}")

    # Salida 2: Top 10 por Artista (Guardar JSON)
    insights_json_file = OUTPUT_DIRECTORY_INSIGHTS / "artist_top_10.json"
    with open(insights_json_file, 'w', encoding='utf-8') as f:
        json.dump(artist_results, f, indent=4, ensure_ascii=False)
    log.info(f"Artist insights saved to {insights_json_file.name}")
    
    # Salida 3: Top 20 Global (Requisito)
    top_20_global = global_word_counter.most_common(20)
    global_json_file = OUTPUT_DIRECTORY_INSIGHTS / "global_top_20.json"
    
    with open(global_json_file, 'w', encoding='utf-8') as f:
        json.dump(top_20_global, f, indent=4, ensure_ascii=False)
    log.info(f"Global insights saved to {global_json_file.name}")
    
    print("-" * 40)
    print("--- Insights Generation Complete! ---")
    print(f"Total artists processed: {len(artist_results)}")
    print(f"Top 20 global words saved to: {global_json_file.name}")
    print("-" * 40)


    end_time = datetime.datetime.now()
    log.info(f"Insights module ended at {end_time}")
    duration = end_time - start_time
    log.info(f"Total duration: {duration}")

    return {"artists": len(artist_results), "global_top_20": top_20_global}


if __name__ == "__main__":
    main()