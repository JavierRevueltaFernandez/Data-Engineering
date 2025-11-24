import logging as log
import json
import sys
import utils.beautifulsoup as bs
import utils.files as files
import re
import time


from utils.data import Song, Artist
from pathlib import Path

# --- Configuration ---
ROOT = "https://acordes.lacuerda.net"
URL_ARTIST_INDEX = "https://acordes.lacuerda.net/tabs/"
SONG_VERSION = None
INDEX = "abcdefghijklmnopqrstuvwxyz"


# --- Utility Functions ---
def get_version(song, version: int = 0):
    """Get the song URL and name based on version.
    Args:
        song (str): The base song URL.
        version (int, optional): The version number. Defaults to 0.
    Returns:
        tuple: A tuple containing the modified song URL and the song name.
    """

    song = (
        str(song)
        if not version
        else str(song).replace(".shtml", f"-{str(version)}.shtml")
    )
    song_name = str(song).split("/")[-1].replace(".shtml", ".txt")

    return song, song_name


def get_artists(start_char: str, end_char: str) -> list[Artist]:
    """Scrapes artist URLs for a given range of starting letters.
    Args:
        start_char (str): The starting letter for artists to catalog (e.g., 'a').
        end_char (str): The ending letter for artists to catalog (e.g., 'z').
    Returns:
        list[Artist]: A list of Artist objects.
    """

    log.info("Starting to build artists catalog...")
    artists = []
    for char_code in range(ord(start_char), ord(end_char) + 1):
        char = chr(char_code)
        artist_index_url = f"{URL_ARTIST_INDEX}/{char}"
        log.info(f"Scraping artist index: {artist_index_url}")

        soup = bs.get_soup(artist_index_url)
        if not soup:
            continue

        ul_tag = soup.find("ul")
        if not ul_tag:
            log.info(f"No <ul> found on {artist_index_url}", file=sys.stderr)
            continue

        for li in ul_tag.find_all("li"):
            a_tag = li.find("a")
            if a_tag and a_tag.get("href"):
                href = ROOT + a_tag["href"]
                artist_display_name = Path(href).name.replace("_", " ").title()
                # Creamos el artista y descargamos los metadatos explícitamente aquí, que es cuando hacemos scraping
                new_artist = Artist(name=artist_display_name, url=href)
                new_artist.fetch_metadata()
                artists.append(new_artist)

    return artists


def get_catalog(
    output_directory: Path,
    start_char: str = "a",
    end_char: str = "z",
) -> dict:
    """
    Generates a catalog of artists and their songs from lacuerda.net.
    This function does NOT download lyrics, only metadata.
    Args:
        output_directory (Path): The base directory where lyrics would eventually be saved.
                                 Used to construct potential output_path for each song.
        start_char (str): The starting letter for artists to catalog (e.g., 'a').
        end_char (str): The ending letter for artists to catalog (e.g., 'z').
    Returns:
        dict: A dictionary with artist names as keys and lists of their Song objects as values.
    """
    start_char = start_char.lower()
    end_char = end_char.lower()

    # Get all artists
    catalog = get_artists(start_char, end_char)

    for artist in catalog:
        log.info(f"Scraping songs for artist: {artist.name} ({artist.url})")
        soup = bs.get_soup(artist.url)
        if not soup:
            continue

        for a_tag in soup.select("li > a"):
            # Filter for valid song links. lacuerda.net song links are relative
            # to the artist page and do not typically contain '.shtml' in the <a> href itself
            # for the first part of the relative path, but they *do* eventually form
            # artist/song.shtml. The original code looked for 'id="r"' which is too specific.
            # We'll assume any relative href on an artist page is a potential song link.
            if a_tag and a_tag.get("href") and not a_tag["href"].startswith("http"):

                song_relative_path = a_tag["href"]

                # Construct the full base URL for the song (before adding .shtml or version)
                # Example: https://acordes.lacuerda.net/artist/song_title
                # We need to ensure artist_url ends with a '/' if song_relative_path doesn't start with one,
                # or remove it if song_relative_path starts with one.
                if not artist.url.endswith("/") and not song_relative_path.startswith(
                    "/"
                ):
                    song_base_url_prefix = f"{artist.url}/"
                else:
                    song_base_url_prefix = artist.url

                url = f"{song_base_url_prefix}{song_relative_path}.shtml"
                full_song_url, song_filename = get_version(url, SONG_VERSION)
                song_title = (
                    Path(song_relative_path).stem.replace("_", " ").title()
                )  # The song title can be derived from the 'stem' of the relative path
                song_output_dir = f"{output_directory}songs/{artist.name.replace(' ', '_').lower()}/{song_filename}"

                artist.songs.append(
                    Song(
                        song_title=song_title,
                        song_url=full_song_url,
                        genre="",  # Cannot be scraped directly from lacuerda.net
                        lyrics_path=song_output_dir,
                    )
                )

    log.info("Cataloging complete.")
    return catalog  


def get_song_lyrics(song_name: str, song_url: str, song_file_path: str) -> str:
    """Fetches the lyrics of a song from its URL.
    Args:
        song_url (str): The URL of the song page.
    Returns:
        str: The lyrics text, or an empty string if not found.
    """
    try:

        song_file_path = files.normalize_relative_path(song_file_path)

        if files.check_file_exists(song_file_path):
            log.info(f"File {song_file_path} already exists. Skipping download.")
            return False

        log.info("song --> %s - url --> %s", song_name, song_url)

        try:
            lyric = bs.get_soup(song_url).findAll("pre")
        except Exception as e:
            log.error(f"Error fetching song from {song_url}: {e}")
            return False

        for p in lyric:

            text = re.sub("<.*?>", "", str(p)).strip()
            if text:

                files.write_string_to_file(song_file_path, text=text)
                print(song_name, "downloaded!")
                return True

    except Exception as e:
        log.error(f"Error fetching lyrics from {song_url}: {e}")
        raise e


def get_songs(output_directory: str, version: int = 0):
    """Downloads song lyrics from lacuerda.net based on the provided version.
    Args:
        output_directory (str): The base directory where lyrics will be saved.
        version (int, optional): The version number of the song to download. Defaults to 0.
    """
    # TODO: Refactor this code to use get_catalog and Song/Artist dataclasses.
    # This function currently duplicates a lot of the logic in get_catalog.
    # It should ideally take a catalog of Song objects and download lyrics for each.
    # This would separate concerns and make the code cleaner.
    #
    # Steps:
    # 1. Get the list of artists from the Catalog. So, the INDEX variable is not needed anymore.
    # 2. For each artist, get their songs
    # 3. For each song, check if the lyrics file already exists
    # 4. If not, fetch the lyrics and save to the appropriate path

    # -------------------- OLD CODE --------------------#
    # output_directory = output_directory + "songs/"
    # for i in INDEX:

    #     artist_index_url = URL_ARTIST_INDEX + "/" + i

    #     lis = bs.get_soup(artist_index_url).find("ul").findAll("li")

    #     artists_urls = []
    #     for li in lis:
    #         href = ROOT + li.find("a")["href"]
    #         if href is not None:
    #             artists_urls.append(href)

    #     for url in artists_urls:
    #         artist = str(url).replace(ROOT, "").replace("/", "")
    #         artist_dir = output_directory + f"/{i}/{artist}"
    #         log.info("artist--> %s - url --> %s", artist, url)

    #         lis = bs.get_soup(url).findAll("li")

    #         song_urls = []

    #         for li in lis:
    #             if 'id="r' in str(li):
    #                 href = url + li.find("a")["href"] + ".shtml"
    #                 if href is not None:
    #                     song_urls.append(href)

    #         for song_url in song_urls:

    #             song_url, song_name = get_version(song_url, version)
    #             song_file_path = f"{artist_dir}/{song_name}"

    #             try:
    #                 if get_song_lyrics(song_name, song_url, song_file_path):
    #                     time.sleep(0.5)  # Be polite and avoid hammering the server
    #                 else:
    #                     log.info(
    #                         f"Skipping download for existing file: {song_file_path}"
    #                     )
    #                     continue
    #             except Exception as e:
    #                 log.error(f"Error fetching song from {song_url}: {e}")
    #                 continue
    # -------------------- OLD CODE --------------------#


    # -------------------- NEW CODE --------------------#
    # 1. Obtener la lista de artistas desde el catálogo
    catalog_path = Path(output_directory) / "catalog.json"
    log.info("Attempting to load catalog from: %s", catalog_path)

    raw_catalog = files.load_from_json(catalog_path)
    if raw_catalog is None:
        log.error("Catalog not found or invalid at %s. Aborting get_songs.", catalog_path)
        return

    # Convertir los diccionarios del JSON en objetos Artist
    try:
        catalog: list[Artist] = [Artist.from_dict(artist_data) for artist_data in raw_catalog]
    except Exception as e:
        log.error("Critical error deserializing catalog: %s", e)
        return


    # 2. Para cada artista, obtener sus canciones
    for artist in catalog:
        log.info("Processing artist: %s", artist.name)
        # Para cada canción, verificar si existe y, si no, descargar las letras
        for song in artist.songs:
            song_url = song.song_url
            song_name = song.song_title
            song_file_path = song.lyrics_path 

            try:
                downloaded = get_song_lyrics(
                    song_name=song_name,
                    song_url=song_url,
                    song_file_path=song_file_path,
                )

                if downloaded:
                    # Evitar saturar el servidor
                    time.sleep(0.5)
                else:
                    log.debug("Skipping download for existing file: %s", song_file_path)
                    
            except Exception as e:
                log.error(
                    "Error fetching song '%s' from %s: %s",
                    song_name,
                    song_url,
                    e,
                )
                continue
    # -------------------- NEW CODE --------------------#
