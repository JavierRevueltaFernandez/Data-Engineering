import musicbrainzngs
import utils.files as files
from dataclasses import dataclass, asdict, field
from pathlib import Path

# --- Config ---

# Initialize MusicBrainz client
musicbrainzngs.set_useragent("MyMusicApp", "1.0", "myemail@example.com")


# --- Data Structures ---
@dataclass
class Song:
    """Represents a song with its metadata.

    Attributes:
        id (int): Auto-generated unique identifier for the song.
        song_title (str): The title of the song.
        song_url (str): The URL to the song's page on lacuerda.net.
        genre (str): The genre of the song (if available).
        lyrics_path (Path): The local file path where the song's lyrics are stored.
    """

    id: int = field(init=False)  # Auto-generated ID
    song_title: str
    song_url: str
    genre: str = ""  # Placeholder, as lacuerda.net doesn't provide genre directly
    lyrics_path: Path = None  # Path where the lyric file would be stored

    # Class variable to track next available ID
    _id_counter = 1

    def __post_init__(self):
        """Automatically assign an incremental ID after initialization."""
        self.id = Song._id_counter
        Song._id_counter += 1

        # CORRECCIÓN: Aseguramos que el path se convierta a string antes de normalizarlo
        if self.lyrics_path is not None:
             self.lyrics_path = files.normalize_relative_path(str(self.lyrics_path))
        else:
             self.lyrics_path = files.normalize_relative_path(self.lyrics_path)

    def to_dict(self):
        return asdict(self)

    @staticmethod
    def from_dict(data):
        # Remove id from data if present (it will be auto-generated)
        data_copy = data.copy()
        data_copy.pop("id", None)

        # Ensure lyrics_path is a Path object if it exists
        if "lyrics_path" in data_copy and data_copy["lyrics_path"]:
            data_copy["lyrics_path"] = Path(data_copy["lyrics_path"])

        song = Song(**data_copy)

        # If the original data had an ID and it's higher than our counter,
        # update the counter to avoid conflicts
        if "id" in data and data["id"] >= Song._id_counter:
            Song._id_counter = data["id"] + 1

        return song

    @classmethod
    def reset_id_counter(cls, start_value=1):
        """Reset the ID counter (useful for testing or reinitialization)."""
        cls._id_counter = start_value


@dataclass
class Artist:
    """Represents an artist with their name, URL, and a list of their songs.

    Attributes:
        id (int): Auto-generated unique identifier for the artist.
        name (str): The artist's name.
        url (str): The URL to the artist's page on lacuerda.net.
        genres (list[str]): List of genres/tags associated with the artist.
        albums (list[str]): List of album titles by the artist.
        songs (list[Song]): List of Song objects associated with the artist.
    """

    id: int = field(init=False)  # Auto-generated ID
    name: str
    url: str
    genres: list[str] = field(default_factory=list)  # List of genres/tags
    albums: list[str] = field(default_factory=list)  # List of album titles
    songs: list[Song] = field(
        default_factory=list
    )  # Use default_factory for mutable defaults

    # Class variable to track next available ID
    _id_counter = 1

    def __post_init__(self):
        """Automatically assign an incremental ID after initialization.
        Also fetches metadata from MusicBrainz.
        """
        self.id = Artist._id_counter
        Artist._id_counter += 1

        # Fetch metadata automatically from MusicBrainz
        # self.fetch_metadata()

    def to_dict(self):
        """Converts the Artist object to a dictionary, including its nested songs."""
        data = asdict(self)
        data["songs"] = [song.to_dict() for song in self.songs]
        return data

    def to_dict_no_songs(self):
        """Converts the Artist object to a dictionary, excluding its nested songs."""
        data = asdict(self)
        data.pop("songs", None)
        return data

    def fetch_metadata(self):
        """Fetch artist metadata like tags (genres), albums, and description."""
        try:
            results = musicbrainzngs.search_artists(artist=self.name, limit=1)
            if results["artist-list"]:
                artist_data = results["artist-list"][0]
                mbid = artist_data["id"]  # MusicBrainz ID

                # Get detailed info: tags (genres), releases (albums)
                details = musicbrainzngs.get_artist_by_id(
                    mbid, includes=["tags", "releases"]
                )

                # Genres/tags
                if "tag-list" in details["artist"]:
                    self.genres = [tag["name"] for tag in details["artist"]["tag-list"]]

                # Albums/releases
                if "release-list" in details:
                    self.albums = list({r["title"] for r in details["release-list"]})

        except Exception as e:
            print(f"Error fetching data for {self.name}: {e}")

    @staticmethod
    def from_dict(data):
        """Creates an Artist object from a dictionary, reconstructing nested songs."""
        data_copy = data.copy()
        data_copy.pop("id", None)  # Remove id (will be auto-generated)

        songs_data = data_copy.pop("songs", [])
        artist = Artist(**data_copy)
        artist.songs = [Song.from_dict(s_data) for s_data in songs_data]

        # If the original data had an ID and it's higher than our counter,
        # update the counter to avoid conflicts
        if "id" in data and data["id"] >= Artist._id_counter:
            Artist._id_counter = data["id"] + 1

        return artist

    @classmethod
    def reset_id_counter(cls, start_value=1):
        """Reset the ID counter (useful for testing or reinitialization)."""
        cls._id_counter = start_value
