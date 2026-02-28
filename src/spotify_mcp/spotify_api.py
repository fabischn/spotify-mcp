import logging
import os
import concurrent.futures
from typing import Optional, Dict, List

import spotipy
from dotenv import load_dotenv
from spotipy.cache_handler import CacheFileHandler
from spotipy.oauth2 import SpotifyOAuth

from . import utils

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")

if REDIRECT_URI:
    REDIRECT_URI = utils.normalize_redirect_uri(REDIRECT_URI)


class Client:
    """Spotify API client updated for Feb 2026 API changes.

    Handles:
    - Search limit max 10 (Spotify Dev Mode restriction)
    - Playlist endpoint /tracks -> /items migration
    - Response field compatibility (tracks/items, track/item)
    - Individual artist fetching (batch endpoints removed)
    """

    DEV_LIMIT = 10  # Spotify API Feb 2026: max search limit reduced to 10

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        scope = ",".join([
            "user-library-read",
            "user-read-playback-state",
            "user-modify-playback-state",
            "user-read-currently-playing",
            "playlist-read-private",
            "playlist-read-collaborative",
            "playlist-modify-private",
            "playlist-modify-public",
        ])
        try:
            self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
                scope=scope,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
                redirect_uri=REDIRECT_URI))
            self.auth_manager: SpotifyOAuth = self.sp.auth_manager
            self.cache_handler: CacheFileHandler = self.auth_manager.cache_handler
        except Exception as e:
            self.logger.error(f"Failed to initialize Spotify client: {str(e)}")
            raise
        self.username = None

    def _safe_limit(self, limit: int) -> int:
        return min(limit, self.DEV_LIMIT)

    # ── Auth ──────────────────────────────────────────────────────────
    def set_username(self):
        if not self.auth_ok():
            self.auth_refresh()
        self.username = self.sp.current_user()['display_name']

    def auth_ok(self) -> bool:
        try:
            token = self.cache_handler.get_cached_token()
            if token is None:
                return False
            return not self.auth_manager.is_token_expired(token)
        except Exception as e:
            self.logger.error(f"Error checking auth status: {str(e)}")
            return False

    def auth_refresh(self):
        self.auth_manager.validate_token(self.cache_handler.get_cached_token())

    # ── Search ───────────────────────────────────────────────────────

    def search(self, query: str, qtype: str = 'track', limit=5):
        if not self.auth_ok():
            self.auth_refresh()
        if self.username is None:
            self.set_username()
            
        results = self.sp.search(q=query, limit=self._safe_limit(limit), type=qtype)
        if not results:
            raise ValueError("No search results found.")
        return utils.parse_search_results(results, qtype, self.username)

    # ── Get Info ─────────────────────────────────────────────────────

    def get_info(self, item_uri: str) -> dict:
        if not self.auth_ok():
            self.auth_refresh()
            
        _, qtype, item_id = item_uri.split(":")
        match qtype:
            case 'track':
                return utils.parse_track(self.sp.track(item_id), detailed=True)
            case 'album':
                return utils.parse_album(self.sp.album(item_id), detailed=True)
            case 'artist':
                artist_info = utils.parse_artist(self.sp.artist(item_id), detailed=True)
                try:
                    albums = self.sp._get(f"artists/{item_id}/albums", limit=self.DEV_LIMIT)
                    if albums and albums.get('items'):
                        artist_info['albums'] = [utils.parse_album(a) for a in albums['items']]
                except Exception as e:
                    self.logger.error(f"Error fetching artist albums: {str(e)}")
                return artist_info
            case 'playlist':
                if self.username is None:
                    self.set_username()
                playlist = self.sp._get(f"playlists/{item_id}")
                return utils.parse_playlist(playlist, self.username, detailed=True)
        raise ValueError(f"Unknown qtype {qtype}")

    # ── Playback ──────────────────────────────────────────────────────

    def get_current_track(self) -> Optional[Dict]:
        if not self.auth_ok():
            self.auth_refresh()
        try:
            current = self.sp.current_user_playing_track()
            if not current:
                return None
            if current.get('currently_playing_type') != 'track':
                return None
            track_info = utils.parse_track(current['item'])
            if 'is_playing' in current:
                track_info['is_playing'] = current['is_playing']
            return track_info
        except Exception as e:
            self.logger.error("Error getting current track info.")
            raise

    def is_track_playing(self) -> bool:
        curr_track = self.get_current_track()
        return bool(curr_track and curr_track.get('is_playing'))

    @utils.validate
    def start_playback(self, spotify_uri=None, device=None):
        try:
            if not spotify_uri:
                if self.is_track_playing():
                    return
                if not self.get_current_track():
                    raise ValueError("No track_id provided and no current playback to resume.")
            uris = None
            context_uri = None
            if spotify_uri is not None:
                if spotify_uri.startswith('spotify:track:'):
                    uris = [spotify_uri]
                else:
                    context_uri = spotify_uri
            device_id = device.get('id') if device else None
            return self.sp.start_playback(uris=uris, context_uri=context_uri, device_id=device_id)
        except Exception as e:
            self.logger.error(f"Error starting playback: {str(e)}.")
            raise

    @utils.validate
    def pause_playback(self, device=None):
        playback = self.sp.current_playback()
        if playback and playback.get('is_playing'):
            self.sp.pause_playback(device.get('id') if device else None)

    @utils.validate
    def add_to_queue(self, track_id: str, device=None):
        self.sp.add_to_queue(track_id, device.get('id') if device else None)

    @utils.validate
    def get_queue(self, device=None):
        queue_info = self.sp.queue()
        queue_info['currently_playing'] = self.get_current_track()
        queue_info['queue'] = [utils.parse_track(track) for track in queue_info.pop('queue')]
        return queue_info

    def skip_track(self, n=1):
        if not self.auth_ok(): self.auth_refresh()
        for _ in range(n):
            self.sp.next_track()

    def previous_track(self):
        if not self.auth_ok(): self.auth_refresh()
        self.sp.previous_track()

    def seek_to_position(self, position_ms):
        if not self.auth_ok(): self.auth_refresh()
        self.sp.seek_track(position_ms=position_ms)

    def set_volume(self, volume_percent):
        if not self.auth_ok(): self.auth_refresh()
        self.sp.volume(volume_percent)

    # ── Library / Liked Songs ────────────────────────────────────────

    def get_liked_songs(self, limit: int = 0) -> List[Dict]:
        """Fetch user's liked/saved songs with pagination.

        Args:
            limit: Max songs to return. 0 means all songs.
        """
        if not self.auth_ok(): self.auth_refresh()
        all_tracks = []
        offset = 0
        batch_size = 50
        while True:
            results = self.sp.current_user_saved_tracks(limit=batch_size, offset=offset)
            if not results or not results.get('items'):
                break
            for item in results['items']:
                track = item.get('track')
                if not track:
                    continue
                track_info = utils.parse_track(track)
                track_info['added_at'] = item.get('added_at')
                artist_ids = [a['id'] for a in track.get('artists', []) if a.get('id')]
                track_info['artist_ids'] = artist_ids
                all_tracks.append(track_info)
                if 0 < limit <= len(all_tracks):
                    return all_tracks[:limit]
            offset += batch_size
            if not results.get('next'):
                break
        return all_tracks

    # ── Artist Info ──────────────────────────────────────────────────

    # Parallel artist fetching with ThreadPoolExecutor (batch endpoint removed in Feb 2026)
    def get_artists_genres(self, artist_ids: List[str]) -> Dict[str, List[str]]:
        if not self.auth_ok(): self.auth_refresh()
        genres_map = {}
        
        def fetch_artist(aid):
            try:
                artist = self.sp._get(f"artists/{aid}")
                return aid, artist.get('genres', []) if artist else []
            except Exception as e:
                self.logger.error(f"Error fetching artist {aid}: {str(e)}")
                return aid, []

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(fetch_artist, artist_ids)
            for aid, genres in results:
                genres_map[aid] = genres
                
        return genres_map

    # ── Playlist ─────────────────────────────────────────────────────

    @utils.ensure_username
    def get_current_user_playlists(self, limit=50) -> List[Dict]:
        if not self.auth_ok(): self.auth_refresh()
        playlists = self.sp.current_user_playlists()
        if not playlists:
            raise ValueError("No playlists found.")
        return [utils.parse_playlist(p, self.username) for p in playlists['items']]

    @utils.ensure_username
    def get_playlist_tracks(self, playlist_id: str, limit=50) -> List[Dict]:
        if not self.auth_ok(): self.auth_refresh()
        results = self.sp._get(f"playlists/{playlist_id}/items", limit=limit)
        if not results:
            return []
        tracks = []
        for item in results.get('items', []):
            if not item:
                continue
            track_data = item.get('item') or item.get('track')
            if track_data:
                tracks.append(utils.parse_track(track_data))
        return tracks

    @utils.ensure_username
    def add_tracks_to_playlist(self, playlist_id: str, track_ids: List[str], position: Optional[int] = None):
        if not self.auth_ok(): self.auth_refresh()
        if not playlist_id:
            raise ValueError("No playlist ID provided.")
        if not track_ids:
            raise ValueError("No track IDs provided.")
        uris = [f"spotify:track:{tid}" if not tid.startswith("spotify:") else tid for tid in track_ids]
        payload = {"uris": uris}
        if position is not None:
            payload["position"] = position
        response = self.sp._post(f"playlists/{playlist_id}/items", payload=payload)
        self.logger.info(f"Added {len(track_ids)} tracks to playlist {playlist_id}")
        return response

    @utils.ensure_username
    def remove_tracks_from_playlist(self, playlist_id: str, track_ids: List[str]):
        if not self.auth_ok(): self.auth_refresh()
        if not playlist_id:
            raise ValueError("No playlist ID provided.")
        if not track_ids:
            raise ValueError("No track IDs provided.")
        uris = [{"uri": f"spotify:track:{tid}" if not tid.startswith("spotify:") else tid} for tid in track_ids]
        payload = {"items": uris}
        response = self.sp._delete(f"playlists/{playlist_id}/items", payload=payload)
        self.logger.info(f"Removed {len(track_ids)} tracks from playlist {playlist_id}")
        return response

    @utils.ensure_username
    def create_playlist(self, name: str, description: Optional[str] = None, public: bool = True):
        if not self.auth_ok(): self.auth_refresh()
        if not name:
            raise ValueError("Playlist name is required.")
        data = {
            "name": name,
            "public": public,
            "collaborative": False,
            "description": description or ""
        }
        playlist = self.sp._post("me/playlists", payload=data)
        self.logger.info(f"Created playlist: {name} (ID: {playlist['id']})")
        return {
            "name": playlist.get("name"),
            "id": playlist.get("id"),
            "owner": playlist.get("owner", {}).get("display_name"),
            "description": playlist.get("description", ""),
            "public": playlist.get("public"),
            "total_tracks": 0
        }

    @utils.ensure_username
    def change_playlist_details(self, playlist_id: str, name: Optional[str] = None, description: Optional[str] = None):
        if not self.auth_ok(): self.auth_refresh()
        if not playlist_id:
            raise ValueError("No playlist ID provided.")
        response = self.sp.playlist_change_details(playlist_id, name=name, description=description)
        self.logger.info(f"Changed playlist details for {playlist_id}")
        return response

    # ── Devices ──────────────────────────────────────────────────────

    def get_devices(self) -> dict:
        if not self.auth_ok(): self.auth_refresh()
        return self.sp.devices()['devices']

    def is_active_device(self):
        return any(d.get('is_active') for d in self.get_devices())

    def _get_candidate_device(self):
        devices = self.get_devices()
        if not devices:
            raise ConnectionError("No active device. Is Spotify open?")
        for device in devices:
            if device.get('is_active'):
                return device
        self.logger.info(f"No active device, assigning {devices[0]['name']}.")
        return devices[0]