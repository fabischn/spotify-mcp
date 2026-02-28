from collections import defaultdict
from typing import Optional, Dict
import functools
from typing import Callable, TypeVar
from urllib.parse import quote, urlparse, urlunparse

from requests import RequestException

T = TypeVar('T')


def normalize_redirect_uri(url: str) -> str:
    if not url:
        return url
    parsed = urlparse(url)
    if parsed.netloc == 'localhost' or parsed.netloc.startswith('localhost:'):
        port = ''
        if ':' in parsed.netloc:
            port = ':' + parsed.netloc.split(':')[1]
        parsed = parsed._replace(netloc=f'127.0.0.1{port}')
    return urlunparse(parsed)


def parse_track(track_item: dict, detailed=False) -> Optional[dict]:
    """Parse track object. Handles both old 'track' and new 'item' field names."""
    if not track_item:
        return None
    narrowed_item = {
        'name': track_item['name'],
        'id': track_item['id'],
    }

    if 'is_playing' in track_item:
        narrowed_item['is_playing'] = track_item['is_playing']

    if detailed:
        album = track_item.get('album')
        if album:
            narrowed_item['album'] = parse_album(album)
        for k in ['track_number', 'duration_ms']:
            if k in track_item:
                narrowed_item[k] = track_item.get(k)

    if not track_item.get('is_playable', True):
        narrowed_item['is_playable'] = False

    artists = [a['name'] for a in track_item.get('artists', [])]
    if detailed:
        artists = [parse_artist(a) for a in track_item.get('artists', [])]

    if len(artists) == 1:
        narrowed_item['artist'] = artists[0]
    elif len(artists) > 1:
        narrowed_item['artists'] = artists

    return narrowed_item


def parse_artist(artist_item: dict, detailed=False) -> Optional[dict]:
    if not artist_item:
        return None
    narrowed_item = {
        'name': artist_item['name'],
        'id': artist_item['id'],
    }
    if detailed:
        narrowed_item['genres'] = artist_item.get('genres')
    return narrowed_item


def parse_playlist(playlist_item: dict, username, detailed=False) -> Optional[dict]:
    """Parse playlist object. Handles Feb 2026 API changes:
    - 'tracks' field renamed to 'items'
    - 'items.items[].track' renamed to 'items.items[].item'
    - 'items' may be absent for playlists user doesn't own
    """
    if not playlist_item:
        return None
    
    # Handle both old 'tracks' and new 'items' field names
    content_info = playlist_item.get('items') or playlist_item.get('tracks') or {}
    
    narrowed_item = {
        'name': playlist_item.get('name'),
        'id': playlist_item.get('id'),
        'owner': playlist_item.get('owner', {}).get('display_name'),
        'user_is_owner': playlist_item.get('owner', {}).get('display_name') == username,
        'total_tracks': content_info.get('total', 0) if isinstance(content_info, dict) else 0,
    }
    if detailed:
        narrowed_item['description'] = playlist_item.get('description')
        tracks = []
        raw_items = content_info.get('items', []) if isinstance(content_info, dict) else []
        for t in raw_items:
            if not t:
                continue
            # Handle both old 'track' and new 'item' field names
            track_data = t.get('item') or t.get('track')
            if track_data:
                tracks.append(parse_track(track_data))
        narrowed_item['tracks'] = tracks

    return narrowed_item


def parse_album(album_item: dict, detailed=False) -> dict:
    if not album_item:
        return {}
    narrowed_item = {
        'name': album_item['name'],
        'id': album_item['id'],
    }

    artists = [a['name'] for a in album_item.get('artists', [])]

    if detailed:
        tracks = []
        album_tracks = album_item.get('tracks', {})
        for t in album_tracks.get('items', []):
            tracks.append(parse_track(t))
        narrowed_item["tracks"] = tracks
        artists = [parse_artist(a) for a in album_item.get('artists', [])]
        for k in ['total_tracks', 'release_date', 'genres']:
            if k in album_item:
                narrowed_item[k] = album_item.get(k)

    if len(artists) == 1:
        narrowed_item['artist'] = artists[0]
    elif len(artists) > 1:
        narrowed_item['artists'] = artists

    return narrowed_item


def parse_search_results(results: Dict, qtype: str, username: Optional[str] = None):
    _results = defaultdict(list)

    for q in qtype.split(","):
        match q:
            case "track":
                for item in results.get('tracks', {}).get('items', []):
                    if not item:
                        continue
                    _results['tracks'].append(parse_track(item))
            case "artist":
                for item in results.get('artists', {}).get('items', []):
                    if not item:
                        continue
                    _results['artists'].append(parse_artist(item))
            case "playlist":
                for item in results.get('playlists', {}).get('items', []):
                    if not item:
                        continue
                    _results['playlists'].append(parse_playlist(item, username))
            case "album":
                for item in results.get('albums', {}).get('items', []):
                    if not item:
                        continue
                    _results['albums'].append(parse_album(item))
            case _:
                raise ValueError(f"Unknown qtype {qtype}")

    return dict(_results)


def parse_tracks(items: list) -> list:
    """Parse a list of playlist item objects.
    Handles both old format (item['track']) and new format (item['item']).
    """
    tracks = []
    for item in items:
        if not item:
            continue
        track_data = item.get('item') or item.get('track')
        if track_data:
            tracks.append(parse_track(track_data))
    return tracks


def build_search_query(base_query: str,
                       artist: Optional[str] = None,
                       track: Optional[str] = None,
                       album: Optional[str] = None,
                       year: Optional[str] = None,
                       year_range: Optional[tuple[int, int]] = None,
                       genre: Optional[str] = None,
                       is_hipster: bool = False,
                       is_new: bool = False
                       ) -> str:
    filters = []
    if artist:
        filters.append(f"artist:{artist}")
    if track:
        filters.append(f"track:{track}")
    if album:
        filters.append(f"album:{album}")
    if year:
        filters.append(f"year:{year}")
    if year_range:
        filters.append(f"year:{year_range[0]}-{year_range[1]}")
    if genre:
        filters.append(f"genre:{genre}")
    if is_hipster:
        filters.append("tag:hipster")
    if is_new:
        filters.append("tag:new")

    query_parts = [base_query] + filters
    return quote(" ".join(query_parts))


def validate(func: Callable[..., T]) -> Callable[..., T]:
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.auth_ok():
            self.auth_refresh()
        if not self.is_active_device():
            kwargs['device'] = self._get_candidate_device()
        return func(self, *args, **kwargs)
    return wrapper


def ensure_username(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.username is None:
            self.set_username()
        return func(self, *args, **kwargs)
    return wrapper
