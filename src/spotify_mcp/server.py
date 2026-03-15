import asyncio
import sys
import json
from typing import List, Optional

import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
from pydantic import BaseModel, Field, AnyUrl
from spotipy import SpotifyException

from . import spotify_api
from .utils import normalize_redirect_uri


def setup_logger():
    class Logger:
        def info(self, message):
            print(f"[INFO] {message}", file=sys.stderr)

        def error(self, message):
            print(f"[ERROR] {message}", file=sys.stderr)

    return Logger()


logger = setup_logger()
server = Server("spotify-mcp")

_spotify_client = None

def get_spotify_client():
    """Lazy initialization of Spotify client. Only created on first tool call."""
    global _spotify_client
    if _spotify_client is None:
        if spotify_api.REDIRECT_URI:
            spotify_api.REDIRECT_URI = normalize_redirect_uri(spotify_api.REDIRECT_URI)
        _spotify_client = spotify_api.Client(logger)
    return _spotify_client


class ToolModel(BaseModel):
    @classmethod
    def as_tool(cls):
        return types.Tool(
            name="Spotify" + cls.__name__,
            description=cls.__doc__,
            inputSchema=cls.model_json_schema()
        )

class Playback(ToolModel):
    """Manages the current playback..."""
    action: str = Field(description="Action to perform: 'get', 'start', 'pause' or 'skip'.")
    spotify_uri: Optional[str] = Field(default=None, description="Spotify uri of item to play...")
    num_skips: Optional[int] = Field(default=1, description="Number of tracks to skip for `skip` action.")

class Queue(ToolModel):
    """Manage the playback queue - get the queue or add tracks."""
    action: str = Field(description="Action to perform: 'add' or 'get'.")
    track_id: Optional[str] = Field(default=None, description="Track ID to add to queue")

class GetInfo(ToolModel):
    """Get detailed information about a Spotify item..."""
    item_uri: str = Field(description="URI of the item to get information about.")

class Search(ToolModel):
    """Search for tracks, albums, artists, or playlists on Spotify."""
    query: str = Field(description="query term")
    qtype: Optional[str] = Field(default="track", description="Type of items to search for")
    limit: Optional[int] = Field(default=5, description="Maximum number of items to return (max 10 per Spotify API limit)")

class Playlist(ToolModel):
    """Manage Spotify playlists."""
    action: str = Field(description="Action to perform: 'get', 'get_tracks', 'add_tracks', 'remove_tracks', 'change_details', 'create', 'delete'.")
    playlist_id: Optional[str] = Field(default=None, description="ID of the playlist to manage.")
    track_ids: Optional[List[str]] = Field(default=None, description="List of track IDs to add/remove.")
    name: Optional[str] = Field(default=None, description="Name for the playlist")
    description: Optional[str] = Field(default=None, description="Description for the playlist.")
    public: Optional[bool] = Field(default=True, description="Whether the playlist should be public")

class LikedSongs(ToolModel):
    """Get user's liked (saved) songs from Spotify library."""
    action: str = Field(description="Action to perform: 'get' or 'get_with_genres'.")
    limit: Optional[int] = Field(default=0, description="Max number of songs to return. 0 for all songs.")

class FollowedArtists(ToolModel):
    """Get all artists the currently logged-in Spotify user is following."""

class ArtistLatestReleases(ToolModel):
    """Get the latest released tracks for one or more artists within a given timeframe. Returns results grouped by artist_id."""
    artist_ids: List[str] = Field(description="List of Spotify artist IDs to fetch releases for.")
    days: Optional[int] = Field(default=30, description="How many days back to look for releases. Default is 30.")

@server.list_prompts()
async def handle_list_prompts() -> list[types.Prompt]:
    return []

@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    return []

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    logger.info("Listing available tools")
    tools = [
        Playback.as_tool(),
        Search.as_tool(),
        Queue.as_tool(),
        GetInfo.as_tool(),
        Playlist.as_tool(),
        LikedSongs.as_tool(),
        FollowedArtists.as_tool(),
        ArtistLatestReleases.as_tool(),
    ]
    logger.info(f"Available tools: {[tool.name for tool in tools]}")
    return tools


@server.call_tool()
async def handle_call_tool(
        name: str, arguments: dict | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    logger.info(f"Tool called: {name} with arguments: {arguments}")
    if not name.startswith("Spotify"):
        return [types.TextContent(type="text", text=f"Unknown tool: {name}")]
    arguments = arguments or {}
    spotify_client = get_spotify_client()
    try:
        match name[7:]:
            case "Playback":
                action = arguments.get("action")
                match action:
                    case "get":
                        logger.info("Attempting to get current track")
                        curr_track = await asyncio.to_thread(spotify_client.get_current_track)
                        if curr_track:
                            return [types.TextContent(type="text", text=json.dumps(curr_track, indent=2))]
                        return [types.TextContent(type="text", text="No track playing.")]
                    
                    case "start":
                        await asyncio.to_thread(
                            spotify_client.start_playback, 
                            spotify_uri=arguments.get("spotify_uri")
                        )
                        return [types.TextContent(type="text", text="Playback starting.")]
                    
                    case "pause":
                        await asyncio.to_thread(spotify_client.pause_playback)
                        return [types.TextContent(type="text", text="Playback paused.")]
                    
                    case "skip":
                        num_skips = int(arguments.get("num_skips", 1))
                        await asyncio.to_thread(spotify_client.skip_track, n=num_skips)
                        return [types.TextContent(type="text", text="Skipped to next track.")]

            case "Search":
                logger.info(f"Performing search with arguments: {arguments}")
                search_results = await asyncio.to_thread(
                    spotify_client.search,
                    query=arguments.get("query", ""),
                    qtype=arguments.get("qtype", "track"),
                    limit=arguments.get("limit", 5)
                )
                return [types.TextContent(type="text", text=json.dumps(search_results, indent=2))]

            case "Queue":
                action = arguments.get("action")
                match action:
                    case "add":
                        track_id = arguments.get("track_id")
                        if not track_id:
                            return [types.TextContent(type="text", text="track_id is required for add action")]
                        await asyncio.to_thread(spotify_client.add_to_queue, track_id)
                        return [types.TextContent(type="text", text="Track added to queue.")]
                    case "get":
                        queue = await asyncio.to_thread(spotify_client.get_queue)
                        return [types.TextContent(type="text", text=json.dumps(queue, indent=2))]

            case "GetInfo":
                item_info = await asyncio.to_thread(
                    spotify_client.get_info, 
                    item_uri=arguments.get("item_uri")
                )
                return [types.TextContent(type="text", text=json.dumps(item_info, indent=2))]

            case "Playlist":
                action = arguments.get("action")
                match action:
                    case "get":
                        playlists = await asyncio.to_thread(spotify_client.get_current_user_playlists)
                        return [types.TextContent(type="text", text=json.dumps(playlists, indent=2))]
                    
                    case "get_tracks":
                        if not arguments.get("playlist_id"):
                            return [types.TextContent(type="text", text="playlist_id is required")]
                        tracks = await asyncio.to_thread(
                            spotify_client.get_playlist_tracks, 
                            arguments.get("playlist_id")
                        )
                        return [types.TextContent(type="text", text=json.dumps(tracks, indent=2))]
                    
                    case "add_tracks":
                        track_ids = arguments.get("track_ids")
                        if isinstance(track_ids, str):
                            track_ids = json.loads(track_ids)
                        await asyncio.to_thread(
                            spotify_client.add_tracks_to_playlist,
                            playlist_id=arguments.get("playlist_id"),
                            track_ids=track_ids
                        )
                        return [types.TextContent(type="text", text="Tracks added to playlist.")]
                    
                    case "remove_tracks":
                        track_ids = arguments.get("track_ids")
                        if isinstance(track_ids, str):
                            track_ids = json.loads(track_ids)
                        await asyncio.to_thread(
                            spotify_client.remove_tracks_from_playlist,
                            playlist_id=arguments.get("playlist_id"),
                            track_ids=track_ids
                        )
                        return [types.TextContent(type="text", text="Tracks removed from playlist.")]

                    case "change_details":
                        await asyncio.to_thread(
                            spotify_client.change_playlist_details,
                            playlist_id=arguments.get("playlist_id"),
                            name=arguments.get("name"),
                            description=arguments.get("description")
                        )
                        return [types.TextContent(type="text", text="Playlist details changed.")]

                    case "create":
                        playlist = await asyncio.to_thread(
                            spotify_client.create_playlist,
                            name=arguments.get("name"),
                            description=arguments.get("description"),
                            public=arguments.get("public", True)
                        )
                        return [types.TextContent(type="text", text=json.dumps(playlist, indent=2))]

                    case "delete":
                        if not arguments.get("playlist_id"):
                            return [types.TextContent(type="text", text="playlist_id is required")]
                        await asyncio.to_thread(
                            spotify_client.delete_playlist,
                            arguments.get("playlist_id")
                        )
                        return [types.TextContent(type="text", text="Playlist deleted (unfollowed).")]

            case "LikedSongs":
                action = arguments.get("action")
                limit = arguments.get("limit", 0)

                match action:
                    case "get":
                        tracks = await asyncio.to_thread(spotify_client.get_liked_songs, limit=limit)
                        return [types.TextContent(
                            type="text",
                            text=json.dumps({"total": len(tracks), "tracks": tracks}, indent=2)
                        )]

                    case "get_with_genres":
                        tracks = await asyncio.to_thread(spotify_client.get_liked_songs, limit=limit)

                        all_artist_ids = set()
                        for track in tracks:
                            for aid in track.get('artist_ids', []):
                                all_artist_ids.add(aid)

                        genres_map = await asyncio.to_thread(
                            spotify_client.get_artists_genres, 
                            list(all_artist_ids)
                        )

                        for track in tracks:
                            track_genres = set()
                            for aid in track.get('artist_ids', []):
                                for genre in genres_map.get(aid, []):
                                    track_genres.add(genre)
                            track['genres'] = list(track_genres)
                            if 'artist_ids' in track:
                                del track['artist_ids']

                        return [types.TextContent(
                            type="text",
                            text=json.dumps({"total": len(tracks), "tracks": tracks}, indent=2)
                        )]

            case "FollowedArtists":
                artists = await asyncio.to_thread(spotify_client.get_followed_artists)
                return [types.TextContent(
                    type="text",
                    text=json.dumps({"total": len(artists), "artists": artists}, indent=2)
                )]

            case "ArtistLatestReleases":
                artist_ids = arguments.get("artist_ids")
                if isinstance(artist_ids, str):
                    artist_ids = json.loads(artist_ids)
                if not artist_ids:
                    return [types.TextContent(type="text", text="artist_ids is required")]
                days = int(arguments.get("days", 30))
                results = await asyncio.to_thread(
                    spotify_client.get_artist_latest_releases,
                    artist_ids=artist_ids,
                    days=days,
                )
                total = sum(len(v) for v in results.values())
                return [types.TextContent(
                    type="text",
                    text=json.dumps({"total": total, "days": days, "by_artist": results}, indent=2)
                )]

            case _:
                error_msg = f"Unknown tool: {name}"
                logger.error(error_msg)
                return [types.TextContent(type="text", text=error_msg)]

    except SpotifyException as se:
        error_msg = f"Spotify Client error occurred: {str(se)}"
        logger.error(error_msg)
        return [types.TextContent(type="text", text=f"An error occurred: {str(se)}")]
    except Exception as e:
        error_msg = f"Unexpected error occurred: {str(e)}"
        logger.error(error_msg)
        return [types.TextContent(type="text", text=error_msg)]


async def main():
    try:
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options()
            )
    except Exception as e:
        logger.error(f"Server error occurred: {str(e)}")
        raise