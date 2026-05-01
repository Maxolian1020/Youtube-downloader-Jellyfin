import os
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from difflib import SequenceMatcher
import json

try:
    import eyed3
except ModuleNotFoundError:
    eyed3 = None

try:
    import yt_dlp
except ModuleNotFoundError:
    yt_dlp = None
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from starlette.requests import Request


APP_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = APP_ROOT / "downloads"
DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

if load_dotenv is not None:
    load_dotenv(APP_ROOT / ".env")


def _int_env(name: str, default: int, low: int, high: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
        return max(low, min(high, value))
    except Exception:
        return default


def _float_env(name: str, default: float, low: float, high: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
        return max(low, min(high, value))
    except Exception:
        return default


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name, "true" if default else "false").strip().lower()
    return value in {"1", "true", "yes", "on"}


MATCH_WORKERS = _int_env("MATCH_WORKERS", 3, 1, 8)
MATCH_PREFETCH = _int_env("MATCH_PREFETCH", 10, 2, 30)
MATCH_CANDIDATES = _int_env("MATCH_CANDIDATES", 6, 1, 15)
CACHE_DB_PATH = Path(os.getenv("SPOTIFY_CACHE_DB", str(APP_ROOT / "spotify_cache.db")))
COOKIES_FILE = os.getenv("YTDLP_COOKIES_FILE", "").strip()
COOKIES_FILES_RAW = os.getenv("YTDLP_COOKIE_FILES", "").strip()
MATCH_CACHE_FIRST = _bool_env("MATCH_CACHE_FIRST", True)
RESUME_FROM_DB = _bool_env("RESUME_FROM_DB", True)
YTDLP_SEARCH_SLEEP_SECONDS = _float_env("YTDLP_SEARCH_SLEEP_SECONDS", 0.35, 0.0, 5.0)
YTDLP_DOWNLOAD_SLEEP_SECONDS = _float_env("YTDLP_DOWNLOAD_SLEEP_SECONDS", 0.7, 0.0, 10.0)
YTDLP_ATTEMPT_SLEEP_SECONDS = _float_env("YTDLP_ATTEMPT_SLEEP_SECONDS", 0.2, 0.0, 5.0)
AUTO_RATE_LIMIT_PAUSE_SECONDS = _int_env("AUTO_RATE_LIMIT_PAUSE_SECONDS", 3600, 60, 86400)
AUTO_PAUSE_CHECK_SECONDS = _float_env("AUTO_PAUSE_CHECK_SECONDS", 15.0, 1.0, 120.0)
NORMALIZE_AUDIO = os.getenv("NORMALIZE_AUDIO", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
LOUDNORM_FILTER = os.getenv(
    "LOUDNORM_FILTER", "loudnorm=I=-14:TP=-1.5:LRA=11"
).strip()
ENABLE_MUSICBRAINZ = _bool_env("ENABLE_MUSICBRAINZ", True)
MUSICBRAINZ_TIMEOUT_SECONDS = _float_env("MUSICBRAINZ_TIMEOUT_SECONDS", 8.0, 1.0, 30.0)
MUSICBRAINZ_USER_AGENT = os.getenv(
    "MUSICBRAINZ_USER_AGENT", "jellyfin-spotify/1.0 (https://localhost; local)"
).strip()
METADATA_MAX_IMAGE_BYTES = _int_env("METADATA_MAX_IMAGE_BYTES", 5_000_000, 100_000, 25_000_000)

SUSPICIOUS_MARKERS = [
    "acoustic",
    "remix",
    "live",
    "cover",
    "sped up",
    "nightcore",
    "slowed",
    "reverb",
    "clean",
    "censored",
    "radio edit",
    "instrumental",
    "karaoke",
]

MISSING_RUNTIME_DEPS: list[str] = []
OPTIONAL_DEPS: list[str] = []
if yt_dlp is None:
    MISSING_RUNTIME_DEPS.append("yt-dlp")
if eyed3 is None:
    OPTIONAL_DEPS.append("eyed3")


class SilentLogger:
    def debug(self, msg: str) -> None:
        return

    def warning(self, msg: str) -> None:
        return

    def error(self, msg: str) -> None:
        print(msg)


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r'[\\/*?:"<>|]', "", name)
    return cleaned.strip() or "unknown"


def parse_playlist_id(playlist_input: str) -> str:
    value = playlist_input.strip()
    if "/playlist/" in value:
        match = re.search(r"/playlist/([A-Za-z0-9]+)", value)
        if not match:
            raise ValueError("Invalid playlist URL")
        return match.group(1)
    if value.startswith("spotify:playlist:"):
        return value.split(":")[-1]
    # Supports cached playlist ids and locally-created playlist ids (uuid, etc).
    if re.fullmatch(r"[A-Za-z0-9_-]+", value):
        return value
    raise ValueError("Invalid playlist id/url")


def ensure_runtime_dependencies() -> None:
    # Kept for backward compatibility; prefer the more specific checks below.
    if MISSING_RUNTIME_DEPS:
        raise RuntimeError(
            "Missing dependencies: "
            + ", ".join(MISSING_RUNTIME_DEPS)
            + ". Install with: pip install -r requirements.txt"
        )


def ensure_yt_dlp_available() -> None:
    if yt_dlp is None:
        raise RuntimeError("yt-dlp is not installed")


def init_cache_db() -> None:
    CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS playlists (
                id TEXT PRIMARY KEY,
                name TEXT,
                owner_id TEXT,
                description TEXT,
                snapshot_id TEXT,
                last_cached_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tracks (
                id TEXT PRIMARY KEY,
                name TEXT,
                album_name TEXT,
                album_id TEXT,
                duration_ms INTEGER,
                explicit INTEGER,
                track_number INTEGER,
                disc_number INTEGER,
                popularity INTEGER,
                artists_json TEXT,
                album_artists_json TEXT,
                album_images_json TEXT,
                external_urls_json TEXT,
                cached_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS artists (
                id TEXT PRIMARY KEY,
                name TEXT,
                genres_json TEXT,
                popularity INTEGER,
                followers_total INTEGER,
                fetched_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS playlist_tracks (
                playlist_id TEXT,
                position INTEGER,
                track_id TEXT,
                added_at TEXT,
                PRIMARY KEY (playlist_id, position)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_playlist_tracks_track_id ON playlist_tracks(track_id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS track_downloads (
                playlist_id TEXT,
                track_id TEXT,
                track_name TEXT,
                artist_name TEXT,
                album_name TEXT,
                status TEXT,
                suspicious INTEGER,
                selected_url TEXT,
                selected_title TEXT,
                selected_score INTEGER,
                selected_attempt INTEGER,
                matched_candidates_json TEXT,
                destination_path TEXT,
                last_error TEXT,
                updated_at TEXT,
                PRIMARY KEY (playlist_id, track_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS track_download_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                playlist_id TEXT,
                track_id TEXT,
                job_id TEXT,
                mode TEXT,
                event_type TEXT,
                status TEXT,
                selected_url TEXT,
                selected_title TEXT,
                selected_score INTEGER,
                cookie_file TEXT,
                format_used TEXT,
                error TEXT,
                payload_json TEXT,
                created_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_checkpoints (
                playlist_id TEXT,
                mode TEXT,
                output_dir TEXT,
                last_job_id TEXT,
                status TEXT,
                total INTEGER,
                completed INTEGER,
                failed INTEGER,
                current_index INTEGER,
                current_track TEXT,
                started_at TEXT,
                finished_at TEXT,
                updated_at TEXT,
                stopped_by_user INTEGER,
                PRIMARY KEY (playlist_id, mode, output_dir)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_track_download_events_track
            ON track_download_events(playlist_id, track_id, created_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_track_download_events_job
            ON track_download_events(job_id, created_at)
            """
        )
        existing_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(track_downloads)").fetchall()
        }
        wanted_cols = {
            "job_id": "TEXT",
            "mode": "TEXT",
            "selected_uploader": "TEXT",
            "selected_channel": "TEXT",
            "selected_duration_seconds": "REAL",
            "selected_view_count": "INTEGER",
            "selected_cookie_file": "TEXT",
            "selected_format": "TEXT",
            "download_started_at": "TEXT",
            "download_finished_at": "TEXT",
            "download_elapsed_ms": "INTEGER",
            "file_size_bytes": "INTEGER",
            "original_bitrate_kbps": "REAL",
            "final_bitrate_kbps": "REAL",
            "audio_duration_seconds": "REAL",
            "audio_sample_rate_hz": "INTEGER",
            "audio_channels": "INTEGER",
            "normalization_applied": "INTEGER",
            "normalization_filter": "TEXT",
            "attempt_history_json": "TEXT",
            "extra_metadata_json": "TEXT",
            "created_at": "TEXT",
            "suspicious_reason": "TEXT",
            "suspicious_manual_override": "INTEGER",
            "review_status": "TEXT",
            "review_notes": "TEXT",
            "review_manual_url": "TEXT",
            "review_updated_at": "TEXT",
        }
        for col, col_type in wanted_cols.items():
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE track_downloads ADD COLUMN {col} {col_type}")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_track_downloads_status ON track_downloads(status, suspicious)"
        )
        existing_track_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(tracks)").fetchall()
        }
        wanted_track_cols = {
            "metadata_source": "TEXT",
            "metadata_updated_at": "TEXT",
            "release_date": "TEXT",
            "musicbrainz_recording_id": "TEXT",
            "musicbrainz_release_id": "TEXT",
            "isrc_json": "TEXT",
        }
        for col, col_type in wanted_track_cols.items():
            if col not in existing_track_cols:
                conn.execute(f"ALTER TABLE tracks ADD COLUMN {col} {col_type}")
        conn.commit()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, default=str)


@dataclass
class CachedArtist:
    id: str = ""
    name: str = ""


@dataclass
class CachedImage:
    url: str = ""
    height: int | None = None
    width: int | None = None


@dataclass
class CachedAlbum:
    id: str = ""
    name: str = ""
    artists: list[CachedArtist] = field(default_factory=list)
    images: list[CachedImage] = field(default_factory=list)


@dataclass
class CachedTrack:
    id: str = ""
    name: str = ""
    album: CachedAlbum = field(default_factory=CachedAlbum)
    artists: list[CachedArtist] = field(default_factory=list)
    duration_ms: int = 0
    explicit: bool = False
    track_number: int = 0
    disc_number: int = 0


@dataclass
class CachedPlaylist:
    id: str = ""
    name: str = ""
    description: str = ""
    snapshot_id: str = ""


@dataclass
class CachedPlaylistItem:
    track: CachedTrack | None = None
    added_at: str = ""


def is_playlist_cached(playlist_id: str) -> bool:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM playlist_tracks WHERE playlist_id = ? LIMIT 1",
            (playlist_id,),
        ).fetchone()
    return bool(row)


def load_cached_playlist_items(playlist_id: str) -> tuple[CachedPlaylist, list[CachedPlaylistItem]]:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        playlist_row = conn.execute(
            """
            SELECT id, name, description, snapshot_id
            FROM playlists
            WHERE id = ?
            """,
            (playlist_id,),
        ).fetchone()
        playlist = CachedPlaylist(
            id=playlist_id,
            name=(playlist_row[1] if playlist_row else "") or "",
            description=(playlist_row[2] if playlist_row else "") or "",
            snapshot_id=(playlist_row[3] if playlist_row else "") or "",
        )

        rows = conn.execute(
            """
            SELECT
                pt.added_at,
                t.id,
                t.name,
                t.album_name,
                t.album_id,
                t.duration_ms,
                t.explicit,
                t.track_number,
                t.disc_number,
                t.artists_json,
                t.album_artists_json,
                t.album_images_json
            FROM playlist_tracks pt
            JOIN tracks t ON t.id = pt.track_id
            WHERE pt.playlist_id = ?
            ORDER BY pt.position ASC
            """,
            (playlist_id,),
        ).fetchall()

    items: list[CachedPlaylistItem] = []
    for r in rows:
        try:
            artists = json.loads(r[9] or "[]")
        except Exception:
            artists = []
        try:
            album_artists = json.loads(r[10] or "[]")
        except Exception:
            album_artists = []
        try:
            images = json.loads(r[11] or "[]")
        except Exception:
            images = []

        track_artists = [
            CachedArtist(id=str(a.get("id", "") or ""), name=str(a.get("name", "") or ""))
            for a in artists
            if isinstance(a, dict)
        ]
        if not track_artists:
            track_artists = [CachedArtist(id="", name="unknown")]
        alb_artists = [
            CachedArtist(id=str(a.get("id", "") or ""), name=str(a.get("name", "") or ""))
            for a in album_artists
            if isinstance(a, dict)
        ]
        alb_images = [
            CachedImage(
                url=str(i.get("url", "") or ""),
                height=(int(i.get("height")) if i.get("height") is not None else None),
                width=(int(i.get("width")) if i.get("width") is not None else None),
            )
            for i in images
            if isinstance(i, dict)
        ]

        album = CachedAlbum(
            id=str(r[4] or ""),
            name=str(r[3] or ""),
            artists=alb_artists,
            images=alb_images,
        )
        track = CachedTrack(
            id=str(r[1] or ""),
            name=str(r[2] or ""),
            album=album,
            artists=track_artists,
            duration_ms=int(r[5] or 0),
            explicit=bool(int(r[6] or 0)),
            track_number=int(r[7] or 0),
            disc_number=int(r[8] or 0),
        )
        items.append(CachedPlaylistItem(track=track, added_at=str(r[0] or "")))

    return playlist, items


def resolve_playlist_id(playlist_id: str | None, playlist: str | None) -> str:
    if playlist_id and playlist_id.strip():
        return playlist_id.strip()
    if playlist and playlist.strip():
        return parse_playlist_id(playlist)
    raise ValueError("Missing playlist_id")


def get_configured_cookie_files() -> list[str]:
    values: list[str] = []
    if COOKIES_FILE:
        values.append(COOKIES_FILE)
    if COOKIES_FILES_RAW:
        values.extend([v.strip() for v in COOKIES_FILES_RAW.split(",") if v.strip()])
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            seen.add(value)
            deduped.append(value)
    return deduped


def get_valid_cookie_files() -> list[str]:
    valid: list[str] = []
    for path_str in get_configured_cookie_files():
        path = Path(path_str)
        if not path.exists():
            continue
        try:
            first = path.read_text(encoding="utf-8", errors="ignore").splitlines()[:1]
            header = first[0].strip() if first else ""
            if header in {"# Netscape HTTP Cookie File", "# HTTP Cookie File"}:
                valid.append(str(path))
        except Exception:
            continue
    return valid


def log_download_event(
    playlist_id: str,
    track: Any,
    event_type: str,
    status: str,
    payload: dict[str, Any] | None = None,
    job_id: str = "",
    mode: str = "",
    selected_url: str = "",
    selected_title: str = "",
    selected_score: int | None = None,
    cookie_file: str = "",
    format_used: str = "",
    error: str = "",
) -> None:
    init_cache_db()
    now = datetime.utcnow().isoformat()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO track_download_events (
                playlist_id, track_id, job_id, mode, event_type, status, selected_url,
                selected_title, selected_score, cookie_file, format_used, error, payload_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                playlist_id,
                getattr(track, "id", ""),
                job_id,
                mode,
                event_type,
                status,
                selected_url,
                selected_title,
                selected_score if selected_score is not None else None,
                cookie_file,
                format_used,
                error,
                _json_dumps(payload or {}),
                now,
            ),
        )
        conn.commit()


def _http_get_bytes(url: str, timeout: float, headers: dict[str, str] | None = None) -> tuple[bytes, str]:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        content_type = str(resp.headers.get("Content-Type", "") or "")
        raw = resp.read(METADATA_MAX_IMAGE_BYTES + 1)
        if len(raw) > METADATA_MAX_IMAGE_BYTES:
            raise RuntimeError(f"metadata download exceeded limit ({METADATA_MAX_IMAGE_BYTES} bytes)")
        return raw, content_type


def _http_get_json(url: str, timeout: float, headers: dict[str, str] | None = None) -> dict[str, Any]:
    raw, _ct = _http_get_bytes(url, timeout=timeout, headers=headers)
    try:
        return json.loads(raw.decode("utf-8", errors="replace"))
    except Exception:
        return {}


def _guess_image_mime(url: str, content_type: str) -> str:
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct in {"image/jpeg", "image/jpg", "image/png", "image/webp"}:
        return "image/jpeg" if ct == "image/jpg" else ct
    lowered = (url or "").lower()
    if lowered.endswith(".png"):
        return "image/png"
    if lowered.endswith(".webp"):
        return "image/webp"
    return "image/jpeg"


def get_youtube_thumbnail_url(video_url: str) -> str:
    if yt_dlp is None:
        return ""
    try:
        with yt_dlp.YoutubeDL(
            {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "logger": SilentLogger(),
            }
        ) as ydl:
            info = ydl.extract_info(video_url, download=False) or {}
        thumb = str(info.get("thumbnail", "") or "").strip()
        return thumb
    except Exception:
        return ""


def musicbrainz_find_artist_tag(artist_name: str) -> str:
    if not ENABLE_MUSICBRAINZ:
        return ""
    name = (artist_name or "").strip()
    if not name:
        return ""
    headers = {"User-Agent": MUSICBRAINZ_USER_AGENT}
    query = urllib.parse.quote(f'artist:"{name}"')
    search_url = f"https://musicbrainz.org/ws/2/artist/?query={query}&fmt=json&limit=1"
    data = _http_get_json(search_url, timeout=MUSICBRAINZ_TIMEOUT_SECONDS, headers=headers)
    artists = data.get("artists") or []
    if not artists or not isinstance(artists, list):
        return ""
    mbid = str((artists[0] or {}).get("id", "") or "").strip()
    if not mbid:
        return ""
    detail_url = f"https://musicbrainz.org/ws/2/artist/{mbid}?inc=tags&fmt=json"
    detail = _http_get_json(detail_url, timeout=MUSICBRAINZ_TIMEOUT_SECONDS, headers=headers)
    tags = detail.get("tags") or []
    if not isinstance(tags, list) or not tags:
        return ""
    best = max(
        (
            (str(t.get("name", "") or "").strip(), int(t.get("count", 0) or 0))
            for t in tags
            if isinstance(t, dict)
        ),
        key=lambda x: x[1],
        default=("", 0),
    )
    return best[0]


def musicbrainz_find_cover_art_url(artist_name: str, album_name: str) -> str:
    if not ENABLE_MUSICBRAINZ:
        return ""
    artist = (artist_name or "").strip()
    album = (album_name or "").strip()
    if not artist or not album:
        return ""
    headers = {"User-Agent": MUSICBRAINZ_USER_AGENT}
    query = urllib.parse.quote(f'artist:"{artist}" AND release:"{album}"')
    search_url = f"https://musicbrainz.org/ws/2/release/?query={query}&fmt=json&limit=1"
    data = _http_get_json(search_url, timeout=MUSICBRAINZ_TIMEOUT_SECONDS, headers=headers)
    releases = data.get("releases") or []
    if not releases or not isinstance(releases, list):
        return ""
    mbid = str((releases[0] or {}).get("id", "") or "").strip()
    if not mbid:
        return ""
    return f"https://coverartarchive.org/release/{mbid}/front-500"


def musicbrainz_cover_art_url_for_release(release_id: str) -> str:
    release = str(release_id or "").strip()
    if not release:
        return ""
    return f"https://coverartarchive.org/release/{release}/front-500"


def musicbrainz_lookup_track_metadata(
    artist_name: str, track_name: str, album_name: str = ""
) -> dict[str, str]:
    if not ENABLE_MUSICBRAINZ:
        return {}
    artist = (artist_name or "").strip()
    track = (track_name or "").strip()
    album = (album_name or "").strip()
    if not track:
        return {}

    headers = {"User-Agent": MUSICBRAINZ_USER_AGENT}
    parts = [f'recording:"{track}"']
    if artist:
        parts.append(f'artist:"{artist}"')
    if album:
        parts.append(f'release:"{album}"')
    query = urllib.parse.quote(" AND ".join(parts))
    search_url = f"https://musicbrainz.org/ws/2/recording/?query={query}&fmt=json&limit=5"
    data = _http_get_json(search_url, timeout=MUSICBRAINZ_TIMEOUT_SECONDS, headers=headers)
    recordings = data.get("recordings") or []
    if not isinstance(recordings, list) or not recordings:
        return {}

    best = None
    best_score = -1
    for row in recordings:
        if not isinstance(row, dict):
            continue
        score = int(row.get("score", 0) or 0)
        if score > best_score:
            best = row
            best_score = score
    if not isinstance(best, dict):
        return {}

    def _matches_expected(expected: str, actual: str) -> bool:
        left = _norm(expected)
        right = _norm(actual)
        if not left or not right:
            return False
        return left == right or left in right or right in left

    best_id = str(best.get("id", "") or "").strip()
    detail = best
    if best_id:
        detail_url = (
            f"https://musicbrainz.org/ws/2/recording/{best_id}"
            "?inc=releases+artists&fmt=json"
        )
        fetched = _http_get_json(detail_url, timeout=MUSICBRAINZ_TIMEOUT_SECONDS, headers=headers)
        if isinstance(fetched, dict) and fetched:
            detail = fetched

    resolved_track = str(detail.get("title", "") or best.get("title", "") or track).strip()
    artist_credit = detail.get("artist-credit") or best.get("artist-credit") or []
    resolved_artist = artist
    if isinstance(artist_credit, list) and artist_credit:
        first = artist_credit[0] if isinstance(artist_credit[0], dict) else {}
        artist_obj = first.get("artist", {}) if isinstance(first, dict) else {}
        resolved_artist = str(artist_obj.get("name", "") or resolved_artist).strip()

    if not _matches_expected(track, resolved_track):
        return {}
    if artist and not _matches_expected(artist, resolved_artist):
        return {}

    release_list = detail.get("releases") or best.get("releases") or []
    resolved_album = album
    resolved_release_id = ""
    resolved_release_date = ""
    resolved_track_number = None
    resolved_disc_number = None
    resolved_release_primary = ""
    if isinstance(release_list, list) and release_list:
        preferred = None
        for release in release_list:
            if not isinstance(release, dict):
                continue
            release_title = str(release.get("title", "") or "").strip()
            if not release_title:
                continue
            rg = release.get("release-group", {}) if isinstance(release.get("release-group"), dict) else {}
            primary = str(rg.get("primary-type", "") or "").strip().lower()
            if primary in {"album", "ep", "single"}:
                preferred = release
                break
            if preferred is None:
                preferred = release
        if isinstance(preferred, dict):
            proposed_album = str(preferred.get("title", "") or "").strip()
            resolved_release_id = str(preferred.get("id", "") or "").strip()
            resolved_release_date = str(preferred.get("date", "") or "").strip()
            rg = preferred.get("release-group", {}) if isinstance(preferred.get("release-group"), dict) else {}
            resolved_release_primary = str(rg.get("primary-type", "") or "").strip().lower()
            if album:
                if _matches_expected(album, proposed_album):
                    resolved_album = proposed_album
            elif resolved_release_primary == "single":
                resolved_album = proposed_album

    if resolved_release_id:
        try:
            release_detail_url = (
                f"https://musicbrainz.org/ws/2/release/{resolved_release_id}"
                "?inc=recordings&fmt=json"
            )
            release_detail = _http_get_json(
                release_detail_url,
                timeout=MUSICBRAINZ_TIMEOUT_SECONDS,
                headers=headers,
            )
            media = release_detail.get("media") or []
            lowered_track = _norm(resolved_track or track)
            for medium in media if isinstance(media, list) else []:
                tracks = medium.get("tracks") if isinstance(medium, dict) else []
                position = medium.get("position") if isinstance(medium, dict) else None
                for medium_track in tracks if isinstance(tracks, list) else []:
                    if not isinstance(medium_track, dict):
                        continue
                    title_value = str(medium_track.get("title", "") or "").strip()
                    if _norm(title_value) == lowered_track:
                        number_value = str(medium_track.get("number", "") or "").strip()
                        if number_value.isdigit():
                            resolved_track_number = int(number_value)
                        else:
                            pos_value = medium_track.get("position")
                            if pos_value is not None:
                                resolved_track_number = int(pos_value)
                        if position is not None:
                            resolved_disc_number = int(position)
                        break
                if resolved_track_number is not None:
                    break
        except Exception:
            pass

    length_ms = detail.get("length")
    resolved_duration_ms = int(length_ms) if length_ms is not None else None
    isrc_list = detail.get("isrcs") or []

    return {
        "artist_name": resolved_artist or artist,
        "track_name": resolved_track or track,
        "album_name": resolved_album or album,
        "musicbrainz_recording_id": best_id,
        "musicbrainz_release_id": resolved_release_id,
        "release_date": resolved_release_date,
        "track_number": resolved_track_number,
        "disc_number": resolved_disc_number,
        "duration_ms": resolved_duration_ms,
        "isrc_json": _json_dumps(isrc_list) if isinstance(isrc_list, list) else "[]",
        "metadata_source": "musicbrainz",
        "match_score": best_score,
        "release_primary_type": resolved_release_primary,
    }


def get_cover_art_bytes(
    track: Any, video_url: str = "", allow_youtube_fallback: bool = True
) -> tuple[bytes, str] | None:
    image_url = ""
    imported_track = str(getattr(track, "id", "") or "").startswith("yt_")
    if imported_track:
        try:
            init_cache_db()
            with sqlite3.connect(CACHE_DB_PATH) as conn:
                row = conn.execute(
                    """
                    SELECT metadata_source, musicbrainz_release_id
                    FROM tracks
                    WHERE id = ?
                    """,
                    (str(getattr(track, "id", "") or ""),),
                ).fetchone()
            metadata_source = str((row[0] if row else "") or "").strip().lower()
            release_id = str((row[1] if row else "") or "").strip()
            if metadata_source == "musicbrainz" and release_id:
                image_url = musicbrainz_cover_art_url_for_release(release_id)
            else:
                return None
        except Exception:
            return None
    try:
        images = [] if imported_track else (getattr(getattr(track, "album", None), "images", None) or [])
        if images:
            image_url = str(getattr(images[0], "url", "") or "").strip()
    except Exception:
        image_url = ""

    if not image_url and video_url and allow_youtube_fallback:
        image_url = get_youtube_thumbnail_url(video_url)

    if not image_url:
        try:
            artist_name = (
                getattr((getattr(track, "artists", None) or [None])[0], "name", "") or ""
            )
            album_name = getattr(getattr(track, "album", None), "name", "") or ""
            image_url = musicbrainz_find_cover_art_url(artist_name, album_name)
        except Exception:
            image_url = ""

    if not image_url:
        return None

    data, content_type = _http_get_bytes(image_url, timeout=20, headers={"User-Agent": MUSICBRAINZ_USER_AGENT})
    return data, _guess_image_mime(image_url, content_type)


def get_artist_genre(_spotify: Any, artist: Any) -> str:
    artist_id = getattr(artist, "id", "") or ""
    artist_name = getattr(artist, "name", "") or ""
    if not artist_id:
        return musicbrainz_find_artist_tag(artist_name) if artist_name else ""

    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            "SELECT genres_json FROM artists WHERE id = ?",
            (artist_id,),
        ).fetchone()
        if row and row[0]:
            try:
                genres = json.loads(row[0])
                if genres:
                    return genres[-1]
            except Exception:
                pass

    return musicbrainz_find_artist_tag(artist_name) if artist_name else ""


def probe_audio_file(path: Path) -> dict[str, Any]:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration,bit_rate,size:stream=codec_type,bit_rate,sample_rate,channels",
        "-of",
        "json",
        str(path),
    ]
    try:
        proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
        data = json.loads(proc.stdout or "{}")
        fmt = data.get("format") or {}
        streams = data.get("streams") or []
        audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), {})
        format_br = float(fmt.get("bit_rate", 0) or 0)
        stream_br = float(audio_stream.get("bit_rate", 0) or 0)
        bitrate_kbps = (stream_br or format_br) / 1000.0 if (stream_br or format_br) else None
        return {
            "duration_seconds": float(fmt.get("duration", 0) or 0) or None,
            "bitrate_kbps": bitrate_kbps,
            "sample_rate_hz": int(audio_stream.get("sample_rate", 0) or 0) or None,
            "channels": int(audio_stream.get("channels", 0) or 0) or None,
            "file_size_bytes": int(fmt.get("size", 0) or 0) or None,
        }
    except Exception:
        return {}


def cache_playlist_snapshot(
    playlist_id: str, playlist: Any, items: list[Any], job: "DownloadJob | None" = None
) -> int:
    init_cache_db()
    now = datetime.utcnow().isoformat()
    cached_tracks = 0

    with sqlite3.connect(CACHE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO playlists (id, name, owner_id, description, snapshot_id, last_cached_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                owner_id=excluded.owner_id,
                description=excluded.description,
                snapshot_id=excluded.snapshot_id,
                last_cached_at=excluded.last_cached_at
            """,
            (
                playlist_id,
                getattr(playlist, "name", ""),
                getattr(getattr(playlist, "owner", None), "id", ""),
                getattr(playlist, "description", ""),
                getattr(playlist, "snapshot_id", ""),
                now,
            ),
        )

        for pos, item in enumerate(items, start=1):
            track = getattr(item, "track", None)
            if not track or not getattr(track, "id", None):
                continue
            artists = [
                {"id": getattr(a, "id", ""), "name": getattr(a, "name", "")}
                for a in (getattr(track, "artists", None) or [])
            ]
            album_artists = [
                {"id": getattr(a, "id", ""), "name": getattr(a, "name", "")}
                for a in (getattr(getattr(track, "album", None), "artists", None) or [])
            ]
            images = [
                {
                    "url": getattr(i, "url", ""),
                    "height": getattr(i, "height", None),
                    "width": getattr(i, "width", None),
                }
                for i in (getattr(getattr(track, "album", None), "images", None) or [])
            ]

            conn.execute(
                """
                INSERT INTO tracks (
                    id, name, album_name, album_id, duration_ms, explicit, track_number, disc_number,
                    popularity, artists_json, album_artists_json, album_images_json, external_urls_json, cached_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    album_name=excluded.album_name,
                    album_id=excluded.album_id,
                    duration_ms=excluded.duration_ms,
                    explicit=excluded.explicit,
                    track_number=excluded.track_number,
                    disc_number=excluded.disc_number,
                    popularity=excluded.popularity,
                    artists_json=excluded.artists_json,
                    album_artists_json=excluded.album_artists_json,
                    album_images_json=excluded.album_images_json,
                    external_urls_json=excluded.external_urls_json,
                    cached_at=excluded.cached_at
                """,
                (
                    track.id,
                    getattr(track, "name", ""),
                    getattr(getattr(track, "album", None), "name", ""),
                    getattr(getattr(track, "album", None), "id", ""),
                    getattr(track, "duration_ms", 0) or 0,
                    1 if getattr(track, "explicit", False) else 0,
                    getattr(track, "track_number", 0) or 0,
                    getattr(track, "disc_number", 0) or 0,
                    getattr(track, "popularity", 0) or 0,
                    _json_dumps(artists),
                    _json_dumps(album_artists),
                    _json_dumps(images),
                    _json_dumps(getattr(track, "external_urls", {}) or {}),
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO playlist_tracks (playlist_id, position, track_id, added_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(playlist_id, position) DO UPDATE SET
                    track_id=excluded.track_id,
                    added_at=excluded.added_at
                """,
                (playlist_id, pos, track.id, getattr(item, "added_at", "")),
            )
            cached_tracks += 1

        conn.commit()

    if job is not None:
        job.cached_tracks = cached_tracks
    return cached_tracks


def assess_suspicious_match(matched_title: str, matched_score: int) -> tuple[bool, str]:
    lowered = matched_title.lower()
    for marker in SUSPICIOUS_MARKERS:
        if marker in lowered:
            return True, f"title contains '{marker}'"
    if matched_score < 20:
        return True, f"low confidence score ({matched_score})"
    return False, ""


def save_download_result(
    playlist_id: str,
    track: Any,
    status: str,
    suspicious: bool,
    job_id: str = "",
    mode: str = "",
    selected_url: str = "",
    selected_title: str = "",
    selected_score: int | None = None,
    selected_attempt: int | None = None,
    selected_uploader: str = "",
    selected_channel: str = "",
    selected_duration_seconds: float | None = None,
    selected_view_count: int | None = None,
    selected_cookie_file: str = "",
    selected_format: str = "",
    download_started_at: str = "",
    download_finished_at: str = "",
    download_elapsed_ms: int | None = None,
    file_size_bytes: int | None = None,
    original_bitrate_kbps: float | None = None,
    final_bitrate_kbps: float | None = None,
    audio_duration_seconds: float | None = None,
    audio_sample_rate_hz: int | None = None,
    audio_channels: int | None = None,
    normalization_applied: bool = False,
    attempt_history: list[dict[str, Any]] | None = None,
    extra_metadata: dict[str, Any] | None = None,
    matched_candidates: list[dict[str, Any]] | None = None,
    destination_path: str = "",
    last_error: str = "",
    suspicious_reason: str = "",
    suspicious_manual_override: bool = False,
) -> None:
    init_cache_db()
    now = datetime.utcnow().isoformat()
    artist_name = ""
    if getattr(track, "artists", None):
        artist_name = getattr(track.artists[0], "name", "")

    with sqlite3.connect(CACHE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO track_downloads (
                playlist_id, track_id, track_name, artist_name, album_name, status, suspicious,
                selected_url, selected_title, selected_score, selected_attempt, matched_candidates_json,
                destination_path, last_error, job_id, mode, selected_uploader, selected_channel,
                selected_duration_seconds, selected_view_count, selected_cookie_file, selected_format,
                download_started_at, download_finished_at, download_elapsed_ms, file_size_bytes,
                original_bitrate_kbps, final_bitrate_kbps, audio_duration_seconds, audio_sample_rate_hz,
                audio_channels, normalization_applied, normalization_filter, attempt_history_json,
                extra_metadata_json, created_at, updated_at, suspicious_reason, suspicious_manual_override
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(playlist_id, track_id) DO UPDATE SET
                track_name=excluded.track_name,
                artist_name=excluded.artist_name,
                album_name=excluded.album_name,
                status=CASE
                    WHEN excluded.mode = 'scan_issues' AND track_downloads.status IN ('downloaded', 'skipped')
                        THEN track_downloads.status
                    ELSE excluded.status
                END,
                suspicious=excluded.suspicious,
                selected_url=COALESCE(NULLIF(excluded.selected_url, ''), track_downloads.selected_url),
                selected_title=COALESCE(NULLIF(excluded.selected_title, ''), track_downloads.selected_title),
                selected_score=COALESCE(excluded.selected_score, track_downloads.selected_score),
                selected_attempt=COALESCE(excluded.selected_attempt, track_downloads.selected_attempt),
                matched_candidates_json=CASE
                    WHEN excluded.matched_candidates_json = '[]' THEN track_downloads.matched_candidates_json
                    ELSE excluded.matched_candidates_json
                END,
                destination_path=COALESCE(NULLIF(excluded.destination_path, ''), track_downloads.destination_path),
                last_error=CASE
                    WHEN excluded.status = 'downloaded' THEN ''
                    ELSE COALESCE(NULLIF(excluded.last_error, ''), track_downloads.last_error)
                END,
                job_id=excluded.job_id,
                mode=excluded.mode,
                selected_uploader=COALESCE(NULLIF(excluded.selected_uploader, ''), track_downloads.selected_uploader),
                selected_channel=COALESCE(NULLIF(excluded.selected_channel, ''), track_downloads.selected_channel),
                selected_duration_seconds=COALESCE(excluded.selected_duration_seconds, track_downloads.selected_duration_seconds),
                selected_view_count=COALESCE(excluded.selected_view_count, track_downloads.selected_view_count),
                selected_cookie_file=COALESCE(NULLIF(excluded.selected_cookie_file, ''), track_downloads.selected_cookie_file),
                selected_format=COALESCE(NULLIF(excluded.selected_format, ''), track_downloads.selected_format),
                download_started_at=COALESCE(NULLIF(excluded.download_started_at, ''), track_downloads.download_started_at),
                download_finished_at=COALESCE(NULLIF(excluded.download_finished_at, ''), track_downloads.download_finished_at),
                download_elapsed_ms=COALESCE(excluded.download_elapsed_ms, track_downloads.download_elapsed_ms),
                file_size_bytes=COALESCE(excluded.file_size_bytes, track_downloads.file_size_bytes),
                original_bitrate_kbps=COALESCE(excluded.original_bitrate_kbps, track_downloads.original_bitrate_kbps),
                final_bitrate_kbps=COALESCE(excluded.final_bitrate_kbps, track_downloads.final_bitrate_kbps),
                audio_duration_seconds=COALESCE(excluded.audio_duration_seconds, track_downloads.audio_duration_seconds),
                audio_sample_rate_hz=COALESCE(excluded.audio_sample_rate_hz, track_downloads.audio_sample_rate_hz),
                audio_channels=COALESCE(excluded.audio_channels, track_downloads.audio_channels),
                normalization_applied=COALESCE(excluded.normalization_applied, track_downloads.normalization_applied),
                normalization_filter=COALESCE(NULLIF(excluded.normalization_filter, ''), track_downloads.normalization_filter),
                attempt_history_json=CASE
                    WHEN excluded.attempt_history_json = '[]' THEN track_downloads.attempt_history_json
                    ELSE excluded.attempt_history_json
                END,
                extra_metadata_json=CASE
                    WHEN excluded.extra_metadata_json = '{}' THEN track_downloads.extra_metadata_json
                    ELSE excluded.extra_metadata_json
                END,
                suspicious_reason=COALESCE(NULLIF(excluded.suspicious_reason, ''), track_downloads.suspicious_reason),
                suspicious_manual_override=CASE
                    WHEN excluded.suspicious_manual_override = 1 THEN 1
                    ELSE COALESCE(track_downloads.suspicious_manual_override, 0)
                END,
                updated_at=excluded.updated_at
            """,
            (
                playlist_id,
                getattr(track, "id", ""),
                getattr(track, "name", ""),
                artist_name,
                getattr(getattr(track, "album", None), "name", ""),
                status,
                1 if suspicious else 0,
                selected_url,
                selected_title,
                selected_score if selected_score is not None else None,
                selected_attempt if selected_attempt is not None else None,
                _json_dumps(
                    matched_candidates or []
                ),
                destination_path,
                last_error,
                job_id,
                mode,
                selected_uploader,
                selected_channel,
                selected_duration_seconds if selected_duration_seconds is not None else None,
                selected_view_count if selected_view_count is not None else None,
                selected_cookie_file,
                selected_format,
                download_started_at,
                download_finished_at,
                download_elapsed_ms if download_elapsed_ms is not None else None,
                file_size_bytes if file_size_bytes is not None else None,
                original_bitrate_kbps if original_bitrate_kbps is not None else None,
                final_bitrate_kbps if final_bitrate_kbps is not None else None,
                audio_duration_seconds if audio_duration_seconds is not None else None,
                audio_sample_rate_hz if audio_sample_rate_hz is not None else None,
                audio_channels if audio_channels is not None else None,
                1 if normalization_applied else 0,
                LOUDNORM_FILTER if normalization_applied else "",
                _json_dumps(attempt_history or []),
                _json_dumps(extra_metadata or {}),
                now,
                now,
                suspicious_reason,
                1 if suspicious_manual_override else 0,
            ),
        )
        conn.commit()


def load_issue_track_ids(playlist_id: str) -> set[str]:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT track_id
            FROM track_downloads
            WHERE playlist_id = ? AND (status = 'failed' OR suspicious = 1)
            """,
            (playlist_id,),
        ).fetchall()
    return {row[0] for row in rows if row and row[0]}


def get_issue_counts(playlist_id: str) -> tuple[int, int]:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        failed = conn.execute(
            "SELECT COUNT(*) FROM track_downloads WHERE playlist_id = ? AND status = 'failed'",
            (playlist_id,),
        ).fetchone()[0]
        suspicious = conn.execute(
            "SELECT COUNT(*) FROM track_downloads WHERE playlist_id = ? AND suspicious = 1",
            (playlist_id,),
        ).fetchone()[0]
    return int(failed), int(suspicious)


def get_track_download_record(playlist_id: str, track_id: str) -> dict[str, Any] | None:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT selected_title, selected_url, selected_score, suspicious, status
            FROM track_downloads
            WHERE playlist_id = ? AND track_id = ?
            """,
            (playlist_id, track_id),
        ).fetchone()
    if not row:
        return None
    return {
        "selected_title": row[0] or "",
        "selected_url": row[1] or "",
        "selected_score": row[2],
        "suspicious": bool(row[3]),
        "status": row[4] or "",
    }


def _normalize_candidate_rows(raw_candidates: Any) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    if not isinstance(raw_candidates, list):
        return normalized
    for row in raw_candidates:
        if isinstance(row, dict):
            url = str(row.get("url", "")).strip()
            if not url:
                continue
            normalized.append(
                {
                    "url": url,
                    "title": str(row.get("title", "")),
                    "score": int(row.get("score", 0) or 0),
                    "uploader": str(row.get("uploader", "")),
                    "channel": str(row.get("channel", "")),
                    "duration": row.get("duration"),
                    "view_count": row.get("view_count"),
                    "search_query": str(row.get("search_query", "")),
                    "search_cookie_file": str(row.get("search_cookie_file", "")),
                }
            )
            continue
        if isinstance(row, (list, tuple)) and len(row) >= 3:
            url = str(row[0]).strip()
            if not url:
                continue
            normalized.append(
                {
                    "url": url,
                    "title": str(row[1] or ""),
                    "score": int(row[2] or 0),
                    "uploader": "",
                    "channel": "",
                    "duration": None,
                    "view_count": None,
                    "search_query": "",
                    "search_cookie_file": "",
                }
            )
    normalized.sort(key=lambda row: int(row.get("score", -99999)), reverse=True)
    return normalized[:MATCH_CANDIDATES]


def get_cached_match_candidates(playlist_id: str, track_id: str) -> list[dict[str, Any]]:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT matched_candidates_json, selected_url, selected_title, selected_score
            FROM track_downloads
            WHERE playlist_id = ? AND track_id = ?
            """,
            (playlist_id, track_id),
        ).fetchone()
    if not row:
        return []

    parsed: list[dict[str, Any]] = []
    raw_json = row[0] or ""
    if raw_json:
        try:
            parsed = _normalize_candidate_rows(json.loads(raw_json))
        except Exception:
            parsed = []

    selected_url = str(row[1] or "").strip()
    selected_title = str(row[2] or "")
    selected_score = int(row[3] or 0)
    if selected_url and all(c.get("url") != selected_url for c in parsed):
        parsed.insert(
            0,
            {
                "url": selected_url,
                "title": selected_title,
                "score": selected_score,
                "uploader": "",
                "channel": "",
                "duration": None,
                "view_count": None,
                "search_query": "cached_selected",
                "search_cookie_file": "",
            },
        )
    return parsed[:MATCH_CANDIDATES]


def is_manual_suspicious_override(playlist_id: str, track_id: str) -> bool:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT suspicious_manual_override
            FROM track_downloads
            WHERE playlist_id = ? AND track_id = ?
            """,
            (playlist_id, track_id),
        ).fetchone()
    return bool(row and int(row[0] or 0) == 1)


def get_resume_completed_track_ids(playlist_id: str) -> set[str]:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT track_id
            FROM track_downloads
            WHERE playlist_id = ? AND status IN ('downloaded', 'skipped')
            """,
            (playlist_id,),
        ).fetchall()
    return {str(r[0]) for r in rows if r and r[0]}


def save_job_checkpoint(job: "DownloadJob", playlist_id: str, stopped_by_user: bool = False) -> None:
    init_cache_db()
    now = datetime.utcnow().isoformat()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO job_checkpoints (
                playlist_id, mode, output_dir, last_job_id, status, total, completed, failed,
                current_index, current_track, started_at, finished_at, updated_at, stopped_by_user
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(playlist_id, mode, output_dir) DO UPDATE SET
                last_job_id=excluded.last_job_id,
                status=excluded.status,
                total=excluded.total,
                completed=excluded.completed,
                failed=excluded.failed,
                current_index=excluded.current_index,
                current_track=excluded.current_track,
                started_at=excluded.started_at,
                finished_at=excluded.finished_at,
                updated_at=excluded.updated_at,
                stopped_by_user=excluded.stopped_by_user
            """,
            (
                playlist_id,
                job.mode,
                job.output_dir,
                job.id,
                job.status,
                job.total,
                job.completed,
                job.failed,
                job.current_index,
                job.current_track,
                job.started_at,
                job.finished_at or "",
                now,
                1 if stopped_by_user else 0,
            ),
        )
        conn.commit()


def is_youtube_rate_limited_error(error_text: str) -> bool:
    text = (error_text or "").lower()
    if not text:
        return False
    markers = [
        "rate-limited by youtube",
        "current session has been rate-limited",
        "this content isn't available, try again later",
        "try again later",
        "use `-t sleep`",
    ]
    return any(marker in text for marker in markers)


def list_issue_rows(playlist_id: str) -> list[dict[str, Any]]:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT track_id, track_name, artist_name, album_name, status, suspicious,
                   selected_title, selected_score, last_error, updated_at, suspicious_reason, suspicious_manual_override
            FROM track_downloads
            WHERE playlist_id = ? AND (status = 'failed' OR suspicious = 1)
            ORDER BY updated_at DESC
            """,
            (playlist_id,),
        ).fetchall()
    result = []
    for r in rows:
        result.append(
            {
                "track_id": r[0],
                "track_name": r[1],
                "artist_name": r[2],
                "album_name": r[3],
                "status": r[4],
                "suspicious": bool(r[5]),
                "selected_title": r[6] or "",
                "selected_score": r[7],
                "last_error": r[8] or "",
                "updated_at": r[9] or "",
                "suspicious_reason": r[10] or "",
                "suspicious_manual_override": bool(r[11]),
            }
        )
    return result


def resolve_suspicious_tracks(playlist_id: str, track_ids: list[str]) -> int:
    if not track_ids:
        return 0
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        placeholders = ",".join("?" for _ in track_ids)
        params = [playlist_id, *track_ids]
        rows = conn.execute(
            f"""
            SELECT track_id, destination_path, status, suspicious
            FROM track_downloads
            WHERE playlist_id = ? AND track_id IN ({placeholders})
            """,
            params,
        ).fetchall()

        cursor = conn.execute(
            f"""
            UPDATE track_downloads
            SET suspicious = 0,
                suspicious_reason = 'manual override',
                suspicious_manual_override = 1
            WHERE playlist_id = ? AND track_id IN ({placeholders})
            """,
            params,
        )

        # If the file already exists on disk, treat stale failed state as recovered.
        for track_id, destination_path, status, _suspicious in rows:
            if not destination_path:
                continue
            try:
                if Path(destination_path).exists() and status != "downloaded":
                    conn.execute(
                        """
                        UPDATE track_downloads
                        SET status = 'downloaded', last_error = '', updated_at = ?
                        WHERE playlist_id = ? AND track_id = ?
                        """,
                        (datetime.utcnow().isoformat(), playlist_id, track_id),
                    )
            except Exception:
                continue

        conn.commit()
        return int(cursor.rowcount or 0)


def set_manual_track_source(
    playlist_id: str, track_id: str, youtube_url: str, manual_title: str = ""
) -> None:
    init_cache_db()
    now = datetime.utcnow().isoformat()
    candidate = {
        "url": youtube_url,
        "title": manual_title or "manual source",
        "score": 999,
        "uploader": "manual",
        "channel": "manual",
        "duration": None,
        "view_count": None,
        "search_query": "manual_source",
        "search_cookie_file": "",
    }
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT 1 FROM track_downloads
            WHERE playlist_id = ? AND track_id = ?
            """,
            (playlist_id, track_id),
        ).fetchone()
        if not row:
            raise ValueError("track not found in issue cache; run scan/download first")

        conn.execute(
            """
            UPDATE track_downloads
            SET selected_url = ?,
                selected_title = ?,
                selected_score = 999,
                selected_uploader = 'manual',
                selected_channel = 'manual',
                matched_candidates_json = ?,
                suspicious = 0,
                suspicious_reason = 'manual source override',
                suspicious_manual_override = 1,
                status = 'failed',
                last_error = '',
                updated_at = ?
            WHERE playlist_id = ? AND track_id = ?
            """,
            (
                youtube_url,
                manual_title or "manual source",
                _json_dumps([candidate]),
                now,
                playlist_id,
                track_id,
            ),
        )
        conn.commit()


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def _tokens(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", _norm(value))
        if len(token) > 1 and token not in {"the", "and", "feat", "ft", "with"}
    }


def yt_search_score(
    target_song: str,
    target_artist: str,
    target_explicit: bool | None,
    title: str,
    uploader: str,
    channel: str,
    duration: int | None,
    expected_seconds: float,
) -> int:
    text = f"{title} {uploader} {channel}".lower()
    target_song_n = _norm(target_song)
    target_artist_n = _norm(target_artist)
    title_n = _norm(title)
    uploader_n = _norm(uploader)
    channel_n = _norm(channel)
    song_tokens = _tokens(target_song)
    artist_tokens = _tokens(target_artist)
    entry_tokens = _tokens(text)

    positive = [
        "official audio",
        "audio",
        "lyrics",
        "lyric video",
        "topic",
        "provided to youtube",
        "auto-generated",
    ]
    negative = [
        "official video",
        "music video",
        " mv ",
        "live",
        "concert",
        "performance",
        "reaction",
        "trailer",
        "clean",
        "censored",
        "radio edit",
        "edited",
    ]

    optional_penalties = [
        "sped up",
        "nightcore",
        "slowed",
        "reverb",
        "8d",
        "cover",
        "parody",
        "karaoke",
        "instrumental",
        "remix",
    ]

    score = 0

    song_overlap = len(song_tokens.intersection(entry_tokens))
    artist_overlap = len(artist_tokens.intersection(entry_tokens))
    score += song_overlap * 4
    score += artist_overlap * 12

    artist_fuzzy = 0.0
    if target_artist_n:
        artist_fuzzy = max(
            SequenceMatcher(None, target_artist_n, uploader_n).ratio() if uploader_n else 0,
            SequenceMatcher(None, target_artist_n, channel_n).ratio() if channel_n else 0,
            SequenceMatcher(None, target_artist_n, title_n).ratio() if title_n else 0,
        )
        if artist_fuzzy >= 0.82:
            score += 10
        elif artist_fuzzy >= 0.72:
            score += 6

    if artist_tokens and artist_overlap == 0:
        score -= 40 if artist_fuzzy < 0.62 else 8

    if song_tokens:
        song_ratio = song_overlap / max(1, len(song_tokens))
        if song_ratio < 0.35:
            score -= 25
        elif song_ratio < 0.55:
            score -= 10

    if artist_tokens:
        artist_ratio = artist_overlap / max(1, len(artist_tokens))
        if artist_ratio < 0.5 and artist_fuzzy < 0.72:
            score -= 18

    if target_song_n and target_song_n in title_n:
        score += 9

    if target_artist_n and target_artist_n in _norm(f"{uploader} {channel} {title}"):
        score += 8

    sim = SequenceMatcher(None, target_song_n, title_n).ratio() if target_song_n else 0
    if sim >= 0.8:
        score += 10
    elif sim >= 0.65:
        score += 5
    elif sim < 0.45:
        score -= 10

    for word in positive:
        if word in text:
            score += 3
    for word in negative:
        if word in text:
            score -= 5

    if target_explicit is True:
        if "explicit" in text:
            score += 4
        if any(x in text for x in ["clean", "censored", "radio edit", "edited"]):
            score -= 20
    elif target_explicit is False and "explicit" in text:
        score -= 2

    for marker in optional_penalties:
        if marker in text and marker not in target_song_n:
            score -= 8

    if duration and expected_seconds > 0:
        diff = abs(duration - expected_seconds)
        if diff <= 5:
            score += 5
        elif diff <= 15:
            score += 3
        elif diff <= 30:
            score += 1
        elif diff > 60:
            score -= 3

    if " - topic" in text or "topic" in channel.lower():
        score += 4

    return score


def find_best_youtube_url(
    track: Any, ydl_base_opts: dict[str, Any]
) -> list[dict[str, Any]]:
    if yt_dlp is None:
        return []

    song = track.name
    artist = track.artists[0].name
    expected_seconds = (track.duration_ms or 0) / 1000
    target_explicit = getattr(track, "explicit", None)

    search_queries = [
        f"{song} {artist} official audio lyrics",
        f"{song} {artist}",
    ]
    opts = dict(ydl_base_opts)
    # Matching/scanning only needs metadata. Avoid format resolution failures here.
    opts.pop("format", None)
    opts.pop("postprocessors", None)
    opts["extract_flat"] = False
    opts["skip_download"] = True
    opts["ignoreerrors"] = True
    opts["ignore_no_formats_error"] = True

    ranked_by_url: dict[str, dict[str, Any]] = {}
    cookie_files = get_valid_cookie_files()
    cookie_modes = cookie_files + [""]

    for idx, search_query in enumerate(search_queries):
        extracted = False
        for cookie_file in cookie_modes:
            phase_opts = dict(opts)
            if cookie_file:
                phase_opts["cookiefile"] = cookie_file
            else:
                phase_opts.pop("cookiefile", None)
            try:
                with yt_dlp.YoutubeDL(phase_opts) as ydl:
                    info = ydl.extract_info(f"ytsearch10:{search_query}", download=False)
                entries = (info or {}).get("entries") or []
                phase_bonus = max(0, 6 - (idx * 3))
                for entry in entries:
                    if not entry:
                        continue
                    url = entry.get("webpage_url") or entry.get("url")
                    if not url:
                        continue
                    score = yt_search_score(
                        target_song=song,
                        target_artist=artist,
                        target_explicit=target_explicit,
                        title=entry.get("title", ""),
                        uploader=entry.get("uploader", ""),
                        channel=entry.get("channel", ""),
                        duration=entry.get("duration"),
                        expected_seconds=expected_seconds,
                    )
                    candidate_score = score + phase_bonus
                    prev = ranked_by_url.get(url)
                    if prev is None or candidate_score > int(prev.get("score", -99999)):
                        ranked_by_url[url] = {
                            "url": url,
                            "title": entry.get("title", ""),
                            "score": candidate_score,
                            "uploader": entry.get("uploader", "") or "",
                            "channel": entry.get("channel", "") or "",
                            "duration": entry.get("duration"),
                            "view_count": entry.get("view_count"),
                            "search_query": search_query,
                            "search_cookie_file": cookie_file,
                        }
                extracted = True
                break
            except Exception as exc:
                print(f"Search phase failed for {song} - {artist}: {exc}")
        if YTDLP_SEARCH_SLEEP_SECONDS > 0:
            time.sleep(YTDLP_SEARCH_SLEEP_SECONDS)
        if not extracted:
            continue

    if not ranked_by_url:
        return []

    ranked = [row for row in ranked_by_url.values() if int(row.get("score", -99999)) >= -10]
    ranked.sort(key=lambda row: int(row.get("score", -99999)), reverse=True)
    return ranked[:MATCH_CANDIDATES]


def build_ydl_base_opts(quality: str) -> dict[str, Any]:
    opts: dict[str, Any] = {
        "format": "bestaudio/best",
        "quiet": True,
        "noplaylist": True,
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": quality,
            }
        ],
        "logger": SilentLogger(),
    }
    return opts


def normalize_audio_file(path: Path, quality: str) -> None:
    if not NORMALIZE_AUDIO:
        return
    normalized_path = path.with_name(path.stem + ".normalized.mp3")
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-y",
        "-i",
        str(path),
        "-af",
        LOUDNORM_FILTER,
        "-c:a",
        "libmp3lame",
        "-b:a",
        f"{quality}k",
        str(normalized_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    normalized_path.replace(path)


def download_candidate_with_fallback(
    ydl_base_opts: dict[str, Any], outtmpl: str, video_url: str
) -> dict[str, Any]:
    format_chain = ["bestaudio/best", "best", "worst"]
    last_error = ""
    attempt_logs: list[dict[str, Any]] = []
    cookie_files = get_valid_cookie_files()
    cookie_modes = cookie_files + [""]
    for cookie_file in cookie_modes:
        for fmt in format_chain:
            started = time.time()
            ydl_opts = dict(ydl_base_opts)
            if cookie_file:
                ydl_opts["cookiefile"] = cookie_file
            else:
                ydl_opts.pop("cookiefile", None)
            ydl_opts["outtmpl"] = outtmpl
            ydl_opts["format"] = fmt
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([video_url])
                elapsed_ms = int((time.time() - started) * 1000)
                attempt_logs.append(
                    {
                        "format": fmt,
                        "cookie_file": cookie_file,
                        "status": "ok",
                        "elapsed_ms": elapsed_ms,
                    }
                )
                return {
                    "ok": True,
                    "error": "",
                    "format": fmt,
                    "cookie_file": cookie_file,
                    "elapsed_ms": elapsed_ms,
                    "attempts": attempt_logs,
                }
            except Exception as exc:
                last_error = str(exc)
                elapsed_ms = int((time.time() - started) * 1000)
                attempt_logs.append(
                    {
                        "format": fmt,
                        "cookie_file": cookie_file,
                        "status": "error",
                        "elapsed_ms": elapsed_ms,
                        "error": last_error,
                    }
                )
                if "does not look like a Netscape format cookies file" in last_error:
                    break
                # If one account/session is rate-limited, keep rotating cookie modes.
                if is_youtube_rate_limited_error(last_error):
                    continue
                if "Requested format is not available" not in last_error:
                    return {
                        "ok": False,
                        "error": last_error,
                        "format": fmt,
                        "cookie_file": cookie_file,
                        "elapsed_ms": elapsed_ms,
                        "attempts": attempt_logs,
                    }
            if YTDLP_ATTEMPT_SLEEP_SECONDS > 0:
                time.sleep(YTDLP_ATTEMPT_SLEEP_SECONDS)
    return {
        "ok": False,
        "error": last_error,
        "format": "",
        "cookie_file": "",
        "elapsed_ms": 0,
        "attempts": attempt_logs,
    }


@dataclass
class DownloadJob:
    id: str
    playlist_input: str
    quality: str
    output_dir: str
    mode: str = "download"
    status: str = "queued"
    logs: list[str] = field(default_factory=list)
    total: int = 0
    completed: int = 0
    failed: int = 0
    cached_tracks: int = 0
    suspicious_tracks: int = 0
    failed_details: list[dict[str, str]] = field(default_factory=list)
    extra_files: list[str] = field(default_factory=list)
    current_index: int = 0
    current_track: str = ""
    playlist_name: str = ""
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    finished_at: str | None = None
    control_state: str = "queued"
    pause_requested: bool = False
    stop_requested: bool = False
    _pause_logged: bool = False
    auto_paused_until: float = 0.0
    auto_pause_reason: str = ""
    _last_auto_pause_log_at: float = 0.0

    def log(self, message: str) -> None:
        timestamp = datetime.utcnow().strftime("%H:%M:%S")
        line = f"[{timestamp}] {message}"
        self.logs.append(line)
        self.logs[:] = self.logs[-200:]
        print(line)

    def add_failure(self, track_label: str, reason: str) -> None:
        self.failed_details.append({"track": track_label, "reason": reason})
        self.failed_details[:] = self.failed_details[-200:]

    def wait_if_paused_or_stopped(self) -> str:
        if self.stop_requested:
            return "stop"
        now_ts = time.time()
        if self.auto_paused_until > now_ts and not self.stop_requested:
            self.control_state = "auto-paused"
            self.status = "paused"
            if (now_ts - self._last_auto_pause_log_at) >= 60:
                remaining = int(max(0, self.auto_paused_until - now_ts))
                self.log(
                    f"Auto-paused due to YouTube rate limit ({remaining}s remaining)."
                )
                self._last_auto_pause_log_at = now_ts
            time.sleep(AUTO_PAUSE_CHECK_SECONDS)
            return self.wait_if_paused_or_stopped()
        if self.auto_paused_until and self.auto_paused_until <= now_ts:
            self.auto_paused_until = 0.0
            self.auto_pause_reason = ""
            self._last_auto_pause_log_at = 0.0
            self.log("Auto-pause window ended, resuming processing.")
        while self.pause_requested and not self.stop_requested:
            self.control_state = "paused"
            self.status = "paused"
            if not self._pause_logged:
                self.log("Paused by user")
                self._pause_logged = True
            time.sleep(0.4)
        if self.stop_requested:
            return "stop"
        if self._pause_logged:
            self.log("Resumed by user")
            self._pause_logged = False
        self.control_state = "running"
        if self.status == "paused":
            self.status = "running"
        return "run"

    def trigger_auto_pause(self, seconds: int, reason: str) -> None:
        target = time.time() + max(1, int(seconds))
        if target <= self.auto_paused_until:
            return
        self.auto_paused_until = target
        self.auto_pause_reason = reason
        self._last_auto_pause_log_at = 0.0
        self.control_state = "auto-paused"
        self.status = "paused"
        self.log(
            f"Auto-paused for {int(seconds)}s due to YouTube rate limit. Reason: {reason}"
        )


class DownloadRequest(BaseModel):
    playlist_id: str | None = Field(
        default=None,
        description="Cached playlist id (from /api/playlists). Preferred.",
    )
    playlist: str | None = Field(
        default=None,
        description="Legacy playlist URL/URI/ID (Spotify URL parsing only; no Spotify API calls).",
    )
    quality: str = Field(default="320", pattern=r"^(190|320)$")
    output_dir: str = Field(default=str(DEFAULT_OUTPUT_DIR))
    mode: str = Field(
        default="download",
        pattern=r"^(download|scan_issues|retry_issues|scan_missing)$",
    )


class IssueResolveRequest(BaseModel):
    playlist_id: str | None = Field(default=None, description="Cached playlist id")
    playlist: str | None = Field(default=None, description="Legacy playlist URL/URI/ID")
    track_ids: list[str] = Field(default_factory=list)


class ManualSourceRequest(BaseModel):
    playlist_id: str | None = Field(default=None, description="Cached playlist id")
    playlist: str | None = Field(default=None, description="Legacy playlist URL/URI/ID")
    track_id: str = Field(..., min_length=4)
    youtube_url: str = Field(..., min_length=8)
    title: str = Field(default="", max_length=300)


class JobControlRequest(BaseModel):
    action: str = Field(..., pattern=r"^(pause|resume|stop)$")


class CreatePlaylistRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)


class AddTrackRequest(BaseModel):
    artist_name: str = Field(..., min_length=1, max_length=300)
    track_name: str = Field(..., min_length=1, max_length=300)
    album_name: str = Field(default="", max_length=300)
    duration_ms: int = Field(default=0, ge=0, le=60 * 60 * 1000)
    explicit: bool = Field(default=False)


class ImportYoutubeVideoRequest(BaseModel):
    url: str = Field(..., min_length=8, max_length=2000)
    playlist_id: str = Field(..., min_length=1, max_length=200)
    output_dir: str | None = Field(default=None, description="Optional output directory for downloads")


class ReviewNextResponse(BaseModel):
    playlist_id: str
    track_id: str
    position: int | None = None
    artist_name: str = ""
    track_name: str = ""
    album_name: str = ""
    status: str = ""
    destination_path: str = ""
    selected_url: str = ""
    selected_title: str = ""
    selected_score: int | None = None
    suspicious: bool = False
    last_error: str = ""
    updated_at: str = ""
    review_status: str = ""
    review_updated_at: str = ""
    duration_ms: int | None = None
    explicit: bool | None = None
    track_number: int | None = None
    disc_number: int | None = None
    release_date: str = ""
    metadata_source: str = ""


class ReviewActionRequest(BaseModel):
    playlist_id: str = Field(..., min_length=1, max_length=200)
    track_id: str = Field(..., min_length=1, max_length=200)
    notes: str = Field(default="", max_length=2000)


class ReviewManualSourceRequest(BaseModel):
    playlist_id: str = Field(..., min_length=1, max_length=200)
    track_id: str = Field(..., min_length=1, max_length=200)
    youtube_url: str = Field(..., min_length=8, max_length=2000)
    title: str = Field(default="", max_length=300)
    notes: str = Field(default="", max_length=2000)


def _review_row_to_response(row: Any) -> ReviewNextResponse:
    return ReviewNextResponse(
        playlist_id=str(row[0] or ""),
        track_id=str(row[1] or ""),
        position=(int(row[2]) if row[2] is not None else None),
        artist_name=str(row[3] or ""),
        track_name=str(row[4] or ""),
        album_name=str(row[5] or ""),
        status=str(row[6] or ""),
        destination_path=str(row[7] or ""),
        selected_url=str(row[8] or ""),
        selected_title=str(row[9] or ""),
        selected_score=(int(row[10]) if row[10] is not None else None),
        suspicious=bool(int(row[11] or 0)),
        last_error=str(row[12] or ""),
        updated_at=str(row[13] or ""),
        review_status=str(row[14] or ""),
        review_updated_at=str(row[15] or ""),
        duration_ms=(int(row[16]) if row[16] is not None else None),
        explicit=(bool(int(row[17])) if row[17] is not None else None),
        track_number=(int(row[18]) if row[18] is not None else None),
        disc_number=(int(row[19]) if row[19] is not None else None),
        release_date=str(row[20] or ""),
        metadata_source=str(row[21] or ""),
    )


def _review_select_fields() -> str:
    return """
        td.playlist_id,
        td.track_id,
        pt.position,
        td.artist_name,
        td.track_name,
        td.album_name,
        td.status,
        td.destination_path,
        td.selected_url,
        td.selected_title,
        td.selected_score,
        td.suspicious,
        td.last_error,
        td.updated_at,
        td.review_status,
        td.review_updated_at,
        t.duration_ms,
        t.explicit,
        t.track_number,
        t.disc_number,
        t.release_date,
        t.metadata_source
    """


jobs: dict[str, DownloadJob] = {}
jobs_lock = threading.Lock()

app = FastAPI(title="Playlist Downloader")
app.mount("/static", StaticFiles(directory=APP_ROOT / "static"), name="static")
templates = Jinja2Templates(directory=str(APP_ROOT / "templates"))


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/review", response_class=HTMLResponse)
def review_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("review.html", {"request": request})


@app.get("/favicon.ico")
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/api/health")
def health() -> dict[str, Any]:
    if MISSING_RUNTIME_DEPS:
        return {
            "status": "degraded",
            "missing_required": ", ".join(MISSING_RUNTIME_DEPS),
            "missing_optional": ", ".join(OPTIONAL_DEPS) if OPTIONAL_DEPS else "",
        }
    return {
        "status": "ok",
        "missing_optional": ", ".join(OPTIONAL_DEPS) if OPTIONAL_DEPS else "",
    }




@app.get("/api/playlists")
def list_playlists_api() -> dict[str, Any]:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT
                p.id,
                p.name,
                p.description,
                p.last_cached_at,
                (SELECT COUNT(*) FROM playlist_tracks pt WHERE pt.playlist_id = p.id) AS track_count
            FROM playlists p
            ORDER BY COALESCE(p.last_cached_at, '') DESC, COALESCE(p.name, '') ASC
            """
        ).fetchall()
    items = [
        {
            "id": str(r[0] or ""),
            "name": str(r[1] or ""),
            "description": str(r[2] or ""),
            "last_cached_at": str(r[3] or ""),
            "track_count": int(r[4] or 0),
        }
        for r in rows
        if r and r[0]
    ]
    return {"count": len(items), "items": items}


@app.post("/api/playlists")
def create_playlist_api(payload: CreatePlaylistRequest) -> dict[str, Any]:
    init_cache_db()
    playlist_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO playlists (id, name, owner_id, description, snapshot_id, last_cached_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                playlist_id,
                payload.name.strip(),
                "local",
                payload.description.strip(),
                "",
                now,
            ),
        )
        conn.commit()
    return {"playlist_id": playlist_id, "name": payload.name.strip()}


@app.get("/api/playlists/{playlist_id}/tracks")
def list_playlist_tracks_api(playlist_id: str) -> dict[str, Any]:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT
                pt.position,
                t.id,
                t.name,
                t.album_name,
                t.duration_ms,
                t.explicit,
                t.artists_json
            FROM playlist_tracks pt
            JOIN tracks t ON t.id = pt.track_id
            WHERE pt.playlist_id = ?
            ORDER BY pt.position ASC
            """,
            (playlist_id,),
        ).fetchall()
    items: list[dict[str, Any]] = []
    for r in rows:
        try:
            artists = json.loads(r[6] or "[]")
        except Exception:
            artists = []
        artist_name = ""
        if isinstance(artists, list) and artists:
            first = artists[0] if isinstance(artists[0], dict) else {}
            artist_name = str(first.get("name", "") or "")
        items.append(
            {
                "position": int(r[0] or 0),
                "track_id": str(r[1] or ""),
                "track_name": str(r[2] or ""),
                "album_name": str(r[3] or ""),
                "duration_ms": int(r[4] or 0),
                "explicit": bool(int(r[5] or 0)),
                "artist_name": artist_name,
            }
        )
    return {"playlist_id": playlist_id, "count": len(items), "items": items}


@app.post("/api/playlists/{playlist_id}/tracks")
def add_playlist_track_api(playlist_id: str, payload: AddTrackRequest) -> dict[str, Any]:
    init_cache_db()
    track_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    artist_name = payload.artist_name.strip()
    track_name = payload.track_name.strip()
    album_name = payload.album_name.strip()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM playlists WHERE id = ?",
            (playlist_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="playlist not found")

        max_pos_row = conn.execute(
            "SELECT MAX(position) FROM playlist_tracks WHERE playlist_id = ?",
            (playlist_id,),
        ).fetchone()
        next_pos = int((max_pos_row[0] or 0) if max_pos_row else 0) + 1

        conn.execute(
            """
            INSERT INTO tracks (
                id, name, album_name, album_id, duration_ms, explicit, track_number, disc_number,
                popularity, artists_json, album_artists_json, album_images_json, external_urls_json, cached_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                track_id,
                track_name,
                album_name,
                "",
                int(payload.duration_ms or 0),
                1 if payload.explicit else 0,
                0,
                0,
                0,
                _json_dumps([{"id": "", "name": artist_name}]),
                _json_dumps([{"id": "", "name": artist_name}]),
                _json_dumps([]),
                _json_dumps({}),
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO playlist_tracks (playlist_id, position, track_id, added_at)
            VALUES (?, ?, ?, ?)
            """,
            (playlist_id, next_pos, track_id, now),
        )
        conn.execute(
            """
            UPDATE playlists
            SET last_cached_at = ?
            WHERE id = ?
            """,
            (now, playlist_id),
        )
        conn.commit()
    return {"playlist_id": playlist_id, "track_id": track_id, "position": next_pos}


def _extract_youtube_list_id(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query or "")
        list_id = (qs.get("list", [""]) or [""])[0]
        return str(list_id or "").strip()
    except Exception:
        return ""


def _parse_artist_title_from_text(text: str) -> tuple[str, str]:
    value = (text or "").strip()
    if not value:
        return "", ""
    # Common patterns: "Artist - Title", "Artist – Title"
    for sep in [" - ", " – ", " — ", "-"]:
        if sep in value:
            left, right = value.split(sep, 1)
            left = left.strip()
            right = right.strip()
            if left and right:
                return left, right
    return "", value


def _clean_youtube_title(text: str) -> str:
    value = (text or "").strip()
    if not value:
        return ""
    value = re.sub(r"\[[^\]]*\]", "", value)
    value = re.sub(r"\((official|lyrics?|audio|video|visualizer|mv)[^)]*\)", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip(" -")
    return value.strip()


def youtube_video_music_metadata(video_url: str) -> dict[str, str]:
    if yt_dlp is None or not video_url:
        return {}
    try:
        with yt_dlp.YoutubeDL(
            {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "extract_flat": False,
                "logger": SilentLogger(),
            }
        ) as ydl:
            info = ydl.extract_info(video_url, download=False) or {}
    except Exception:
        return {}
    return {
        "artist_name": str(info.get("artist", "") or "").strip(),
        "track_name": str(info.get("track", "") or "").strip(),
        "album_name": str(info.get("album", "") or "").strip(),
        "title": str(info.get("title", "") or "").strip(),
    }


def resolve_youtube_import_metadata(
    title: str,
    artist_name: str,
    track_name: str,
    album_name: str,
    fallback_uploader: str = "",
    source_url: str = "",
) -> dict[str, str]:
    resolved_artist = (artist_name or "").strip()
    resolved_track = (track_name or "").strip()
    resolved_album = (album_name or "").strip()
    cleaned_title = _clean_youtube_title(title)
    cleaned_track = _clean_youtube_title(resolved_track)

    if cleaned_track and (
        resolved_track.lower() == (title or "").strip().lower()
        or any(marker in resolved_track.lower() for marker in ["official", "lyrics", "video", "audio"])
    ):
        resolved_track = cleaned_track

    if not resolved_artist or not resolved_track:
        parsed_artist, parsed_track = _parse_artist_title_from_text(cleaned_title)
        resolved_artist = resolved_artist or parsed_artist
        resolved_track = resolved_track or parsed_track

    if not resolved_track:
        resolved_track = cleaned_title or (title or "").strip() or "unknown"
    if not resolved_artist:
        resolved_artist = (fallback_uploader or "").strip() or "unknown"

    if source_url and (not resolved_album or not resolved_artist or not resolved_track):
        try:
            yt_meta = youtube_video_music_metadata(source_url)
            yt_title = yt_meta.get("title", "") or ""
            if yt_title and yt_title != title:
                cleaned_title = _clean_youtube_title(yt_title)
            resolved_artist = yt_meta.get("artist_name", "") or resolved_artist
            resolved_track = yt_meta.get("track_name", "") or resolved_track
            resolved_album = yt_meta.get("album_name", "") or resolved_album
            if (not resolved_artist or not resolved_track) and cleaned_title:
                parsed_artist, parsed_track = _parse_artist_title_from_text(cleaned_title)
                resolved_artist = resolved_artist or parsed_artist
                resolved_track = resolved_track or parsed_track
        except Exception:
            pass

    enriched: dict[str, Any] = {}
    try:
        enriched = musicbrainz_lookup_track_metadata(
            artist_name=resolved_artist,
            track_name=resolved_track,
            album_name=resolved_album,
        )
        if enriched:
            resolved_artist = enriched.get("artist_name", "") or resolved_artist
            resolved_track = enriched.get("track_name", "") or resolved_track
            resolved_album = enriched.get("album_name", "") or resolved_album
    except Exception:
        pass

    return {
        "artist_name": resolved_artist or "unknown",
        "track_name": resolved_track or "unknown",
        "album_name": resolved_album or "",
        "track_number": enriched.get("track_number"),
        "disc_number": enriched.get("disc_number"),
        "duration_ms": enriched.get("duration_ms"),
        "release_date": str(enriched.get("release_date", "") or ""),
        "musicbrainz_recording_id": str(enriched.get("musicbrainz_recording_id", "") or ""),
        "musicbrainz_release_id": str(enriched.get("musicbrainz_release_id", "") or ""),
        "isrc_json": str(enriched.get("isrc_json", "[]") or "[]"),
        "metadata_source": str(enriched.get("metadata_source", "youtube+heuristic") or "youtube+heuristic"),
        "clear_album_images": True,
    }


def enrich_cached_track_metadata_if_needed(track: Any) -> dict[str, str]:
    track_id = str(getattr(track, "id", "") or "")
    try:
        current_artist = str(getattr((getattr(track, "artists", None) or [None])[0], "name", "") or "")
    except Exception:
        current_artist = ""
    current_track = str(getattr(track, "name", "") or "")
    current_album = str(getattr(getattr(track, "album", None), "name", "") or "")

    looks_imported = track_id.startswith("yt_") or current_artist.lower() in {"", "unknown"}
    looks_dirty = any(marker in current_track.lower() for marker in ["official", "lyrics", "video", "audio"])
    if not looks_imported and not looks_dirty and current_album:
        return {
            "artist_name": current_artist or "unknown",
            "track_name": current_track or "unknown",
            "album_name": current_album or "",
        }

    resolved = resolve_youtube_import_metadata(
        title=current_track,
        artist_name=current_artist,
        track_name=current_track,
        album_name=current_album,
        fallback_uploader=current_artist,
        source_url="",
    )

    if track_id:
        try:
            update_track_metadata_row(track_id, resolved, manual=False)
        except Exception:
            pass

    return resolved


def update_track_metadata_row(
    track_id: str,
    metadata: dict[str, Any],
    manual: bool = False,
) -> None:
    if not track_id:
        return
    init_cache_db()
    now = datetime.utcnow().isoformat()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT name, album_name, duration_ms, explicit, track_number, disc_number,
                   popularity, artists_json, album_artists_json, album_images_json, external_urls_json,
                   metadata_source
            FROM tracks
            WHERE id = ?
            """,
            (track_id,),
        ).fetchone()
        if not row:
            return

        existing_name = str(row[0] or "")
        existing_album = str(row[1] or "")
        existing_duration = row[2]
        existing_explicit = row[3]
        existing_track_number = row[4]
        existing_disc_number = row[5]
        existing_popularity = row[6]
        existing_artists_json = str(row[7] or "[]")
        existing_album_artists_json = str(row[8] or "[]")
        existing_images_json = str(row[9] or "[]")
        existing_urls_json = str(row[10] or "{}")
        existing_source = str(row[11] or "")

        is_spotify_like = not track_id.startswith("yt_")
        safe_fill_only = is_spotify_like and not manual

        def choose_text(existing: str, incoming: Any) -> str:
            candidate = str(incoming or "").strip()
            if safe_fill_only:
                return existing or candidate
            return candidate or existing

        def choose_int(existing: Any, incoming: Any) -> Any:
            if incoming is None:
                return existing
            if safe_fill_only and existing not in (None, 0, ""):
                return existing
            return incoming

        artist_name = str(metadata.get("artist_name", "") or "").strip()
        album_name = choose_text(existing_album, metadata.get("album_name", ""))
        track_name = choose_text(existing_name, metadata.get("track_name", ""))
        duration_ms = choose_int(existing_duration, metadata.get("duration_ms"))
        track_number = choose_int(existing_track_number, metadata.get("track_number"))
        disc_number = choose_int(existing_disc_number, metadata.get("disc_number"))
        explicit = choose_int(existing_explicit, metadata.get("explicit"))
        popularity = choose_int(existing_popularity, metadata.get("popularity"))

        artists_json = existing_artists_json
        album_artists_json = existing_album_artists_json
        if artist_name:
            candidate = _json_dumps([{"id": "", "name": artist_name}])
            if not safe_fill_only or existing_artists_json in {"", "[]"}:
                artists_json = candidate
            if not safe_fill_only or existing_album_artists_json in {"", "[]"}:
                album_artists_json = candidate

        images_json = existing_images_json
        if metadata.get("clear_album_images") and not safe_fill_only:
            images_json = "[]"
        incoming_images = metadata.get("album_images_json")
        if incoming_images:
            candidate = str(incoming_images)
            if not safe_fill_only or existing_images_json in {"", "[]"}:
                images_json = candidate

        urls_json = existing_urls_json
        try:
            merged_urls = json.loads(existing_urls_json or "{}")
            if not isinstance(merged_urls, dict):
                merged_urls = {}
        except Exception:
            merged_urls = {}
        incoming_urls = metadata.get("external_urls") or {}
        if isinstance(incoming_urls, dict):
            for key, value in incoming_urls.items():
                if not value:
                    continue
                if safe_fill_only and key in merged_urls and merged_urls.get(key):
                    continue
                merged_urls[key] = value
        urls_json = _json_dumps(merged_urls)

        metadata_source = existing_source or ""
        incoming_source = str(metadata.get("metadata_source", "") or "").strip()
        if manual:
            metadata_source = "manual"
        elif not safe_fill_only and incoming_source:
            metadata_source = incoming_source
        elif not metadata_source and incoming_source:
            metadata_source = incoming_source

        conn.execute(
            """
            UPDATE tracks
            SET name = ?,
                album_name = ?,
                duration_ms = ?,
                explicit = ?,
                track_number = ?,
                disc_number = ?,
                popularity = ?,
                artists_json = ?,
                album_artists_json = ?,
                album_images_json = ?,
                external_urls_json = ?,
                cached_at = ?,
                metadata_source = ?,
                metadata_updated_at = ?,
                release_date = COALESCE(NULLIF(?, ''), release_date),
                musicbrainz_recording_id = COALESCE(NULLIF(?, ''), musicbrainz_recording_id),
                musicbrainz_release_id = COALESCE(NULLIF(?, ''), musicbrainz_release_id),
                isrc_json = CASE
                    WHEN ? = '[]' OR ? = '' THEN isrc_json
                    ELSE ?
                END
            WHERE id = ?
            """,
            (
                track_name,
                album_name,
                duration_ms,
                explicit,
                track_number,
                disc_number,
                popularity,
                artists_json,
                album_artists_json,
                images_json,
                urls_json,
                now,
                metadata_source,
                now,
                str(metadata.get("release_date", "") or ""),
                str(metadata.get("musicbrainz_recording_id", "") or ""),
                str(metadata.get("musicbrainz_release_id", "") or ""),
                str(metadata.get("isrc_json", "[]") or "[]"),
                str(metadata.get("isrc_json", "[]") or "[]"),
                str(metadata.get("isrc_json", "[]") or "[]"),
                track_id,
            ),
        )
        conn.commit()


def remove_empty_parents(path: Path, stop_at: Path) -> None:
    current = path.parent
    try:
        stop_resolved = stop_at.resolve()
    except Exception:
        stop_resolved = stop_at
    while True:
        try:
            current_resolved = current.resolve()
        except Exception:
            current_resolved = current
        if current_resolved == stop_resolved or stop_resolved not in current_resolved.parents:
            break
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def locate_track_file_for_repair(
    playlist_id: str, track: Any, output_root: Path
) -> Path | None:
    track_id = str(getattr(track, "id", "") or "")
    candidate_names: list[str] = []
    current_name = f"{sanitize_filename(track.artists[0].name)} - {sanitize_filename(track.name)}.mp3"
    candidate_names.append(current_name)

    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT destination_path, selected_title
            FROM track_downloads
            WHERE playlist_id = ? AND track_id = ?
            """,
            (playlist_id, track_id),
        ).fetchone()
    if row:
        dest = str(row[0] or "").strip()
        if dest:
            path = Path(dest).expanduser()
            if path.exists() and path.is_file():
                return path
        selected_title = str(row[1] or "").strip()
        if selected_title:
            candidate_names.append(f"{sanitize_filename(selected_title)}.mp3")

    seen: set[str] = set()
    unique_names: list[str] = []
    for name in candidate_names:
        if name and name not in seen:
            seen.add(name)
            unique_names.append(name)

    for name in unique_names:
        try:
            matches = list(output_root.rglob(name))
        except Exception:
            matches = []
        for match in matches:
            if match.is_file():
                return match
    for root in _allowed_media_roots():
        if root == output_root:
            continue
        for name in unique_names:
            try:
                matches = list(root.rglob(name))
            except Exception:
                matches = []
            for match in matches:
                if match.is_file():
                    return match
    return None


def rewrite_audio_tags(
    file_path: Path, track: Any, video_url: str = "", prefer_non_youtube_art: bool = False
) -> None:
    if eyed3 is None:
        return
    audiofile = eyed3.load(str(file_path))
    if audiofile is None:
        return
    if audiofile.tag is None:
        audiofile.initTag()

    tag_meta = enrich_cached_track_metadata_if_needed(track)
    tag_artist = tag_meta.get("artist_name", "") or track.artists[0].name
    tag_title = tag_meta.get("track_name", "") or track.name
    tag_album = tag_meta.get("album_name", "") or track.album.name
    audiofile.tag.artist = tag_artist
    audiofile.tag.title = tag_title
    audiofile.tag.album = tag_album
    if track.album.artists:
        audiofile.tag.album_artist = track.album.artists[0].name
    elif tag_artist:
        audiofile.tag.album_artist = tag_artist

    genre = ""
    try:
        genre = get_artist_genre(None, track.artists[0])
    except Exception:
        genre = ""
    if genre:
        audiofile.tag.genre = genre

    art = None
    try:
        if prefer_non_youtube_art:
            image_url = musicbrainz_find_cover_art_url(tag_artist, tag_album)
            if image_url:
                image_data, mime = _http_get_bytes(
                    image_url,
                    timeout=20,
                    headers={"User-Agent": MUSICBRAINZ_USER_AGENT},
                )
                art = (image_data, _guess_image_mime(image_url, mime))
        else:
            art = get_cover_art_bytes(track, video_url=video_url)
    except Exception:
        art = None
    if art:
        image_data, mime = art
        try:
            audiofile.tag.images.set(3, image_data, mime)
        except Exception:
            pass
    audiofile.tag.save()


def unique_destination_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    counter = 2
    while True:
        candidate = path.with_name(f"{stem} ({counter}){suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def infer_library_root_from_file(file_path: Path) -> Path:
    try:
        resolved = file_path.resolve()
    except Exception:
        resolved = file_path
    roots = _allowed_media_roots()
    for root in roots:
        try:
            root_resolved = root.resolve()
        except Exception:
            root_resolved = root
        if resolved == root_resolved or root_resolved in resolved.parents:
            return root_resolved
    if len(resolved.parents) >= 3:
        return resolved.parents[2]
    return DEFAULT_OUTPUT_DIR.resolve()


def upsert_playlist_row(playlist_id: str, name: str, description: str = "") -> None:
    init_cache_db()
    now = datetime.utcnow().isoformat()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO playlists (id, name, owner_id, description, snapshot_id, last_cached_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                description=excluded.description,
                last_cached_at=excluded.last_cached_at
            """,
            (playlist_id, name, "import", description, "", now),
        )
        conn.commit()


def upsert_youtube_track_into_playlist(
    playlist_id: str,
    position: int,
    video_id: str,
    video_url: str,
    title: str,
    artist_name: str,
    track_name: str,
    album_name: str,
    thumbnails: Any,
) -> str:
    init_cache_db()
    now = datetime.utcnow().isoformat()
    track_id = f"yt_{video_id}" if video_id else str(uuid.uuid4())

    images: list[dict[str, Any]] = []

    artists_json = _json_dumps([{"id": "", "name": artist_name or "unknown"}])
    album_artists_json = artists_json
    external_urls_json = _json_dumps({"youtube": video_url})

    with sqlite3.connect(CACHE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO tracks (
                id, name, album_name, album_id, duration_ms, explicit, track_number, disc_number,
                popularity, artists_json, album_artists_json, album_images_json, external_urls_json, cached_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                album_name=excluded.album_name,
                artists_json=excluded.artists_json,
                album_artists_json=excluded.album_artists_json,
                album_images_json=excluded.album_images_json,
                external_urls_json=excluded.external_urls_json,
                cached_at=excluded.cached_at
            """,
            (
                track_id,
                track_name or (title or "unknown"),
                album_name or "",
                "",
                0,
                0,
                0,
                0,
                0,
                artists_json,
                album_artists_json,
                _json_dumps(images),
                external_urls_json,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO playlist_tracks (playlist_id, position, track_id, added_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(playlist_id, position) DO UPDATE SET
                track_id=excluded.track_id,
                added_at=excluded.added_at
            """,
            (playlist_id, int(position), track_id, now),
        )
        conn.commit()

    try:
        track_meta = {
            "artist_name": artist_name,
            "track_name": track_name or (title or "unknown"),
            "album_name": album_name,
            "duration_ms": None,
            "track_number": position,
            "disc_number": None,
            "metadata_source": "youtube_import",
            "external_urls": {"youtube": video_url},
            "clear_album_images": True,
        }
        update_track_metadata_row(track_id, track_meta, manual=False)
    except Exception:
        pass

    # Seed a manual candidate so downloads use the video directly (no search needed).
    try:
        track_obj = CachedTrack(
            id=track_id,
            name=track_name or (title or "unknown"),
            album=CachedAlbum(
                id="",
                name=album_name or "",
                artists=[CachedArtist(id="", name=artist_name or "unknown")],
                images=[CachedImage(url=images[0]["url"], height=images[0].get("height"), width=images[0].get("width"))]
                if images and images[0].get("url")
                else [],
            ),
            artists=[CachedArtist(id="", name=artist_name or "unknown")],
        )
        candidate = {
            "url": video_url,
            "title": title or "youtube",
            "score": 999,
            "uploader": "youtube",
            "channel": "youtube",
            "duration": None,
            "view_count": None,
            "search_query": "youtube_import",
            "search_cookie_file": "",
        }
        save_download_result(
            playlist_id=playlist_id,
            track=track_obj,
            status="failed",
            suspicious=False,
            selected_url=video_url,
            selected_title=title or "",
            selected_score=999,
            matched_candidates=[candidate],
            destination_path="",
            last_error="imported from youtube playlist",
            suspicious_reason="imported from youtube playlist",
        )
    except Exception:
        pass

    return track_id


@app.post("/api/import/youtube-video")
def import_youtube_video_api(payload: ImportYoutubeVideoRequest) -> dict[str, Any]:
    ensure_yt_dlp_available()
    url = payload.url.strip()
    if not re.match(r"^https?://", url, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Provide a valid URL")

    playlist_id = (payload.playlist_id or "").strip()
    if not playlist_id:
        raise HTTPException(status_code=400, detail="Select a playlist first")
    if not is_playlist_cached(playlist_id):
        raise HTTPException(status_code=400, detail="Selected playlist was not found in the database")

    try:
        with yt_dlp.YoutubeDL(
            {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "extract_flat": False,
                "logger": SilentLogger(),
            }
        ) as ydl:
            info = ydl.extract_info(url, download=False) or {}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to read YouTube video: {exc}") from exc

    title = str(info.get("title", "") or "").strip()
    artist = str(info.get("artist", "") or "").strip()
    track_name = str(info.get("track", "") or "").strip()
    album = str(info.get("album", "") or "").strip()
    resolved = resolve_youtube_import_metadata(
        title=title,
        artist_name=artist,
        track_name=track_name,
        album_name=album,
        fallback_uploader=str(info.get("uploader", "") or info.get("channel", "") or ""),
        source_url=str(info.get("webpage_url", "") or url),
    )

    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        max_pos_row = conn.execute(
            "SELECT MAX(position) FROM playlist_tracks WHERE playlist_id = ?",
            (playlist_id,),
        ).fetchone()
        next_pos = int((max_pos_row[0] or 0) if max_pos_row else 0) + 1

    track_id = upsert_youtube_track_into_playlist(
        playlist_id=playlist_id,
        position=next_pos,
        video_id=str(info.get("id", "") or "").strip(),
        video_url=str(info.get("webpage_url", "") or url),
        title=title,
        artist_name=resolved["artist_name"],
        track_name=resolved["track_name"],
        album_name=resolved["album_name"],
        thumbnails=info.get("thumbnails") or info.get("thumbnail"),
    )
    return {"playlist_id": playlist_id, "track_id": track_id, "position": next_pos}


def _review_upsert_manual_source(
    playlist_id: str, track_id: str, youtube_url: str, title: str, notes: str
) -> None:
    init_cache_db()
    now = datetime.utcnow().isoformat()
    candidate = {
        "url": youtube_url,
        "title": title or "manual source",
        "score": 999,
        "uploader": "manual",
        "channel": "manual",
        "duration": None,
        "view_count": None,
        "search_query": "manual_source",
        "search_cookie_file": "",
    }
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        # Best-effort labels from tracks table.
        track_row = conn.execute(
            "SELECT name, album_name, artists_json FROM tracks WHERE id = ?",
            (track_id,),
        ).fetchone()
        track_name = str(track_row[0] or "") if track_row else ""
        album_name = str(track_row[1] or "") if track_row else ""
        artist_name = ""
        if track_row and track_row[2]:
            try:
                artists = json.loads(track_row[2])
                if isinstance(artists, list) and artists and isinstance(artists[0], dict):
                    artist_name = str(artists[0].get("name", "") or "")
            except Exception:
                artist_name = ""

        conn.execute(
            """
            INSERT INTO track_downloads (
                playlist_id, track_id, track_name, artist_name, album_name,
                status, suspicious, selected_url, selected_title, selected_score,
                selected_uploader, selected_channel, matched_candidates_json,
                last_error, updated_at, suspicious_reason, suspicious_manual_override,
                review_status, review_notes, review_manual_url, review_updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(playlist_id, track_id) DO UPDATE SET
                status=excluded.status,
                suspicious=0,
                selected_url=excluded.selected_url,
                selected_title=excluded.selected_title,
                selected_score=excluded.selected_score,
                selected_uploader=excluded.selected_uploader,
                selected_channel=excluded.selected_channel,
                matched_candidates_json=excluded.matched_candidates_json,
                last_error='',
                updated_at=excluded.updated_at,
                suspicious_reason='review manual source override',
                suspicious_manual_override=1,
                review_status='needs_redownload',
                review_notes=excluded.review_notes,
                review_manual_url=excluded.review_manual_url,
                review_updated_at=excluded.review_updated_at
            """,
            (
                playlist_id,
                track_id,
                track_name,
                artist_name,
                album_name,
                "failed",
                0,
                youtube_url,
                title or "manual source",
                999,
                "manual",
                "manual",
                _json_dumps([candidate]),
                "",
                now,
                "review manual source override",
                1,
                "needs_redownload",
                notes or "",
                youtube_url,
                now,
            ),
        )
        conn.commit()


def build_cached_track_from_db(track_id: str) -> CachedTrack | None:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT id, name, album_name, duration_ms, explicit, track_number, disc_number,
                   artists_json, album_artists_json, album_images_json
            FROM tracks
            WHERE id = ?
            """,
            (track_id,),
        ).fetchone()
    if not row:
        return None
    try:
        artists = json.loads(row[7] or "[]")
    except Exception:
        artists = []
    try:
        album_artists = json.loads(row[8] or "[]")
    except Exception:
        album_artists = []
    try:
        images = json.loads(row[9] or "[]")
    except Exception:
        images = []
    track_artists = [
        CachedArtist(id=str(a.get("id", "") or ""), name=str(a.get("name", "") or ""))
        for a in artists if isinstance(a, dict)
    ] or [CachedArtist(id="", name="unknown")]
    alb_artists = [
        CachedArtist(id=str(a.get("id", "") or ""), name=str(a.get("name", "") or ""))
        for a in album_artists if isinstance(a, dict)
    ] or [CachedArtist(id="", name=track_artists[0].name)]
    alb_images = [
        CachedImage(
            url=str(i.get("url", "") or ""),
            height=(int(i.get("height")) if i.get("height") is not None else None),
            width=(int(i.get("width")) if i.get("width") is not None else None),
        )
        for i in images if isinstance(i, dict)
    ]
    return CachedTrack(
        id=str(row[0] or ""),
        name=str(row[1] or ""),
        album=CachedAlbum(id="", name=str(row[2] or ""), artists=alb_artists, images=alb_images),
        artists=track_artists,
        duration_ms=int(row[3] or 0),
        explicit=bool(int(row[4] or 0)),
        track_number=int(row[5] or 0),
        disc_number=int(row[6] or 0),
    )


def retag_and_move_downloaded_file(
    playlist_id: str, track_id: str
) -> tuple[str, bool]:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT destination_path, selected_url
            FROM track_downloads
            WHERE playlist_id = ? AND track_id = ?
            """,
            (playlist_id, track_id),
        ).fetchone()
    if not row:
        raise ValueError("Track download row not found")

    destination_path = str(row[0] or "").strip()
    selected_url = str(row[1] or "").strip()
    if not destination_path:
        raise ValueError("Track has no saved destination path")

    file_path = Path(destination_path).expanduser()
    if not file_path.exists() or not file_path.is_file():
        raise ValueError("Downloaded file no longer exists on disk")

    track = build_cached_track_from_db(track_id)
    if track is None:
        raise ValueError("Track metadata row not found")
    if not track.artists:
        track.artists = [CachedArtist(id="", name="unknown")]
    if not track.album.artists:
        track.album.artists = [CachedArtist(id="", name=track.artists[0].name)]

    rewrite_audio_tags(
        file_path,
        track,
        video_url=selected_url,
        prefer_non_youtube_art=track_id.startswith("yt_"),
    )

    artist = sanitize_filename(track.artists[0].name or "unknown")
    album = sanitize_filename(track.album.name or "unknown")
    song = sanitize_filename(track.name or "unknown")
    base_root = infer_library_root_from_file(file_path)
    destination_dir = base_root / artist / album
    destination_dir.mkdir(parents=True, exist_ok=True)
    desired_path = destination_dir / f"{artist} - {song}.mp3"
    old_parent = file_path.parent
    moved = False
    final_path = file_path

    try:
        same_path = file_path.resolve() == desired_path.resolve()
    except Exception:
        same_path = str(file_path) == str(desired_path)

    if not same_path:
        final_path = unique_destination_path(desired_path)
        file_path.replace(final_path)
        moved = True
        remove_empty_parents(old_parent, base_root)

    with sqlite3.connect(CACHE_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE track_downloads
            SET destination_path = ?, updated_at = ?
            WHERE playlist_id = ? AND track_id = ?
            """,
            (str(final_path), datetime.utcnow().isoformat(), playlist_id, track_id),
        )
        conn.commit()
    return str(final_path), moved


def delete_downloaded_file_if_present(playlist_id: str, track_id: str) -> dict[str, Any]:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT destination_path
            FROM track_downloads
            WHERE playlist_id = ? AND track_id = ?
            """,
            (playlist_id, track_id),
        ).fetchone()
    destination_path = str((row[0] if row else "") or "").strip()
    if not destination_path:
        return {"deleted": False, "path": "", "reason": "no destination path"}

    file_path = Path(destination_path).expanduser()
    if not file_path.exists() or not file_path.is_file():
        return {"deleted": False, "path": str(file_path), "reason": "file missing"}

    roots = _allowed_media_roots()
    if roots and not _is_under_any_root(file_path, roots):
        return {"deleted": False, "path": str(file_path), "reason": "outside allowed roots"}

    base_root = infer_library_root_from_file(file_path)
    try:
        file_path.unlink()
    except Exception as exc:
        raise RuntimeError(f"Could not delete local file: {exc}") from exc
    remove_empty_parents(file_path, base_root)
    return {"deleted": True, "path": str(file_path), "reason": ""}


def build_manual_candidate(url: str, title: str = "") -> dict[str, Any]:
    return {
        "url": url,
        "title": title or "manual source",
        "score": 999,
        "uploader": "manual",
        "channel": "manual",
        "duration": None,
        "view_count": None,
        "search_query": "manual_source",
        "search_cookie_file": "",
    }


def reset_track_for_redownload(playlist_id: str, track_id: str) -> dict[str, Any]:
    cleanup = delete_downloaded_file_if_present(playlist_id, track_id)
    now = datetime.utcnow().isoformat()
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT selected_url, selected_title, review_manual_url
            FROM track_downloads
            WHERE playlist_id = ? AND track_id = ?
            """,
            (playlist_id, track_id),
        ).fetchone()
        if not row:
            raise ValueError("Track download row not found")

        selected_url = str(row[0] or "").strip()
        selected_title = str(row[1] or "").strip()
        manual_url = str(row[2] or "").strip()
        preserved_url = manual_url
        preserved_title = selected_title if manual_url else ""
        matched_candidates_json = "[]"
        selected_score: int | None = None
        selected_uploader = ""
        selected_channel = ""
        suspicious_manual_override = 0
        if preserved_url:
            candidate = build_manual_candidate(preserved_url, preserved_title)
            matched_candidates_json = _json_dumps([candidate])
            selected_score = 999
            selected_uploader = "manual"
            selected_channel = "manual"
            suspicious_manual_override = 1

        conn.execute(
            """
            UPDATE track_downloads
            SET status = 'failed',
                suspicious = 0,
                destination_path = '',
                last_error = 'reset for fresh redownload',
                selected_url = ?,
                selected_title = ?,
                selected_score = ?,
                selected_uploader = ?,
                selected_channel = ?,
                matched_candidates_json = ?,
                suspicious_reason = 'reset for fresh redownload',
                suspicious_manual_override = ?,
                review_status = 'needs_redownload',
                review_updated_at = ?,
                updated_at = ?
            WHERE playlist_id = ? AND track_id = ?
            """,
            (
                preserved_url,
                preserved_title,
                selected_score,
                selected_uploader,
                selected_channel,
                matched_candidates_json,
                suspicious_manual_override,
                now,
                now,
                playlist_id,
                track_id,
            ),
        )
        conn.commit()

    if manual_url:
        try:
            track = build_cached_track_from_db(track_id)
            artist_name = track.artists[0].name if track and track.artists else ""
            track_name = track.name if track else ""
            album_name = track.album.name if track and track.album else ""
            resolved = resolve_youtube_import_metadata(
                title=selected_title or track_name,
                artist_name=artist_name,
                track_name=track_name,
                album_name=album_name,
                fallback_uploader=artist_name,
                source_url=manual_url,
            )
            update_track_metadata_row(track_id, resolved, manual=False)
        except Exception:
            pass

    return {
        "ok": True,
        "deleted_file": bool(cleanup.get("deleted")),
        "deleted_path": str(cleanup.get("path", "") or ""),
        "preserved_manual_url": manual_url,
    }


def delete_track_from_playlist_database(playlist_id: str, track_id: str) -> dict[str, Any]:
    cleanup = delete_downloaded_file_if_present(playlist_id, track_id)
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            "SELECT position FROM playlist_tracks WHERE playlist_id = ? AND track_id = ?",
            (playlist_id, track_id),
        ).fetchone()
        deleted_position = int(row[0]) if row and row[0] is not None else None

        conn.execute(
            "DELETE FROM playlist_tracks WHERE playlist_id = ? AND track_id = ?",
            (playlist_id, track_id),
        )
        if deleted_position is not None:
            conn.execute(
                """
                UPDATE playlist_tracks
                SET position = position - 1
                WHERE playlist_id = ? AND position > ?
                """,
                (playlist_id, deleted_position),
            )
        conn.execute(
            "DELETE FROM track_downloads WHERE playlist_id = ? AND track_id = ?",
            (playlist_id, track_id),
        )
        conn.execute(
            "DELETE FROM track_download_events WHERE playlist_id = ? AND track_id = ?",
            (playlist_id, track_id),
        )

        remaining_refs = conn.execute(
            "SELECT COUNT(*) FROM playlist_tracks WHERE track_id = ?",
            (track_id,),
        ).fetchone()
        if int((remaining_refs[0] if remaining_refs else 0) or 0) == 0:
            conn.execute("DELETE FROM tracks WHERE id = ?", (track_id,))
        conn.commit()

    return {
        "ok": True,
        "deleted_file": bool(cleanup.get("deleted")),
        "deleted_path": str(cleanup.get("path", "") or ""),
        "track_id": track_id,
        "playlist_id": playlist_id,
    }


def reset_youtube_imports_for_playlist(playlist_id: str) -> dict[str, Any]:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT pt.track_id
            FROM playlist_tracks pt
            WHERE pt.playlist_id = ?
              AND pt.track_id LIKE 'yt_%'
            ORDER BY pt.position ASC
            """,
            (playlist_id,),
        ).fetchall()

    track_ids = [str(r[0] or "").strip() for r in rows if r and r[0]]
    reset_count = 0
    deleted_files = 0
    kept_manual_urls = 0
    failures: list[dict[str, str]] = []

    for track_id in track_ids:
        try:
            result = reset_track_for_redownload(playlist_id, track_id)
            reset_count += 1
            if result.get("deleted_file"):
                deleted_files += 1
            if result.get("preserved_manual_url"):
                kept_manual_urls += 1
        except Exception as exc:
            failures.append({"track_id": track_id, "error": str(exc)})

    return {
        "ok": True,
        "playlist_id": playlist_id,
        "total_yt_tracks": len(track_ids),
        "reset_count": reset_count,
        "deleted_files": deleted_files,
        "kept_manual_urls": kept_manual_urls,
        "failure_count": len(failures),
        "failures": failures[:100],
    }


def _allowed_media_roots() -> list[Path]:
    roots: list[Path] = []
    try:
        roots.append(DEFAULT_OUTPUT_DIR.resolve())
    except Exception:
        pass
    try:
        init_cache_db()
        with sqlite3.connect(CACHE_DB_PATH) as conn:
            rows = conn.execute(
                "SELECT DISTINCT output_dir FROM job_checkpoints WHERE output_dir IS NOT NULL AND output_dir != ''"
            ).fetchall()
        for (outdir,) in rows:
            try:
                p = Path(str(outdir)).expanduser().resolve()
                roots.append(p)
            except Exception:
                continue
    except Exception:
        pass
    deduped: list[Path] = []
    seen: set[str] = set()
    for r in roots:
        key = str(r)
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    return deduped


def _is_under_any_root(path: Path, roots: list[Path]) -> bool:
    for root in roots:
        if path == root:
            return True
        if root in path.parents:
            return True
    return False


@app.get("/api/review/next", response_model=ReviewNextResponse | None)
def review_next(playlist_id: str) -> ReviewNextResponse | None:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            f"""
            SELECT
                {_review_select_fields()}
            FROM track_downloads td
            LEFT JOIN playlist_tracks pt
                ON pt.playlist_id = td.playlist_id AND pt.track_id = td.track_id
            LEFT JOIN tracks t ON t.id = td.track_id
            WHERE td.playlist_id = ?
              AND td.status IN ('downloaded', 'skipped')
              AND (td.review_status IS NULL OR td.review_status = '')
            ORDER BY COALESCE(pt.position, 999999) ASC, COALESCE(td.updated_at, '') DESC
            LIMIT 1
            """,
            (playlist_id,),
        ).fetchone()
    if not row:
        return None
    return _review_row_to_response(row)


@app.get("/api/review/track", response_model=ReviewNextResponse | None)
def review_track(playlist_id: str, track_id: str) -> ReviewNextResponse | None:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            f"""
            SELECT
                {_review_select_fields()}
            FROM track_downloads td
            LEFT JOIN playlist_tracks pt
                ON pt.playlist_id = td.playlist_id AND pt.track_id = td.track_id
            LEFT JOIN tracks t ON t.id = td.track_id
            WHERE td.playlist_id = ? AND td.track_id = ?
            LIMIT 1
            """,
            (playlist_id, track_id),
        ).fetchone()
    if not row:
        return None
    return _review_row_to_response(row)


@app.get("/api/review/search")
def review_search(playlist_id: str, q: str) -> dict[str, Any]:
    query = (q or "").strip()
    if len(query) < 2:
        return {"playlist_id": playlist_id, "count": 0, "items": []}

    init_cache_db()
    like = f"%{query.lower()}%"
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        rows = conn.execute(
            f"""
            SELECT
                {_review_select_fields()}
            FROM track_downloads td
            LEFT JOIN playlist_tracks pt
                ON pt.playlist_id = td.playlist_id AND pt.track_id = td.track_id
            LEFT JOIN tracks t ON t.id = td.track_id
            WHERE td.playlist_id = ?
              AND (
                LOWER(COALESCE(td.artist_name, '')) LIKE ?
                OR LOWER(COALESCE(td.track_name, '')) LIKE ?
                OR LOWER(COALESCE(td.album_name, '')) LIKE ?
                OR LOWER(COALESCE(td.selected_title, '')) LIKE ?
              )
            ORDER BY COALESCE(pt.position, 999999) ASC, COALESCE(td.updated_at, '') DESC
            LIMIT 50
            """,
            (playlist_id, like, like, like, like),
        ).fetchall()
    items = [_review_row_to_response(row).model_dump() for row in rows]
    return {"playlist_id": playlist_id, "count": len(items), "items": items}


@app.post("/api/review/previous", response_model=ReviewNextResponse | None)
def review_previous(payload: ReviewActionRequest) -> ReviewNextResponse | None:
    init_cache_db()
    now = datetime.utcnow().isoformat()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        first_unreviewed = conn.execute(
            """
            SELECT COALESCE(pt.position, 999999)
            FROM track_downloads td
            LEFT JOIN playlist_tracks pt
                ON pt.playlist_id = td.playlist_id AND pt.track_id = td.track_id
            WHERE td.playlist_id = ?
              AND td.status IN ('downloaded', 'skipped')
              AND (td.review_status IS NULL OR td.review_status = '')
            ORDER BY COALESCE(pt.position, 999999) ASC, COALESCE(td.updated_at, '') DESC
            LIMIT 1
            """,
            (payload.playlist_id,),
        ).fetchone()

        if first_unreviewed is not None and first_unreviewed[0] is not None:
            candidate = conn.execute(
                f"""
                SELECT
                    {_review_select_fields()}
                FROM track_downloads td
                LEFT JOIN playlist_tracks pt
                    ON pt.playlist_id = td.playlist_id AND pt.track_id = td.track_id
                LEFT JOIN tracks t ON t.id = td.track_id
                WHERE td.playlist_id = ?
                  AND td.status IN ('downloaded', 'skipped')
                  AND COALESCE(td.review_status, '') != ''
                AND COALESCE(pt.position, 999999) < ?
                ORDER BY COALESCE(pt.position, 999999) DESC, COALESCE(td.updated_at, '') DESC
                LIMIT 1
                """,
                (payload.playlist_id, first_unreviewed[0]),
            ).fetchone()
        else:
            candidate = conn.execute(
                f"""
                SELECT
                    {_review_select_fields()}
                FROM track_downloads td
                LEFT JOIN playlist_tracks pt
                    ON pt.playlist_id = td.playlist_id AND pt.track_id = td.track_id
                LEFT JOIN tracks t ON t.id = td.track_id
                WHERE td.playlist_id = ?
                  AND td.status IN ('downloaded', 'skipped')
                  AND COALESCE(td.review_status, '') != ''
                ORDER BY COALESCE(pt.position, 999999) DESC, COALESCE(td.updated_at, '') DESC
                LIMIT 1
                """,
                (payload.playlist_id,),
            ).fetchone()

        if not candidate:
            return None

        conn.execute(
            """
            UPDATE track_downloads
            SET review_status = '',
                review_notes = '',
                review_updated_at = ?,
                updated_at = ?
            WHERE playlist_id = ? AND track_id = ?
            """,
            (now, now, payload.playlist_id, candidate[1]),
        )
        conn.commit()

    response = _review_row_to_response(candidate)
    response.review_status = ""
    response.review_updated_at = now
    return response


@app.post("/api/review/approve")
def review_approve(payload: ReviewActionRequest) -> dict[str, Any]:
    init_cache_db()
    now = datetime.utcnow().isoformat()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        cur = conn.execute(
            """
            UPDATE track_downloads
            SET review_status = 'approved',
                review_notes = ?,
                review_updated_at = ?,
                updated_at = ?
            WHERE playlist_id = ? AND track_id = ?
            """,
            (payload.notes.strip(), now, now, payload.playlist_id, payload.track_id),
        )
        conn.commit()
    return {"ok": True, "updated": int(cur.rowcount or 0)}


@app.post("/api/review/manual-source")
def review_manual_source(payload: ReviewManualSourceRequest) -> dict[str, Any]:
    url = payload.youtube_url.strip()
    if not re.match(r"^https?://(www\.)?(youtube\.com|youtu\.be)/", url, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Provide a valid YouTube URL")
    _review_upsert_manual_source(
        playlist_id=payload.playlist_id,
        track_id=payload.track_id,
        youtube_url=url,
        title=payload.title.strip(),
        notes=payload.notes.strip(),
    )
    return {"ok": True}


@app.get("/api/review/file")
def review_file(playlist_id: str, track_id: str) -> FileResponse:
    init_cache_db()
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT destination_path, artist_name, track_name, album_name
            FROM track_downloads
            WHERE playlist_id = ? AND track_id = ?
            """,
            (playlist_id, track_id),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Track not found in track_downloads")

    destination_path = str(row[0] or "").strip()
    artist_name = str(row[1] or "").strip()
    track_name = str(row[2] or "").strip()
    album_name = str(row[3] or "").strip()

    def _serve_path(path: Path) -> FileResponse:
        if path.suffix.lower() != ".mp3":
            raise HTTPException(
                status_code=400, detail="Only .mp3 files are supported for local playback"
            )
        if not path.exists() or not path.is_file():
            raise HTTPException(status_code=404, detail="Local file not found on disk")
        roots = _allowed_media_roots()
        if roots and not _is_under_any_root(path, roots):
            raise HTTPException(
                status_code=403,
                detail="File is outside allowed media roots. Run a job so the output_dir is recorded, or move the file under the downloads folder.",
            )
        return FileResponse(str(path), media_type="audio/mpeg", filename=path.name)

    if destination_path:
        raw_path = Path(destination_path).expanduser()
        try:
            resolved = raw_path.resolve()
        except Exception:
            resolved = raw_path
        if resolved.exists() and resolved.is_file():
            return _serve_path(resolved)

    roots = _allowed_media_roots()
    if not roots:
        raise HTTPException(status_code=404, detail="No media roots configured/found")

    # Fallback: older DB rows may not have destination_path populated. Try to locate the file.
    safe_artist = sanitize_filename(artist_name or "unknown")
    safe_track = sanitize_filename(track_name or "unknown")
    safe_album = sanitize_filename(album_name or "unknown")
    expected_name = f"{safe_artist} - {safe_track}.mp3"

    candidate_paths: list[Path] = []
    for root in roots:
        candidate_paths.append(root / safe_artist / safe_album / expected_name)

    found_path: Path | None = None
    for cand in candidate_paths:
        if cand.exists() and cand.is_file():
            found_path = cand
            break

    if found_path is None:
        # Last resort: recursive search by exact filename under allowed roots.
        for root in roots:
            try:
                for p in root.rglob(expected_name):
                    if p.is_file():
                        found_path = p
                        break
            except Exception:
                continue
            if found_path is not None:
                break

    if found_path is None:
        raise HTTPException(
            status_code=404,
            detail=f"Local file not found. destination_path missing/stale. expected_name={expected_name}",
        )

    # Backfill destination_path for future requests.
    try:
        with sqlite3.connect(CACHE_DB_PATH) as conn:
            conn.execute(
                """
                UPDATE track_downloads
                SET destination_path = ?, updated_at = ?
                WHERE playlist_id = ? AND track_id = ?
                """,
                (str(found_path), datetime.utcnow().isoformat(), playlist_id, track_id),
            )
            conn.commit()
    except Exception:
        pass

    return _serve_path(found_path)


@app.post("/api/download")
def start_download(payload: DownloadRequest) -> dict[str, str]:
    try:
        playlist_id = resolve_playlist_id(payload.playlist_id, payload.playlist)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not is_playlist_cached(playlist_id):
        raise HTTPException(
            status_code=400,
            detail=f"Playlist {playlist_id} is not cached in DB ({CACHE_DB_PATH}). Select an existing playlist or import a song into one first.",
        )

    output_path = Path(payload.output_dir).expanduser().resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    job = DownloadJob(
        id=str(uuid.uuid4()),
        playlist_input=playlist_id,
        quality=payload.quality,
        output_dir=str(output_path),
        mode=payload.mode,
    )

    with jobs_lock:
        jobs[job.id] = job

    if payload.mode == "scan_issues":
        target = run_scan_job
    elif payload.mode == "scan_missing":
        target = run_missing_scan_job
    else:
        target = run_download_job
    thread = threading.Thread(target=target, args=(job.id,), daemon=True)
    thread.start()

    return {"job_id": job.id}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job not found")

        return {
            "id": job.id,
            "mode": job.mode,
            "playlist": job.playlist_input,
            "playlist_name": job.playlist_name,
            "quality": job.quality,
            "output_dir": job.output_dir,
            "status": job.status,
            "total": job.total,
            "completed": job.completed,
            "failed": job.failed,
            "cached_tracks": job.cached_tracks,
            "suspicious_tracks": job.suspicious_tracks,
            "failed_details": job.failed_details,
            "extra_files": job.extra_files,
            "current_index": job.current_index,
            "current_track": job.current_track,
            "progress_pct": int((job.completed / job.total) * 100) if job.total else 0,
            "processed_pct": int(((job.completed + job.failed) / job.total) * 100)
            if job.total
            else 0,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "control_state": job.control_state,
            "pause_requested": job.pause_requested,
            "stop_requested": job.stop_requested,
            "auto_paused_until": (
                datetime.utcfromtimestamp(job.auto_paused_until).isoformat()
                if job.auto_paused_until > 0
                else None
            ),
            "auto_pause_reason": job.auto_pause_reason,
            "logs": job.logs,
        }


@app.get("/api/active-job")
def get_active_job() -> dict[str, Any]:
    with jobs_lock:
        active = [
            j
            for j in jobs.values()
            if j.status in {"queued", "running", "paused"} or j.control_state in {"stopping", "auto-paused"}
        ]
        if not active:
            return {"active": False}
        active.sort(key=lambda j: j.started_at, reverse=True)
        job = active[0]
        return {
            "active": True,
            "job_id": job.id,
            "status": job.status,
            "control_state": job.control_state,
        }


@app.post("/api/jobs/{job_id}/control")
def control_job(job_id: str, payload: JobControlRequest) -> dict[str, Any]:
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status in {"finished", "failed", "stopped"}:
            return {
                "id": job.id,
                "status": job.status,
                "control_state": job.control_state,
                "message": "job already ended",
            }
        if payload.action == "pause":
            job.pause_requested = True
            job.control_state = "paused"
            try:
                save_job_checkpoint(job, job.playlist_input)
            except Exception:
                pass
            return {
                "id": job.id,
                "status": job.status,
                "control_state": job.control_state,
                "message": "pause requested",
            }
        if payload.action == "resume":
            job.pause_requested = False
            job.auto_paused_until = 0.0
            job.auto_pause_reason = ""
            if not job.stop_requested:
                job.control_state = "running"
                if job.status == "paused":
                    job.status = "running"
            try:
                save_job_checkpoint(job, job.playlist_input)
            except Exception:
                pass
            return {
                "id": job.id,
                "status": job.status,
                "control_state": job.control_state,
                "message": "resume requested",
            }
        job.stop_requested = True
        job.pause_requested = False
        job.control_state = "stopping"
        try:
            save_job_checkpoint(job, job.playlist_input, stopped_by_user=True)
        except Exception:
            pass
        return {
            "id": job.id,
            "status": job.status,
            "control_state": job.control_state,
            "message": "stop requested",
        }


@app.get("/api/issues")
def get_issues(playlist_id: str | None = None, playlist: str | None = None) -> dict[str, Any]:
    try:
        resolved = resolve_playlist_id(playlist_id, playlist)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    rows = list_issue_rows(resolved)
    return {"playlist_id": resolved, "count": len(rows), "items": rows}


@app.post("/api/issues/resolve")
def resolve_issues(payload: IssueResolveRequest) -> dict[str, Any]:
    try:
        playlist_id = resolve_playlist_id(payload.playlist_id, payload.playlist)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    updated = resolve_suspicious_tracks(playlist_id, payload.track_ids)
    failed, suspicious = get_issue_counts(playlist_id)
    return {
        "playlist_id": playlist_id,
        "updated": updated,
        "remaining_failed": failed,
        "remaining_suspicious": suspicious,
    }


@app.post("/api/issues/manual-source")
def set_issue_manual_source(payload: ManualSourceRequest) -> dict[str, Any]:
    try:
        playlist_id = resolve_playlist_id(payload.playlist_id, payload.playlist)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    url = payload.youtube_url.strip()
    if not re.match(r"^https?://(www\.)?(youtube\.com|youtu\.be)/", url, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Provide a valid YouTube URL")

    try:
        set_manual_track_source(playlist_id, payload.track_id, url, payload.title.strip())
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to save manual source: {exc}") from exc

    failed, suspicious = get_issue_counts(playlist_id)
    return {
        "playlist_id": playlist_id,
        "track_id": payload.track_id,
        "manual_url": url,
        "remaining_failed": failed,
        "remaining_suspicious": suspicious,
    }


def get_playlist_snapshot_for_job(playlist_id: str) -> tuple[CachedPlaylist, list[CachedPlaylistItem]]:
    playlist, items = load_cached_playlist_items(playlist_id)
    if not items:
        raise RuntimeError(f"Playlist {playlist_id} is not cached in DB ({CACHE_DB_PATH}).")
    return playlist, items


def run_scan_job(job_id: str) -> None:
    with jobs_lock:
        job = jobs[job_id]
        job.status = "running"
        job.control_state = "running"

    try:
        ensure_yt_dlp_available()
        playlist_id = job.playlist_input
        playlist, items = get_playlist_snapshot_for_job(playlist_id)
        tracks = [item.track for item in items if getattr(item, "track", None)]
        force_retry_ids: set[str] = set()
        job.playlist_name = getattr(playlist, "name", "")
        job.total = len(tracks)
        job.log(f"Scan mode playlist: {job.playlist_name} ({job.total} tracks)")
        save_job_checkpoint(job, playlist_id)
        job.cached_tracks = len(tracks)
        job.log(f"Using cached playlist metadata from DB: {CACHE_DB_PATH}")

        ydl_base_opts = build_ydl_base_opts(job.quality)
        cookie_files = get_valid_cookie_files()
        if cookie_files:
            job.log(f"Using yt-dlp cookie files ({len(cookie_files)}): {', '.join(cookie_files)}")

        cached_match_candidates: dict[int, list[dict[str, Any]]] = {}
        if MATCH_CACHE_FIRST:
            for pos, track in enumerate(tracks, start=1):
                track_id = getattr(track, "id", "")
                if not track_id:
                    continue
                cached = get_cached_match_candidates(playlist_id, track_id)
                if cached:
                    cached_match_candidates[pos] = cached
            if cached_match_candidates:
                job.log(
                    f"Cache-first matching enabled: reusing DB candidates for {len(cached_match_candidates)}/{len(tracks)} tracks"
                )

        match_futures: dict[int, Future[list[dict[str, Any]]]] = {}
        with ThreadPoolExecutor(max_workers=MATCH_WORKERS) as match_pool:
            for submit_idx, submit_track in enumerate(tracks[:MATCH_PREFETCH], start=1):
                if submit_idx in cached_match_candidates:
                    continue
                match_futures[submit_idx] = match_pool.submit(
                    find_best_youtube_url, submit_track, ydl_base_opts
                )

            for idx, track in enumerate(tracks, start=1):
                if job.wait_if_paused_or_stopped() == "stop":
                    job.status = "stopped"
                    job.control_state = "stopped"
                    job.current_track = ""
                    job.finished_at = datetime.utcnow().isoformat()
                    job.log("Stopped by user")
                    save_job_checkpoint(job, playlist_id, stopped_by_user=True)
                    return
                next_idx = idx + MATCH_PREFETCH
                if next_idx <= len(tracks):
                    if next_idx in cached_match_candidates:
                        pass
                    else:
                        match_futures[next_idx] = match_pool.submit(
                            find_best_youtube_url, tracks[next_idx - 1], ydl_base_opts
                        )

                song = sanitize_filename(track.name)
                artist = sanitize_filename(track.artists[0].name)
                album = sanitize_filename(track.album.name)
                manual_override = is_manual_suspicious_override(
                    playlist_id, getattr(track, "id", "")
                )
                job.current_index = idx
                job.current_track = f"{artist} - {song}"
                destination_file = Path(job.output_dir) / artist / album / f"{artist} - {song}.mp3"

                if idx in cached_match_candidates:
                    candidates = cached_match_candidates[idx]
                    job.log(f"[{idx}/{job.total}] Using cached DB match candidates")
                else:
                    candidates = match_futures[idx].result()
                if not candidates:
                    job.failed += 1
                    job.add_failure(f"{artist} - {song}", "scan: no candidates found")
                    save_download_result(
                        playlist_id=playlist_id,
                        track=track,
                        status="failed",
                        suspicious=not manual_override,
                        job_id=job.id,
                        mode=job.mode,
                        matched_candidates=[],
                        destination_path=str(destination_file),
                        last_error="scan: no candidates found",
                        suspicious_reason="manual override" if manual_override else "scan: no candidates found",
                        suspicious_manual_override=manual_override,
                    )
                    log_download_event(
                        playlist_id=playlist_id,
                        track=track,
                        event_type="scan",
                        status="failed",
                        job_id=job.id,
                        mode=job.mode,
                        error="scan: no candidates found",
                    )
                    job.log(f"[{idx}/{job.total}] Scan failed: no candidates for {artist} - {song}")
                    save_job_checkpoint(job, playlist_id)
                    continue

                selected = candidates[0]
                selected_url = str(selected.get("url", ""))
                selected_title = str(selected.get("title", ""))
                selected_score = int(selected.get("score", 0))
                suspicious, reason = assess_suspicious_match(selected_title, selected_score)
                if manual_override and suspicious:
                    suspicious = False
                    reason = "manual override"
                if suspicious:
                    job.suspicious_tracks += 1
                    job.log(
                        f"[{idx}/{job.total}] Suspicious: {artist} - {song} -> {selected_title} ({reason})"
                    )

                save_download_result(
                    playlist_id=playlist_id,
                    track=track,
                    status="scanned",
                    suspicious=suspicious,
                    job_id=job.id,
                    mode=job.mode,
                    selected_url=selected_url,
                    selected_title=selected_title,
                    selected_score=selected_score,
                    selected_attempt=1,
                    selected_uploader=str(selected.get("uploader", "")),
                    selected_channel=str(selected.get("channel", "")),
                    selected_duration_seconds=(
                        float(selected.get("duration", 0) or 0)
                        if selected.get("duration") is not None
                        else None
                    ),
                    selected_view_count=(
                        int(selected.get("view_count", 0) or 0)
                        if selected.get("view_count") is not None
                        else None
                    ),
                    matched_candidates=candidates,
                    destination_path=str(destination_file),
                    last_error="",
                    suspicious_reason=reason,
                    suspicious_manual_override=manual_override,
                )
                log_download_event(
                    playlist_id=playlist_id,
                    track=track,
                    event_type="scan",
                    status="scanned",
                    job_id=job.id,
                    mode=job.mode,
                    selected_url=selected_url,
                    selected_title=selected_title,
                    selected_score=selected_score,
                    cookie_file=str(selected.get("search_cookie_file", "")),
                    payload={
                        "matched_candidates": candidates,
                        "suspicious": suspicious,
                        "reason": reason,
                    },
                )
                job.completed += 1
                save_job_checkpoint(job, playlist_id)

        failed_count, suspicious_count = get_issue_counts(playlist_id)
        job.suspicious_tracks = suspicious_count
        job.status = "finished"
        job.control_state = "finished"
        job.current_track = ""
        job.finished_at = datetime.utcnow().isoformat()
        save_job_checkpoint(job, playlist_id)
        job.log(
            f"Scan complete: scanned={job.completed}, failed={job.failed}, backlog failed={failed_count}, suspicious={suspicious_count}"
        )
    except Exception as exc:
        job.status = "failed"
        job.control_state = "failed"
        job.current_track = ""
        job.finished_at = datetime.utcnow().isoformat()
        job.log(f"Scan job failed: {exc}")
        try:
            save_job_checkpoint(job, job.playlist_input)
        except Exception:
            pass


def run_missing_scan_job(job_id: str) -> None:
    with jobs_lock:
        job = jobs[job_id]
        job.status = "running"
        job.control_state = "running"

    try:
        playlist_id = job.playlist_input
        playlist, items = get_playlist_snapshot_for_job(playlist_id)
        tracks = [item.track for item in items if getattr(item, "track", None)]
        job.playlist_name = getattr(playlist, "name", "") or playlist_id
        job.total = len(tracks)
        job.log(f"Missing-file scan playlist: {job.playlist_name} ({job.total} tracks)")
        job.cached_tracks = len(tracks)
        job.log(f"Using cached playlist metadata from DB: {CACHE_DB_PATH}")

        output_root = Path(job.output_dir).expanduser().resolve()
        if not output_root.exists():
            raise RuntimeError(f"Output directory does not exist: {output_root}")

        init_cache_db()
        destination_by_track_id: dict[str, str] = {}
        with sqlite3.connect(CACHE_DB_PATH) as conn:
            for track_id, destination_path in conn.execute(
                "SELECT track_id, destination_path FROM track_downloads WHERE playlist_id = ?",
                (playlist_id,),
            ).fetchall():
                if track_id:
                    destination_by_track_id[str(track_id)] = str(destination_path or "")

        files_by_norm: dict[str, list[str]] = {}
        all_files: list[Path] = []
        total_files = 0
        for path in output_root.rglob("*.mp3"):
            if not path.is_file():
                continue
            all_files.append(path)
            total_files += 1
            key = _norm(path.stem)
            if not key:
                continue
            files_by_norm.setdefault(key, []).append(str(path))

        job.log(f"Scanned downloads folder: {output_root} (mp3 files={total_files})")

        missing = 0
        found = 0
        expected_norms: set[str] = set()
        used_files: set[str] = set()

        for idx, track in enumerate(tracks, start=1):
            if job.wait_if_paused_or_stopped() == "stop":
                job.status = "stopped"
                job.control_state = "stopped"
                job.current_track = ""
                job.finished_at = datetime.utcnow().isoformat()
                job.log("Stopped by user")
                save_job_checkpoint(job, playlist_id, stopped_by_user=True)
                return

            track_id = getattr(track, "id", "") or ""
            song = sanitize_filename(getattr(track, "name", "") or "unknown")
            artist_name = "unknown"
            try:
                artists = getattr(track, "artists", None) or []
                if artists:
                    artist_name = sanitize_filename(getattr(artists[0], "name", "") or "unknown")
            except Exception:
                pass
            album_name = sanitize_filename(getattr(getattr(track, "album", None), "name", "") or "unknown")
            expected_name = f"{artist_name} - {song}.mp3"
            expected_path = output_root / artist_name / album_name / expected_name
            expected_norms.add(_norm(f"{artist_name} - {song}"))
            job.current_index = idx
            job.current_track = f"{artist_name} - {song}"

            dest = destination_by_track_id.get(track_id, "").strip()
            if dest and Path(dest).exists():
                found += 1
                try:
                    dest_path = Path(dest).resolve()
                    if output_root in dest_path.parents:
                        used_files.add(str(dest_path))
                except Exception:
                    pass
                continue
            if expected_path.exists():
                found += 1
                used_files.add(str(expected_path.resolve()))
                continue
            norm_key = _norm(f"{artist_name} - {song}")
            if norm_key and norm_key in files_by_norm:
                found += 1
                # If multiple candidates exist for a normalized name, treat duplicates as extras.
                match_paths = files_by_norm.get(norm_key) or []
                if match_paths:
                    used_files.add(str(Path(match_paths[0]).resolve()))
                continue

            missing += 1
            job.add_failure(f"{artist_name} - {song}", f"missing file (expected: {expected_path})")
            try:
                save_download_result(
                    playlist_id=playlist_id,
                    track=track,
                    status="failed",
                    suspicious=False,
                    destination_path=str(expected_path),
                    last_error="missing on disk",
                    suspicious_reason="missing on disk",
                )
            except Exception:
                pass

        extra_paths: list[str] = []
        for path in all_files:
            resolved = ""
            try:
                resolved = str(path.resolve())
            except Exception:
                resolved = str(path)
            if resolved in used_files:
                continue
            extra_paths.append(resolved)
        extra_paths.sort()
        job.extra_files = extra_paths[:500]
        extras = len(extra_paths)

        job.completed = found
        job.failed = missing
        job.status = "finished"
        job.control_state = "finished"
        job.current_track = ""
        job.finished_at = datetime.utcnow().isoformat()
        save_job_checkpoint(job, playlist_id)
        job.log(
            f"Missing-file scan complete: found={found}, missing={missing}, extra_files={extras}. Missing tracks were recorded as issues (status=failed) for Retry mode."
        )
        if extras:
            preview = job.extra_files[:25]
            job.log("Extra files (preview):")
            for row in preview:
                job.log(f"  - {row}")
            if extras > len(preview):
                job.log(f"  ... and {extras - len(preview)} more")
    except Exception as exc:
        with jobs_lock:
            job = jobs[job_id]
        job.status = "failed"
        job.control_state = "failed"
        job.current_track = ""
        job.finished_at = datetime.utcnow().isoformat()
        job.log(f"Missing-file scan failed: {exc}")
        try:
            save_job_checkpoint(job, job.playlist_input)
        except Exception:
            pass


def run_repair_imports_job(job_id: str) -> None:
    with jobs_lock:
        job = jobs[job_id]
        job.status = "running"
        job.control_state = "running"

    try:
        playlist_id = job.playlist_input
        playlist, items = get_playlist_snapshot_for_job(playlist_id)
        tracks = [
            item.track
            for item in items
            if getattr(item, "track", None) and str(getattr(item.track, "id", "")).startswith("yt_")
        ]
        job.playlist_name = getattr(playlist, "name", "") or playlist_id
        job.total = len(tracks)
        job.cached_tracks = len(tracks)
        job.log(f"Repair imports playlist: {job.playlist_name} ({job.total} imported tracks)")

        output_root = Path(job.output_dir).expanduser().resolve()
        output_root.mkdir(parents=True, exist_ok=True)
        save_job_checkpoint(job, playlist_id)

        for idx, track in enumerate(tracks, start=1):
            if job.wait_if_paused_or_stopped() == "stop":
                job.status = "stopped"
                job.control_state = "stopped"
                job.current_track = ""
                job.finished_at = datetime.utcnow().isoformat()
                job.log("Stopped by user")
                save_job_checkpoint(job, playlist_id, stopped_by_user=True)
                return

            track_id = str(getattr(track, "id", "") or "")
            selected_url = ""
            selected_title = ""
            existing_destination = ""
            with sqlite3.connect(CACHE_DB_PATH) as conn:
                row = conn.execute(
                    "SELECT selected_url, selected_title, destination_path FROM track_downloads WHERE playlist_id = ? AND track_id = ?",
                    (playlist_id, track_id),
                ).fetchone()
                selected_url = str((row[0] if row else "") or "")
                selected_title = str((row[1] if row else "") or "")
                existing_destination = str((row[2] if row else "") or "")

            current_meta = resolve_youtube_import_metadata(
                title=selected_title or track.name,
                artist_name=(track.artists[0].name if getattr(track, "artists", None) else ""),
                track_name=track.name,
                album_name=(track.album.name if getattr(track, "album", None) else ""),
                fallback_uploader=(track.artists[0].name if getattr(track, "artists", None) else ""),
                source_url=selected_url,
            )
            track.name = current_meta.get("track_name", "") or track.name
            if getattr(track, "artists", None):
                track.artists[0].name = current_meta.get("artist_name", "") or track.artists[0].name
            if getattr(track, "album", None):
                track.album.name = current_meta.get("album_name", "") or track.album.name
                if getattr(track.album, "artists", None):
                    track.album.artists[0].name = current_meta.get("artist_name", "") or track.album.artists[0].name

            song = sanitize_filename(track.name)
            artist = sanitize_filename(track.artists[0].name)
            album = sanitize_filename(track.album.name or "unknown")
            job.current_index = idx
            job.current_track = f"{artist} - {song}"
            existing_file = locate_track_file_for_repair(playlist_id, track, output_root)
            if existing_file is None:
                base_root = output_root
                destination_dir = base_root / artist / album
                destination_dir.mkdir(parents=True, exist_ok=True)
                destination_file = destination_dir / f"{artist} - {song}.mp3"
                job.failed += 1
                job.add_failure(f"{artist} - {song}", "repair could not find local file; flagged for redownload")
                save_download_result(
                    playlist_id=playlist_id,
                    track=track,
                    status="failed",
                    suspicious=False,
                    last_error="repair could not find local file",
                    suspicious_reason="repair requested redownload",
                    destination_path=str(destination_file),
                )
                save_job_checkpoint(job, playlist_id)
                continue

            base_root = infer_library_root_from_file(existing_file)
            destination_dir = base_root / artist / album
            destination_dir.mkdir(parents=True, exist_ok=True)
            destination_file = destination_dir / f"{artist} - {song}.mp3"

            old_parent = existing_file.parent
            try:
                rewrite_audio_tags(
                    existing_file,
                    track,
                    video_url=selected_url,
                    prefer_non_youtube_art=True,
                )
            except Exception as exc:
                job.log(f"[{idx}/{job.total}] Retag warning: {exc}")

            moved_path = existing_file
            try:
                if existing_file.resolve() != destination_file.resolve():
                    final_destination = unique_destination_path(destination_file)
                    moved_path = existing_file.replace(final_destination)
                    remove_empty_parents(old_parent, base_root)
            except Exception as exc:
                job.failed += 1
                job.add_failure(f"{artist} - {song}", f"move failed; flagged for redownload ({exc})")
                save_download_result(
                    playlist_id=playlist_id,
                    track=track,
                    status="failed",
                    suspicious=False,
                    last_error=f"repair move failed: {exc}",
                    suspicious_reason="repair requested redownload",
                    destination_path=str(destination_file),
                )
                save_job_checkpoint(job, playlist_id)
                continue

            post_probe = probe_audio_file(moved_path)
            save_download_result(
                playlist_id=playlist_id,
                track=track,
                status="downloaded",
                suspicious=False,
                destination_path=str(moved_path),
                selected_url=selected_url,
                selected_title=track.name,
                file_size_bytes=post_probe.get("file_size_bytes"),
                final_bitrate_kbps=post_probe.get("bitrate_kbps"),
                audio_duration_seconds=post_probe.get("duration_seconds"),
                audio_sample_rate_hz=post_probe.get("sample_rate_hz"),
                audio_channels=post_probe.get("channels"),
                last_error="",
                suspicious_reason="",
            )
            job.completed += 1
            job.log(f"[{idx}/{job.total}] Repaired: {moved_path}")
            save_job_checkpoint(job, playlist_id)

        job.status = "finished"
        job.control_state = "finished"
        job.current_track = ""
        job.finished_at = datetime.utcnow().isoformat()
        save_job_checkpoint(job, playlist_id)
        job.log(f"Repair imports complete: repaired={job.completed}, flagged_for_retry={job.failed}")
    except Exception as exc:
        with jobs_lock:
            job = jobs[job_id]
        job.status = "failed"
        job.control_state = "failed"
        job.current_track = ""
        job.finished_at = datetime.utcnow().isoformat()
        job.log(f"Repair imports failed: {exc}")
        try:
            save_job_checkpoint(job, job.playlist_input)
        except Exception:
            pass


def run_download_job(job_id: str) -> None:
    with jobs_lock:
        job = jobs[job_id]
        job.status = "running"
        job.control_state = "running"

    staging_dir: Path | None = None

    try:
        ensure_yt_dlp_available()
        playlist_id = job.playlist_input
        playlist, items = get_playlist_snapshot_for_job(playlist_id)
        tracks = [item.track for item in items if getattr(item, "track", None)]
        force_retry_ids: set[str] = set()
        job.playlist_name = getattr(playlist, "name", "")
        job.total = len(tracks)
        job.log(f"Playlist: {job.playlist_name} ({job.total} tracks)")
        job.cached_tracks = len(tracks)
        job.log(f"Using cached playlist metadata from DB: {CACHE_DB_PATH}")

        if job.mode == "retry_issues":
            issue_ids = load_issue_track_ids(playlist_id)
            if not issue_ids:
                job.log("No failed/suspicious tracks found for retry.")
                job.status = "finished"
                job.control_state = "finished"
                job.finished_at = datetime.utcnow().isoformat()
                save_job_checkpoint(job, playlist_id)
                return
            tracks = [t for t in tracks if getattr(t, "id", "") in issue_ids]
            job.total = len(tracks)
            job.log(f"Retry mode: processing {job.total} previously failed/suspicious tracks")
        elif RESUME_FROM_DB and job.mode == "download":
            done_ids = get_resume_completed_track_ids(playlist_id)
            force_retry_ids = load_issue_track_ids(playlist_id)
            if done_ids:
                before = len(tracks)
                tracks = [t for t in tracks if getattr(t, "id", "") not in done_ids]
                skipped = before - len(tracks)
                if skipped > 0:
                    job.log(f"Resume-from-DB enabled: skipping {skipped} already completed tracks")
            if force_retry_ids:
                job.log(
                    f"Resume-from-DB will retry {len(force_retry_ids)} failed/suspicious tracks when encountered"
                )
            job.total = len(tracks)

        save_job_checkpoint(job, playlist_id)

        ydl_base_opts = build_ydl_base_opts(job.quality)
        cookie_files = get_valid_cookie_files()
        if cookie_files:
            job.log(f"Using yt-dlp cookie files ({len(cookie_files)}): {', '.join(cookie_files)}")
        else:
            configured = get_configured_cookie_files()
            if configured:
                job.log("Configured cookie files are missing or invalid Netscape format")

        output_root = Path(job.output_dir)
        output_root.mkdir(parents=True, exist_ok=True)
        staging_dir = output_root / ".tmp" / job.id
        staging_dir.mkdir(parents=True, exist_ok=True)

        job.log(
            f"Matcher config: workers={MATCH_WORKERS}, prefetch={MATCH_PREFETCH}, candidates={MATCH_CANDIDATES}"
        )
        match_futures: dict[int, Future[list[dict[str, Any]]]] = {}
        artist_genre_cache: dict[str, str] = {}
        cached_match_candidates: dict[int, list[dict[str, Any]]] = {}
        if MATCH_CACHE_FIRST:
            for pos, track in enumerate(tracks, start=1):
                track_id = getattr(track, "id", "")
                if not track_id:
                    continue
                cached = get_cached_match_candidates(playlist_id, track_id)
                if cached:
                    cached_match_candidates[pos] = cached
            if cached_match_candidates:
                job.log(
                    f"Cache-first matching enabled: reusing DB candidates for {len(cached_match_candidates)}/{len(tracks)} tracks"
                )

        with ThreadPoolExecutor(max_workers=MATCH_WORKERS) as match_pool:
            for submit_idx, submit_track in enumerate(tracks[:MATCH_PREFETCH], start=1):
                if submit_idx in cached_match_candidates:
                    continue
                match_futures[submit_idx] = match_pool.submit(
                    find_best_youtube_url, submit_track, ydl_base_opts
                )

            for idx, track in enumerate(tracks, start=1):
                if job.wait_if_paused_or_stopped() == "stop":
                    job.status = "stopped"
                    job.control_state = "stopped"
                    job.current_track = ""
                    job.finished_at = datetime.utcnow().isoformat()
                    job.log("Stopped by user")
                    save_job_checkpoint(job, playlist_id, stopped_by_user=True)
                    return
                next_idx = idx + MATCH_PREFETCH
                if next_idx <= len(tracks):
                    if next_idx in cached_match_candidates:
                        pass
                    else:
                        match_futures[next_idx] = match_pool.submit(
                            find_best_youtube_url, tracks[next_idx - 1], ydl_base_opts
                        )

                song = sanitize_filename(track.name)
                artist = sanitize_filename(track.artists[0].name)
                album = sanitize_filename(track.album.name)
                job.current_index = idx
                job.current_track = f"{artist} - {song}"
                file_name = f"{artist} - {song}.mp3"
                destination_dir = output_root / artist / album
                destination_dir.mkdir(parents=True, exist_ok=True)
                destination_file = destination_dir / file_name
                manual_override = is_manual_suspicious_override(
                    playlist_id, getattr(track, "id", "")
                )

                track_id = getattr(track, "id", "")
                should_force_retry = track_id in force_retry_ids
                if destination_file.exists() and job.mode != "retry_issues" and not should_force_retry:
                    job.log(f"[{idx}/{job.total}] Skipping existing: {file_name}")
                    job.completed += 1
                    save_download_result(
                        playlist_id=playlist_id,
                        track=track,
                        status="skipped",
                        suspicious=False,
                        job_id=job.id,
                        mode=job.mode,
                        destination_path=str(destination_file),
                    )
                    log_download_event(
                        playlist_id=playlist_id,
                        track=track,
                        event_type="download",
                        status="skipped",
                        job_id=job.id,
                        mode=job.mode,
                        payload={"destination_path": str(destination_file)},
                    )
                    save_job_checkpoint(job, playlist_id)
                    continue
                if destination_file.exists() and should_force_retry:
                    job.log(
                        f"[{idx}/{job.total}] Existing file present but retrying due to prior failed/suspicious status: {file_name}"
                    )

                job.log(f"[{idx}/{job.total}] Resolving YouTube source: {artist} - {song}")
                if idx in cached_match_candidates:
                    match_candidates = cached_match_candidates[idx]
                    job.log(f"[{idx}/{job.total}] Using cached DB match candidates")
                else:
                    match_candidates = match_futures[idx].result()
                if not match_candidates:
                    job.log(f"[{idx}/{job.total}] No candidate found: {file_name}")
                    job.failed += 1
                    job.add_failure(f"{artist} - {song}", "no candidates found")
                    save_download_result(
                        playlist_id=playlist_id,
                        track=track,
                        status="failed",
                        suspicious=not manual_override,
                        job_id=job.id,
                        mode=job.mode,
                        matched_candidates=[],
                        destination_path=str(destination_file),
                        last_error="no candidates found",
                        suspicious_reason="manual override" if manual_override else "no candidates found",
                        suspicious_manual_override=manual_override,
                    )
                    log_download_event(
                        playlist_id=playlist_id,
                        track=track,
                        event_type="download",
                        status="failed",
                        job_id=job.id,
                        mode=job.mode,
                        error="no candidates found",
                    )
                    save_job_checkpoint(job, playlist_id)
                    continue

                previous_record = None
                previous_title_norm = ""
                previous_url = ""
                imported_track = str(getattr(track, "id", "") or "").startswith("yt_")
                if job.mode == "retry_issues":
                    previous_record = get_track_download_record(
                        playlist_id, getattr(track, "id", "")
                    )
                    previous_title_norm = _norm(
                        (previous_record or {}).get("selected_title", "")
                    )
                    previous_url = str((previous_record or {}).get("selected_url", "")).strip()

                downloaded = False
                last_error = ""
                for attempt, candidate in enumerate(match_candidates, start=1):
                    if job.wait_if_paused_or_stopped() == "stop":
                        job.status = "stopped"
                        job.control_state = "stopped"
                        job.current_track = ""
                        job.finished_at = datetime.utcnow().isoformat()
                        job.log("Stopped by user")
                        save_job_checkpoint(job, playlist_id, stopped_by_user=True)
                        return
                    video_url = str(candidate.get("url", ""))
                    matched_title = str(candidate.get("title", ""))
                    matched_score = int(candidate.get("score", 0))
                    if job.mode == "retry_issues":
                        manual_candidate = (
                            str(candidate.get("search_query", "")) == "manual_source"
                            or str(candidate.get("uploader", "")).lower() == "manual"
                            or str(candidate.get("channel", "")).lower() == "manual"
                        )
                        title_norm = _norm(matched_title)
                        suspicious_candidate, suspicious_reason = assess_suspicious_match(
                            matched_title, matched_score
                        )
                        if suspicious_candidate:
                            job.log(
                                f"[{idx}/{job.total}] Skip candidate (still suspicious): {matched_title} ({suspicious_reason})"
                            )
                            continue
                        if (
                            previous_url
                            and video_url
                            and previous_url == video_url
                            and not manual_candidate
                            and not imported_track
                        ):
                            job.log(
                                f"[{idx}/{job.total}] Skip candidate (same URL as previous): {matched_title}"
                            )
                            continue
                        if (
                            not previous_url
                            and previous_title_norm
                            and title_norm == previous_title_norm
                            and not manual_candidate
                            and not imported_track
                        ):
                            job.log(
                                f"[{idx}/{job.total}] Skip candidate (same as previous): {matched_title}"
                            )
                            continue

                    job.log(
                        f"[{idx}/{job.total}] Try {attempt}/{len(match_candidates)}: {matched_title} (score={matched_score})"
                    )

                    temp_template = str(staging_dir / f"{artist} - {song}.%(ext)s")

                    try:
                        attempt_started_at = datetime.utcnow().isoformat()
                        wall_started = time.time()
                        dl_result = download_candidate_with_fallback(
                            ydl_base_opts, temp_template, video_url
                        )
                        if not dl_result.get("ok"):
                            raise RuntimeError(str(dl_result.get("error", "download failed")))

                        temp_file = staging_dir / file_name
                        if not temp_file.exists():
                            raise RuntimeError("download output missing")
                        pre_probe = probe_audio_file(temp_file)
                        original_bitrate_kbps = pre_probe.get("bitrate_kbps")

                        tag_meta = enrich_cached_track_metadata_if_needed(track)
                        tag_artist = tag_meta.get("artist_name", "") or track.artists[0].name
                        tag_title = tag_meta.get("track_name", "") or track.name
                        tag_album = tag_meta.get("album_name", "") or track.album.name

                        audiofile = eyed3.load(str(temp_file)) if eyed3 is not None else None
                        if audiofile is not None:
                            if audiofile.tag is None:
                                audiofile.initTag()
                            audiofile.tag.artist = tag_artist
                            audiofile.tag.title = tag_title
                            audiofile.tag.album = tag_album
                            if track.album.artists:
                                audiofile.tag.album_artist = track.album.artists[0].name
                            elif tag_artist:
                                audiofile.tag.album_artist = tag_artist
                            if getattr(track, "track_number", None):
                                audiofile.tag.track_num = track.track_number

                            try:
                                artist_obj = track.artists[0]
                                artist_key = getattr(artist_obj, "id", "") or getattr(
                                    artist_obj, "name", ""
                                )
                                if artist_key not in artist_genre_cache:
                                    artist_genre_cache[artist_key] = get_artist_genre(
                                        spotify, artist_obj
                                    )
                                genre = artist_genre_cache.get(artist_key, "")
                                if genre:
                                    audiofile.tag.genre = genre
                            except Exception:
                                pass

                            try:
                                imported_track = str(getattr(track, "id", "") or "").startswith("yt_")
                                art = get_cover_art_bytes(
                                    track,
                                    video_url=video_url,
                                    allow_youtube_fallback=not imported_track,
                                )
                                if art:
                                    image_data, mime = art
                                    audiofile.tag.images.set(3, image_data, mime)
                            except Exception as exc:
                                job.log(f"[{idx}/{job.total}] Album art skipped: {exc}")

                            audiofile.tag.save()

                        normalization_applied = False
                        try:
                            normalize_audio_file(temp_file, job.quality)
                            normalization_applied = NORMALIZE_AUDIO
                        except Exception as exc:
                            job.log(f"[{idx}/{job.total}] Normalization skipped: {exc}")

                        post_probe = probe_audio_file(temp_file)
                        temp_file.replace(destination_file)
                        suspicious, reason = assess_suspicious_match(
                            matched_title, matched_score
                        )
                        if manual_override and suspicious:
                            suspicious = False
                            reason = "manual override"
                        if suspicious:
                            job.suspicious_tracks += 1
                            job.log(
                                f"[{idx}/{job.total}] Flagged suspicious match: {reason}"
                            )
                        job.completed += 1
                        job.log(f"[{idx}/{job.total}] Downloaded: {destination_file}")
                        attempt_finished_at = datetime.utcnow().isoformat()
                        elapsed_ms = int((time.time() - wall_started) * 1000)
                        selected_cookie = str(dl_result.get("cookie_file", ""))
                        selected_format = str(dl_result.get("format", ""))
                        save_download_result(
                            playlist_id=playlist_id,
                            track=track,
                            status="downloaded",
                            suspicious=suspicious,
                            job_id=job.id,
                            mode=job.mode,
                            selected_url=video_url,
                            selected_title=matched_title,
                            selected_score=matched_score,
                            selected_attempt=attempt,
                            selected_uploader=str(candidate.get("uploader", "")),
                            selected_channel=str(candidate.get("channel", "")),
                            selected_duration_seconds=(
                                float(candidate.get("duration", 0) or 0)
                                if candidate.get("duration") is not None
                                else None
                            ),
                            selected_view_count=(
                                int(candidate.get("view_count", 0) or 0)
                                if candidate.get("view_count") is not None
                                else None
                            ),
                            selected_cookie_file=selected_cookie,
                            selected_format=selected_format,
                            download_started_at=attempt_started_at,
                            download_finished_at=attempt_finished_at,
                            download_elapsed_ms=elapsed_ms,
                            file_size_bytes=post_probe.get("file_size_bytes"),
                            original_bitrate_kbps=original_bitrate_kbps,
                            final_bitrate_kbps=post_probe.get("bitrate_kbps"),
                            audio_duration_seconds=post_probe.get("duration_seconds"),
                            audio_sample_rate_hz=post_probe.get("sample_rate_hz"),
                            audio_channels=post_probe.get("channels"),
                            normalization_applied=normalization_applied,
                            attempt_history=(
                                dl_result.get("attempts", [])
                                if isinstance(dl_result.get("attempts", []), list)
                                else []
                            ),
                            extra_metadata={
                                "search_query": candidate.get("search_query", ""),
                                "search_cookie_file": candidate.get("search_cookie_file", ""),
                                "pre_probe": pre_probe,
                                "post_probe": post_probe,
                            },
                            matched_candidates=match_candidates,
                            destination_path=str(destination_file),
                            last_error="",
                            suspicious_reason=reason,
                            suspicious_manual_override=manual_override,
                        )
                        log_download_event(
                            playlist_id=playlist_id,
                            track=track,
                            event_type="download_attempt",
                            status="downloaded",
                            job_id=job.id,
                            mode=job.mode,
                            selected_url=video_url,
                            selected_title=matched_title,
                            selected_score=matched_score,
                            cookie_file=selected_cookie,
                            format_used=selected_format,
                            payload={
                                "attempt": attempt,
                                "attempts_total": len(match_candidates),
                                "download_attempts": dl_result.get("attempts", []),
                                "pre_probe": pre_probe,
                                "post_probe": post_probe,
                                "normalization_applied": normalization_applied,
                                "search_query": candidate.get("search_query", ""),
                                "search_cookie_file": candidate.get("search_cookie_file", ""),
                                "elapsed_ms": elapsed_ms,
                                "destination_path": str(destination_file),
                            },
                        )
                        downloaded = True
                        save_job_checkpoint(job, playlist_id)
                        if YTDLP_DOWNLOAD_SLEEP_SECONDS > 0:
                            time.sleep(YTDLP_DOWNLOAD_SLEEP_SECONDS)
                        break
                    except Exception as exc:
                        last_error = str(exc)
                        if is_youtube_rate_limited_error(last_error):
                            job.trigger_auto_pause(
                                AUTO_RATE_LIMIT_PAUSE_SECONDS, last_error[:240]
                            )
                            save_job_checkpoint(job, playlist_id)
                        job.log(
                            f"[{idx}/{job.total}] Candidate failed: {matched_title} ({exc})"
                        )
                        log_download_event(
                            playlist_id=playlist_id,
                            track=track,
                            event_type="download_attempt",
                            status="failed",
                            job_id=job.id,
                            mode=job.mode,
                            selected_url=video_url,
                            selected_title=matched_title,
                            selected_score=matched_score,
                            error=last_error,
                            payload={
                                "attempt": attempt,
                                "attempts_total": len(match_candidates),
                                "candidate": candidate,
                            },
                        )
                        save_job_checkpoint(job, playlist_id)

                if not downloaded:
                    job.failed += 1
                    job.add_failure(
                        f"{artist} - {song}",
                        last_error or "all candidates failed or were skipped",
                    )
                    job.log(f"[{idx}/{job.total}] Failed all candidates: {file_name}")
                    save_download_result(
                        playlist_id=playlist_id,
                        track=track,
                        status="failed",
                        suspicious=not manual_override,
                        job_id=job.id,
                        mode=job.mode,
                        matched_candidates=match_candidates,
                        destination_path=str(destination_file),
                        last_error=last_error or "all candidates failed",
                        suspicious_reason="manual override"
                        if manual_override
                        else (last_error or "all candidates failed"),
                        suspicious_manual_override=manual_override,
                    )
                    log_download_event(
                        playlist_id=playlist_id,
                        track=track,
                        event_type="download",
                        status="failed",
                        job_id=job.id,
                        mode=job.mode,
                        error=last_error or "all candidates failed",
                        payload={"matched_candidates": match_candidates},
                    )
                    save_job_checkpoint(job, playlist_id)

        job.status = "finished"
        job.control_state = "finished"
        job.current_track = ""
        job.finished_at = datetime.utcnow().isoformat()
        save_job_checkpoint(job, playlist_id)
        failed_count, suspicious_count = get_issue_counts(playlist_id)
        job.suspicious_tracks = suspicious_count
        job.log(
            f"Issue backlog: failed={failed_count}, suspicious={suspicious_count} (mode={job.mode})"
        )
        job.log(f"Done. success={job.completed} failed={job.failed} total={job.total}")
    except Exception as exc:
        job.status = "failed"
        job.control_state = "failed"
        job.current_track = ""
        job.finished_at = datetime.utcnow().isoformat()
        job.log(f"Job failed: {exc}")
        try:
            save_job_checkpoint(job, job.playlist_input)
        except Exception:
            pass
    finally:
        if staging_dir and staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
