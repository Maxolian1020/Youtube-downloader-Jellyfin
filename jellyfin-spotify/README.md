# Playlist Downloader UI

Web UI for DB-backed playlists to MP3 downloading.

## What it does
- Runs a local web UI on port `8000`
- Lets you create/select playlists stored in a local SQLite DB
- Downloads MP3 (`190` or `320`) with folder structure: `Artist/Album/`
- Adds ID3 tags + album art (when `eyed3` is installed)
- Improves YouTube matching to prefer audio/lyrics/topic uploads over cinematic official MVs
- Stores playlist/track metadata in SQLite for later re-download workflows

## Setup

1. Create and activate a virtual environment (if needed):
   ```bash
   python3 -m venv .venv
   . .venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create local env file:
   ```bash
   cp .env.example .env
   ```
4. Edit `.env`:
   - optional matcher tuning:
     - `MATCH_WORKERS=3` (parallel match threads, 1-8)
     - `MATCH_PREFETCH=10` (how many upcoming tracks to pre-match, 2-30)
     - `MATCH_CANDIDATES=6` (download fallback attempts per track, 1-15)
     - `MATCH_CACHE_FIRST=true` (reuse cached DB match candidates before querying YouTube)
     - `RESUME_FROM_DB=true` (skip completed tracks and retry failed/suspicious tracks when restarting download mode)
   - optional yt-dlp auth, throttling, and audio processing:
     - `YTDLP_COOKIES_FILE=/path/to/cookies.txt`
     - `YTDLP_COOKIE_FILES=/path/to/cookies_a.txt,/path/to/cookies_b.txt`
     - `YTDLP_SEARCH_SLEEP_SECONDS=0.35`
     - `YTDLP_DOWNLOAD_SLEEP_SECONDS=0.7`
     - `YTDLP_ATTEMPT_SLEEP_SECONDS=0.2`
     - `AUTO_RATE_LIMIT_PAUSE_SECONDS=3600` (auto-pause duration when YouTube rate limiting is detected)
     - `AUTO_PAUSE_CHECK_SECONDS=15` (how often paused jobs check whether to resume)
     - `NORMALIZE_AUDIO=true`
     - `LOUDNORM_FILTER=loudnorm=I=-14:TP=-1.5:LRA=11`
   - optional metadata cache db location:
     - `SPOTIFY_CACHE_DB=/path/to/spotify_cache.db`
   - optional metadata enrichment:
     - `ENABLE_MUSICBRAINZ=true` (cover art + genre tags without Spotify; uses MusicBrainz + Cover Art Archive)
     - `MUSICBRAINZ_USER_AGENT=...` (required by MusicBrainz; set something identifying)
5. Ensure `ffmpeg` is installed and on your PATH.
6. Run:
   ```bash
   ./start.sh
   ```

Open `http://127.0.0.1:8000`.

Create a playlist in the UI (or select an existing cached one) and add tracks (Artist + Title, optional Album).
To replace Spotify playlists, you can import:
- A YouTube (or YouTube Music) playlist URL (creates a cached playlist and seeds per-track direct video URLs)
- A single YouTube video URL (adds it to the currently selected playlist)

Use `http://127.0.0.1:8000/review` for one-track-at-a-time download review (approve or paste a replacement URL). Review state is persisted in SQLite.

### Mode Selection
- `Download`: runs matching+download.
- `Scan + Flag Suspicious`: runs matching only, writes candidate/source confidence into DB, no download.
- `Scan Missing Files (DB vs Downloads)`: recursively scans the output directory, lists extra files, and marks missing tracks as issues so `Retry Failed/Suspicious` can download them again.
- `Retry Failed/Suspicious Only`: only reprocesses tracks previously marked `failed` or `suspicious`.

### Job Controls
- Use **Pause**, **Resume**, and **Stop** buttons in the Status card during active runs.
- Stopped jobs are checkpointed in DB and can be resumed by starting the same playlist again with `RESUME_FROM_DB=true`.
- UI auto-reattaches to active jobs after page reload and from other devices via backend active-job discovery.
- If YouTube rate-limits the session, the job auto-pauses for `AUTO_RATE_LIMIT_PAUSE_SECONDS` and then resumes automatically.

### Issue Review UI
- Use **Load Issues** to view failed/suspicious tracks from DB.
- Uncheck false positives and click **Clear Selected Suspicious** before running retry mode.
- Playlist URL, mode, quality, and output directory are persisted in browser local storage.

### Cookies for age-restricted videos
1. Export cookies once:
   ```bash
   yt-dlp --cookies-from-browser firefox --cookies /home/youruser/jellyfin-spotify/cookies.txt "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
   ```
2. Set `YTDLP_COOKIES_FILE` in `.env` to that file.
   - Or set `YTDLP_COOKIE_FILES` with comma-separated cookie files to rotate across multiple accounts.
3. Restart `./start.sh`.

### Audio normalization
- `NORMALIZE_AUDIO=true` runs ffmpeg `loudnorm` and rewrites the audio stream (it is not ReplayGain tagging).
- Output loudness target is controlled by `LOUDNORM_FILTER`.

### Database capture
- `track_downloads` now stores richer telemetry: selected uploader/channel, selected duration/view count, cookie+format used, per-track timing, original/final bitrate, audio duration/sample rate/channels, normalization flags, and attempt history.
- `track_download_events` stores append-only event history per track/run for scan/download attempts and failures.
- `job_checkpoints` stores latest run state (status/index/current track) per playlist+mode+output_dir.
- Artist genre lookups are cached in DB (`artists` table) to reduce repeated metadata calls.
- With `MATCH_CACHE_FIRST=true`, reruns reuse stored `matched_candidates_json` before calling YouTube search.

## Notes
- Downloads default to `./downloads` unless changed in UI.
- If search still picks a bad source for a track, rerunning can pick a different candidate due search result changes.
