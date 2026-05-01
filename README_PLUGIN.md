# Jellyfin Spotify Downloader Plugin

Download Spotify playlists and import YouTube videos directly into your Jellyfin media library.

## Features

- 🎵 **Download Spotify Playlists** - Batch download entire playlists with automatic metadata tagging
- 🎥 **Import YouTube Videos** - Add individual YouTube videos to your playlists
- 📁 **Library Selection** - Choose which Jellyfin library to save downloads to
- 🎛️ **Admin Configuration** - Configure Python backend, ports, and default paths via Jellyfin admin panel
- 👤 **User Interface** - Intuitive playlist management and import interface within Jellyfin
- 🚀 **Auto-Start Backend** - Automatically starts the Python backend on Jellyfin startup

## Installation

### Requirements

- Jellyfin 10.11.0 or later
- Python 3.8+ with dependencies (yt-dlp, eyed3, etc.)
- .NET Runtime 9.0+

### Setup

1. **Download the Plugin DLL**
   - Download from [Releases](https://github.com/Maxolian1020/Youtube-downloader-Jellyfin/releases)

2. **Install the Plugin**
   - Copy `Jellyfin.Plugin.SpotifyDownloader.dll` to your Jellyfin plugins folder
   - Typically: `C:\ProgramData\Jellyfin\plugins\` (Windows) or `/var/lib/jellyfin/plugins/` (Linux)

3. **Configure in Jellyfin Admin Panel**
   - Go to **Admin → Plugins → Spotify Downloader**
   - Set Python executable path (e.g., `python` or `/usr/bin/python3`)
   - Set the app root path (location of `app.py` and backend files)
   - Set backend port (default: 8000)
   - Set default library path (where downloads go)
   - Enable auto-start if desired

4. **Restart Jellyfin**
   - Restart the Jellyfin service for changes to take effect

## Usage

### In Jellyfin Web Interface

1. Open the **Spotify Downloader** plugin
2. **Select a Library** - Choose where downloads should be saved
3. **Select or Create a Playlist** - Manage playlists for organizing imports
4. **Import a YouTube Video** - Paste a YouTube URL to add to your playlist
5. **Start Download** - Initiate the download job to save tracks to your library

## Project Structure

```
youtube-downloader-jellyfin/
├── jellyfin-spotify/
│   ├── app.py                      # Python FastAPI backend
│   ├── requirements.txt
│   ├── Jellyfin.Plugin.SpotifyDownloader/
│   │   ├── Plugin.cs               # Main plugin class
│   │   ├── ServiceRegistrator.cs   # Dependency injection
│   │   ├── Configuration/          # Settings classes & pages
│   │   ├── Controllers/            # API endpoints
│   │   ├── UserPage.html           # User interface
│   │   └── *.csproj
│   ├── static/                     # Frontend assets (JS, CSS)
│   ├── templates/                  # HTML templates
│   └── downloads/                  # Default download directory
├── manifest.json                   # Plugin manifest for Jellyfin
└── README.md
```

## Configuration

The plugin stores configuration in Jellyfin's configuration database. Key settings:

- **PythonExecutablePath** - Path to Python interpreter
- **AppRootPath** - Root directory of the Python backend
- **BackendPort** - Port for the FastAPI backend
- **DownloadDirectory** - Default fallback download location
- **DefaultLibraryPath** - Default Jellyfin library path
- **AutoStartBackend** - Auto-start backend with Jellyfin
- **WebAppUrl** - Optional direct URL to the web interface

## Building from Source

### Prerequisites

- .NET SDK 9.0+
- PowerShell or bash

### Build

```bash
# Restore dependencies
dotnet restore jellyfin-spotify/Jellyfin.Plugin.SpotifyDownloader/Jellyfin.Plugin.SpotifyDownloader.csproj

# Build the plugin
dotnet build jellyfin-spotify/Jellyfin.Plugin.SpotifyDownloader/Jellyfin.Plugin.SpotifyDownloader.csproj --configuration Release

# Output: jellyfin-spotify/Jellyfin.Plugin.SpotifyDownloader/bin/Release/net9.0/Jellyfin.Plugin.SpotifyDownloader.dll
```

## Backend Requirements

The plugin requires the Python backend from this repository. Make sure the `app.py` and supporting files are properly installed:

1. Install Python dependencies:
   ```bash
   pip install -r jellyfin-spotify/requirements.txt
   ```

2. Ensure the backend path is correctly configured in Jellyfin admin settings

3. The backend will be automatically started unless auto-start is disabled

## API Endpoints

The plugin exposes the following API endpoints (proxied through Jellyfin):

- `GET /Plugins/SpotifyDownloader/Backend/playlists` - List playlists
- `POST /Plugins/SpotifyDownloader/Backend/playlists` - Create playlist
- `GET /Plugins/SpotifyDownloader/Backend/playlists/{id}/tracks` - List tracks
- `POST /Plugins/SpotifyDownloader/Backend/import/youtube-video` - Import YouTube video
- `POST /Plugins/SpotifyDownloader/Backend/download` - Start download job

## Troubleshooting

### Backend Won't Start

- Check Python path in admin settings
- Verify app root path points to correct directory
- Check Jellyfin logs for error messages
- Ensure port is not already in use

### Import Failed

- Verify YouTube URL is valid
- Check internet connectivity
- Review backend logs
- Ensure yt-dlp is installed

### Library Selection Not Working

- Verify Jellyfin libraries are configured
- Check admin settings for correct library paths
- Restart plugin if recently configured

## License

MIT License - See LICENSE file for details

## Contributing

Contributions are welcome! Please fork the repository and submit pull requests.

## Support

For issues, questions, or suggestions, please open an issue on [GitHub](https://github.com/Maxolian1020/Youtube-downloader-Jellyfin/issues).

## Credits

- Built for Jellyfin media server
- Uses yt-dlp for YouTube downloading
- Uses eyed3 for audio metadata tagging
