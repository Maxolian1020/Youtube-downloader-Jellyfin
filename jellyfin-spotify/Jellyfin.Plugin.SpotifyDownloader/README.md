# Jellyfin Spotify Downloader Plugin

This folder contains a Jellyfin plugin scaffold for the existing `jellyfin-spotify` webapp.

## What is included

- A Jellyfin plugin project targeting .NET 9.0.
- `PluginConfiguration` with settings for the Python executable, backend app root, backend port, webapp URL, and download location.
- A Jellyfin admin configuration page.
- A user-facing plugin page where users can create playlists, import YouTube videos, and view playlist tracks directly inside Jellyfin.

## Next steps

1. Install the .NET SDK (required to build the plugin).
2. Build the plugin project:
   ```powershell
dotnet publish .\Jellyfin.Plugin.SpotifyDownloader\Jellyfin.Plugin.SpotifyDownloader.csproj -c Release
   ```
3. Copy the generated DLL to your Jellyfin plugin directory or use a plugin packaging workflow.
4. Configure the plugin in Jellyfin:
   - `Python executable`
   - `Backend app root`
   - `Backend port`
   - `Default save location`
   - `Auto-start backend on Jellyfin startup`

## Notes

- The plugin is designed to auto-start the Python backend and proxy user-facing requests through Jellyfin.
- Users can import YouTube videos directly through the plugin page once the backend is configured and running.
- The imported audio files can be saved to a location that Jellyfin scans as a music library.
