using MediaBrowser.Model.Plugins;

namespace Jellyfin.Plugin.SpotifyDownloader.Configuration;

/// <summary>
/// Plugin configuration.
/// </summary>
public class PluginConfiguration : BasePluginConfiguration
{
    public PluginConfiguration()
    {
        PythonExecutablePath = "python";
        AppRootPath = string.Empty;
        BackendPort = 8000;
        DownloadDirectory = string.Empty;
        AutoStartBackend = true;
        WebAppUrl = "http://127.0.0.1:8000";
        DefaultLibraryPath = string.Empty;
    }

    public string PythonExecutablePath { get; set; }

    public string AppRootPath { get; set; }

    public int BackendPort { get; set; }

    public string DownloadDirectory { get; set; }

    public bool AutoStartBackend { get; set; }

    public string WebAppUrl { get; set; }

    public string DefaultLibraryPath { get; set; }

    public string BackendBaseUrl => $"http://127.0.0.1:{BackendPort}";
}
