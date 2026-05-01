using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.SpotifyDownloader;

public class SpotifyDownloaderHostService : IHostedService
{
    private readonly SpotifyDownloaderService _backendService;
    private readonly ILogger<SpotifyDownloaderHostService> _logger;

    public SpotifyDownloaderHostService(SpotifyDownloaderService backendService, ILogger<SpotifyDownloaderHostService> logger)
    {
        _backendService = backendService;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (Plugin.Instance?.Configuration?.AutoStartBackend ?? false)
        {
            _logger.LogInformation("Starting Spotify Downloader backend from Jellyfin plugin.");
            await _backendService.EnsureBackendStartedAsync(cancellationToken);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _backendService.StopBackendAsync();
    }
}
