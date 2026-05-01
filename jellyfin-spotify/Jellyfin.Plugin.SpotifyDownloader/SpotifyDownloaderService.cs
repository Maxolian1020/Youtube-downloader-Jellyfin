using System;
using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Plugin.SpotifyDownloader.Configuration;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.SpotifyDownloader;

public class SpotifyDownloaderService
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<SpotifyDownloaderService> _logger;
    private readonly SemaphoreSlim _startupLock = new(1, 1);
    private Process? _backendProcess;

    public SpotifyDownloaderService(IHttpClientFactory httpClientFactory, ILogger<SpotifyDownloaderService> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    private PluginConfiguration? Config => Plugin.Instance?.Configuration;

    public bool IsConfigured => Config is not null && !string.IsNullOrWhiteSpace(Config.AppRootPath);

    public bool IsProcessRunning => _backendProcess is not null && !_backendProcess.HasExited;

    public string BackendBaseUrl => Config?.BackendBaseUrl ?? "http://127.0.0.1:8000";

    public async Task<bool> EnsureBackendStartedAsync(CancellationToken cancellationToken = default)
    {
        if (!IsConfigured)
        {
            _logger.LogWarning("Spotify Downloader backend is not configured.");
            return false;
        }

        await _startupLock.WaitAsync(cancellationToken);
        try
        {
            if (IsProcessRunning && await IsBackendHealthyAsync(cancellationToken))
            {
                return true;
            }

            if (IsProcessRunning)
            {
                _logger.LogInformation("Existing backend process is unhealthy. Restarting.");
                await StopBackendAsync();
            }

            var config = Config!;
            if (string.IsNullOrWhiteSpace(config.PythonExecutablePath) || string.IsNullOrWhiteSpace(config.AppRootPath))
            {
                _logger.LogWarning("Backend executable path or app root path is missing.");
                return false;
            }

            var startInfo = new ProcessStartInfo
            {
                FileName = config.PythonExecutablePath,
                Arguments = $"-m uvicorn app:app --host 127.0.0.1 --port {config.BackendPort}",
                WorkingDirectory = config.AppRootPath,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            _logger.LogInformation("Starting Spotify Downloader backend: {Command}", startInfo.FileName + " " + startInfo.Arguments);
            try
            {
                _backendProcess = Process.Start(startInfo);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start Spotify Downloader backend process.");
                return false;
            }

            if (_backendProcess is null)
            {
                _logger.LogError("Failed to start backend process. Process.Start returned null.");
                return false;
            }

            _backendProcess.OutputDataReceived += (_, e) => { if (!string.IsNullOrEmpty(e.Data)) _logger.LogDebug("[SpotifyDownloader] {Message}", e.Data); };
            _backendProcess.ErrorDataReceived += (_, e) => { if (!string.IsNullOrEmpty(e.Data)) _logger.LogWarning("[SpotifyDownloader] {Message}", e.Data); };
            _backendProcess.BeginOutputReadLine();
            _backendProcess.BeginErrorReadLine();

            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
            {
                if (await IsBackendHealthyAsync(cancellationToken))
                {
                    _logger.LogInformation("Spotify Downloader backend is healthy.");
                    return true;
                }

                await Task.Delay(500, cancellationToken);
            }

            _logger.LogError("Spotify Downloader backend did not respond within the startup timeout.");
            return false;
        }
        finally
        {
            _startupLock.Release();
        }
    }

    public async Task<bool> IsBackendHealthyAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var client = _httpClientFactory.CreateClient(nameof(SpotifyDownloaderService));
            client.Timeout = TimeSpan.FromSeconds(5);
            using var response = await client.GetAsync(new Uri(new Uri(BackendBaseUrl), "api/health"), cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task StopBackendAsync()
    {
        if (_backendProcess is null)
        {
            return;
        }

        try
        {
            if (!_backendProcess.HasExited)
            {
                _backendProcess.Kill(true);
                await _backendProcess.WaitForExitAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to stop Spotify Downloader backend process.");
        }
        finally
        {
            _backendProcess = null;
        }
    }

    public async Task<HttpResponseMessage> ForwardRequestAsync(HttpMethod method, string path, string? body = null, string? contentType = null, CancellationToken cancellationToken = default)
    {
        var request = new HttpRequestMessage(method, new Uri(new Uri(BackendBaseUrl), path.TrimStart('/')));
        if (!string.IsNullOrEmpty(body))
        {
            request.Content = new StringContent(body, Encoding.UTF8, contentType ?? "application/json");
        }

        using var client = _httpClientFactory.CreateClient(nameof(SpotifyDownloaderService));
        client.Timeout = TimeSpan.FromSeconds(30);
        return await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
    }
}
