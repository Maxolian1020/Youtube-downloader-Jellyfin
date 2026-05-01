using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace Jellyfin.Plugin.SpotifyDownloader.Controllers;

[ApiController]
[Route("Plugins/SpotifyDownloader/Backend")]
public class SpotifyDownloaderController : ControllerBase
{
    private readonly SpotifyDownloaderService _backendService;

    public SpotifyDownloaderController(SpotifyDownloaderService backendService)
    {
        _backendService = backendService;
    }

    [HttpGet("health")]
    public async Task<IActionResult> Health()
    {
        if (!_backendService.IsConfigured)
        {
            return BadRequest(new { message = "Spotify Downloader backend is not configured." });
        }

        if (!await _backendService.EnsureBackendStartedAsync(HttpContext.RequestAborted))
        {
            return StatusCode(502, new { message = "Unable to start or reach the backend service." });
        }

        var response = await _backendService.ForwardRequestAsync(HttpMethod.Get, "api/health", null, null, HttpContext.RequestAborted);
        return await BuildProxyResult(response);
    }

    [HttpGet("playlists")]
    public async Task<IActionResult> GetPlaylists()
    {
        if (!await EnsureBackendAvailable())
        {
            return StatusCode(502, new { message = "Backend is unavailable." });
        }

        var response = await _backendService.ForwardRequestAsync(HttpMethod.Get, "api/playlists", null, null, HttpContext.RequestAborted);
        return await BuildProxyResult(response);
    }

    [HttpPost("playlists")]
    public async Task<IActionResult> CreatePlaylist()
    {
        if (!await EnsureBackendAvailable())
        {
            return StatusCode(502, new { message = "Backend is unavailable." });
        }

        var body = await new StreamReader(Request.Body).ReadToEndAsync();
        var response = await _backendService.ForwardRequestAsync(HttpMethod.Post, "api/playlists", body, Request.ContentType, HttpContext.RequestAborted);
        return await BuildProxyResult(response);
    }

    [HttpGet("playlists/{playlistId}/tracks")]
    public async Task<IActionResult> GetPlaylistTracks(string playlistId)
    {
        if (!await EnsureBackendAvailable())
        {
            return StatusCode(502, new { message = "Backend is unavailable." });
        }

        var response = await _backendService.ForwardRequestAsync(HttpMethod.Get, $"api/playlists/{playlistId}/tracks", null, null, HttpContext.RequestAborted);
        return await BuildProxyResult(response);
    }

    [HttpPost("import/youtube-video")]
    public async Task<IActionResult> ImportYoutubeVideo()
    {
        if (!await EnsureBackendAvailable())
        {
            return StatusCode(502, new { message = "Backend is unavailable." });
        }

        var body = await new StreamReader(Request.Body).ReadToEndAsync();
        var response = await _backendService.ForwardRequestAsync(HttpMethod.Post, "api/import/youtube-video", body, Request.ContentType, HttpContext.RequestAborted);
        return await BuildProxyResult(response);
    }

    private async Task<bool> EnsureBackendAvailable()
    {
        return _backendService.IsConfigured && await _backendService.EnsureBackendStartedAsync(HttpContext.RequestAborted);
    }

    private static async Task<IActionResult> BuildProxyResult(HttpResponseMessage response)
    {
        var content = await response.Content.ReadAsStringAsync();
        var contentType = response.Content.Headers.ContentType?.ToString() ?? "application/json";
        return new ContentResult
        {
            Content = content,
            ContentType = contentType,
            StatusCode = (int)response.StatusCode,
        };
    }
}
