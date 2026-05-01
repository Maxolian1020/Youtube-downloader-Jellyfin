const form = document.getElementById('download-form');
const statusCard = document.getElementById('status');
const summary = document.getElementById('summary');
const details = document.getElementById('details');
const failures = document.getElementById('failures');
const logs = document.getElementById('logs');
const progressBar = document.getElementById('progress-bar');

const playlistSelect = document.getElementById('playlist_id');
const playlistMeta = document.getElementById('playlist-meta');
const refreshPlaylistsBtn = document.getElementById('refresh-playlists');
const importYoutubeVideoBtn = document.getElementById('import-youtube-video');
const youtubeVideoUrlInput = document.getElementById('youtube_video_url');
const loadIssuesBtn = document.getElementById('load-issues');
const resolveIssuesBtn = document.getElementById('resolve-issues');
const issuesSummary = document.getElementById('issues-summary');
const issuesList = document.getElementById('issues-list');
const pauseJobBtn = document.getElementById('pause-job');
const resumeJobBtn = document.getElementById('resume-job');
const stopJobBtn = document.getElementById('stop-job');

let currentJobId = null;
let pollHandle = null;
let currentIssues = [];
let pendingIssueTrackIds = new Set();
let pendingManualSources = new Map();

const STORAGE_KEYS = {
  playlistId: 'dl_playlist_id',
  outputDir: 'spotify_dl_output_dir',
  mode: 'spotify_dl_mode',
  quality: 'spotify_dl_quality',
  jobId: 'spotify_dl_current_job_id',
};

function fmtTime(value) {
  if (!value) return '-';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

function setSummary(job) {
  summary.textContent = `Mode: ${job.mode} | Status: ${job.status} | Control: ${job.control_state || '-'} | Completed: ${job.completed}/${job.total} | Failed: ${job.failed} | Suspicious: ${job.suspicious_tracks || 0} | Cached: ${job.cached_tracks || 0} | Success: ${job.progress_pct}% | Processed: ${job.processed_pct || 0}%`;
  progressBar.style.width = `${job.progress_pct || 0}%`;
  details.innerHTML = [
    `Playlist: ${job.playlist_name || job.playlist}`,
    `Current: ${job.current_track || '-'}`,
    `Quality: ${job.quality} kbps`,
    `Output: ${job.output_dir}`,
    `Auto pause until: ${fmtTime(job.auto_paused_until)}`,
    `Started: ${fmtTime(job.started_at)}`,
    `Finished: ${fmtTime(job.finished_at)}`,
  ].join('<br>');
  updateControlButtons(job);
}

function updateControlButtons(job) {
  const ended = ['finished', 'failed', 'stopped'].includes(job.status);
  pauseJobBtn.disabled = ended || job.control_state === 'paused';
  resumeJobBtn.disabled = ended || job.control_state !== 'paused';
  stopJobBtn.disabled = ended || job.control_state === 'stopping';
}

function setFailures(job) {
  const rows = job.failed_details || [];
  const extras = job.extra_files || [];
  if (!rows.length && !extras.length) {
    failures.textContent = 'Issues: none';
    return;
  }
  const out = [];
  if (rows.length) {
    out.push('Issues:');
    out.push(...rows.map((x) => `- ${x.track}: ${x.reason}`));
  } else {
    out.push('Issues: none');
  }
  if (extras.length) {
    out.push('');
    out.push(`Extra files (showing up to ${extras.length}):`);
    out.push(...extras.map((p) => `- ${p}`));
  }
  failures.textContent = out.join('\n');
}

function persistFormState() {
  localStorage.setItem(STORAGE_KEYS.playlistId, playlistSelect.value);
  localStorage.setItem(STORAGE_KEYS.outputDir, document.getElementById('output_dir').value);
  localStorage.setItem(STORAGE_KEYS.mode, document.getElementById('mode').value);
  localStorage.setItem(STORAGE_KEYS.quality, document.getElementById('quality').value);
}

function restoreFormState() {
  const playlistId = localStorage.getItem(STORAGE_KEYS.playlistId);
  const outputDir = localStorage.getItem(STORAGE_KEYS.outputDir);
  const mode = localStorage.getItem(STORAGE_KEYS.mode);
  const quality = localStorage.getItem(STORAGE_KEYS.quality);
  if (playlistId) playlistSelect.dataset.restoreValue = playlistId;
  if (outputDir) document.getElementById('output_dir').value = outputDir;
  if (mode) document.getElementById('mode').value = mode;
  if (quality) document.getElementById('quality').value = quality;
}

function renderIssues(items, resetPending = true) {
  currentIssues = items || [];
  if (resetPending) {
    pendingIssueTrackIds = new Set();
    pendingManualSources = new Map();
  }
  if (!currentIssues.length) {
    issuesSummary.textContent = 'No failed/suspicious tracks found in DB for this playlist.';
    issuesList.innerHTML = '';
    return;
  }

  const suspiciousCount = currentIssues.filter((item) => Boolean(item.suspicious)).length;
  const failedCount = currentIssues.filter((item) => String(item.status) === 'failed').length;
  issuesSummary.textContent = `Loaded ${currentIssues.length} issue rows (${suspiciousCount} suspicious, ${failedCount} failed). All checkboxes start unchecked. Check rows to confirm/fix, then click Confirm.`;
  issuesList.innerHTML = currentIssues
    .map((item, idx) => {
      const label = `${item.artist_name} - ${item.track_name}`;
      const searchQuery = encodeURIComponent(`${item.artist_name || ''} ${item.track_name || ''}`.trim());
      const pendingManual = pendingManualSources.get(item.track_id) || {};
      const pending = pendingIssueTrackIds.has(item.track_id);
      const rowClass = pending ? 'issue-row issue-row-pending' : 'issue-row';
      const isFailed = String(item.status) === 'failed';
      const canSelect = Boolean(item.suspicious) || isFailed;
      const pendingText = pending ? ' -> will be fixed on confirm (clear suspicious and/or stale failed)' : '';
      const reasonLabel = (item.suspicious_reason || '').trim()
        || (item.last_error || '').trim()
        || (item.suspicious ? 'flagged as suspicious' : item.status || 'issue');
      const sub = `${reasonLabel}${pendingText}`;
      return `
        <div class="${rowClass}" data-issue-index="${idx}">
          <label>
            <input type="checkbox" data-issue-index="${idx}" ${pending ? 'checked' : ''} ${canSelect ? '' : 'disabled'} />
            ${label}
          </label><br />
          <span class="issue-sub">${sub}</span>
          <div class="manual-source-row">
            <input
              type="text"
              class="manual-url-input"
              data-issue-index="${idx}"
              value="${pendingManual.url || ''}"
              placeholder="Paste YouTube URL for this track"
            />
            <input
              type="text"
              class="manual-title-input"
              data-issue-index="${idx}"
              value="${pendingManual.title || ''}"
              placeholder="Optional custom title"
            />
            <button type="button" class="manual-source-btn" data-issue-index="${idx}">Set Manual Source</button>
            <a class="manual-search-link" href="https://www.youtube.com/results?search_query=${searchQuery}" target="_blank" rel="noopener noreferrer">Search</a>
          </div>
        </div>
      `;
    })
    .join('');
}

async function loadIssues() {
  const playlistId = (playlistSelect.value || '').trim();
  if (!playlistId) {
    alert('Select a playlist first.');
    return;
  }
  const response = await fetch(`/api/issues?playlist_id=${encodeURIComponent(playlistId)}`);
  const data = await response.json();
  if (!response.ok) {
    alert(data.detail || 'Failed to load issues');
    return;
  }
  renderIssues(data.items || []);
}

async function resolveSelectedIssues() {
  const playlistId = (playlistSelect.value || '').trim();
  if (!playlistId) {
    alert('Select a playlist first.');
    return;
  }
  const trackIds = Array.from(pendingIssueTrackIds);

  if (!trackIds.length) {
    alert('No rows marked for suspicious-clear.');
    return;
  }

  const response = await fetch('/api/issues/resolve', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ playlist_id: playlistId, track_ids: trackIds }),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.detail || 'Failed to resolve issues');
    return;
  }
  issuesSummary.textContent = `Updated ${data.updated}. Remaining failed=${data.remaining_failed}, suspicious=${data.remaining_suspicious}`;
  pendingIssueTrackIds = new Set();
  await loadIssues();
}

function onIssueToggle(event) {
  const target = event.target;
  if (!target || target.tagName !== 'INPUT' || target.type !== 'checkbox') return;
  const idx = Number(target.getAttribute('data-issue-index'));
  if (!Number.isInteger(idx) || idx < 0 || idx >= currentIssues.length) return;
  const issue = currentIssues[idx];
  if (!issue || !issue.track_id) return;
  if (target.checked) {
    pendingIssueTrackIds.add(issue.track_id);
  } else {
    pendingIssueTrackIds.delete(issue.track_id);
  }
  renderIssues(currentIssues, false);
}

async function applyManualSource(idx) {
  if (!Number.isInteger(idx) || idx < 0 || idx >= currentIssues.length) return;
  const issue = currentIssues[idx];
  if (!issue || !issue.track_id) return;
  const urlInput = issuesList.querySelector(`.manual-url-input[data-issue-index="${idx}"]`);
  const titleInput = issuesList.querySelector(`.manual-title-input[data-issue-index="${idx}"]`);
  const url = (urlInput?.value || '').trim();
  const title = (titleInput?.value || '').trim();
  if (!url) {
    alert('Paste a YouTube URL first.');
    return;
  }
  pendingManualSources.set(issue.track_id, { url, title });
  const playlistId = (playlistSelect.value || '').trim();
  const response = await fetch('/api/issues/manual-source', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      playlist_id: playlistId,
      track_id: issue.track_id,
      youtube_url: url,
      title,
    }),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.detail || 'Failed to save manual source');
    return;
  }
  issuesSummary.textContent = `Manual source set for ${issue.artist_name} - ${issue.track_name}. Run Retry Failed/Suspicious to force this URL.`;
  await loadIssues();
}

function onIssueClick(event) {
  const target = event.target;
  if (!target) return;
  if (!target.classList.contains('manual-source-btn')) return;
  event.preventDefault();
  const idx = Number(target.getAttribute('data-issue-index'));
  if (!Number.isInteger(idx)) return;
  applyManualSource(idx);
}

function setLogs(job) {
  logs.textContent = (job.logs || []).join('\n');
  logs.scrollTop = logs.scrollHeight;
}

function setPlaylistMeta(text) {
  if (!playlistMeta) return;
  playlistMeta.textContent = text || '';
}

async function safeReadJson(response) {
  try {
    return await response.json();
  } catch (_) {
    try {
      const text = await response.text();
      return { detail: text ? `Non-JSON response: ${String(text).slice(0, 200)}` : 'Non-JSON response' };
    } catch (__) {
      return { detail: 'Non-JSON response' };
    }
  }
}

async function loadPlaylists() {
  let response;
  try {
    response = await fetch('/api/playlists');
  } catch (err) {
    setPlaylistMeta(`Failed to reach backend: ${err}`);
    return;
  }
  const data = await safeReadJson(response);
  if (!response.ok) {
    const detail = data.detail || `Failed to load playlists (HTTP ${response.status})`;
    setPlaylistMeta(`${detail}. If you just updated the code, restart the backend.`);
    return;
  }
  const items = data.items || [];
  const prev = ((playlistSelect.value || '').trim() || (playlistSelect.dataset.restoreValue || '').trim());
  playlistSelect.innerHTML = items
    .map((p) => {
      const label = `${p.name || p.id} (${p.track_count || 0})`;
      return `<option value="${p.id}">${label}</option>`;
    })
    .join('');
  if (prev && items.some((p) => p.id === prev)) {
    playlistSelect.value = prev;
  }
  playlistSelect.dataset.restoreValue = '';
  if (playlistSelect.value) {
    const selected = items.find((p) => p.id === playlistSelect.value);
    setPlaylistMeta(selected ? `Selected: ${selected.name || selected.id} | Tracks: ${selected.track_count || 0}` : '');
  } else {
    setPlaylistMeta(items.length ? 'Select a playlist.' : 'No playlists found in the database.');
  }
}

async function importYoutubeVideo() {
  const url = (youtubeVideoUrlInput?.value || '').trim();
  if (!url) {
    alert('Paste a YouTube video URL.');
    return;
  }
  const playlistId = (playlistSelect.value || '').trim();
  if (!playlistId) {
    alert('Select a playlist first.');
    return;
  }
  const response = await fetch('/api/import/youtube-video', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url, playlist_id: playlistId }),
  });
  const data = await safeReadJson(response);
  if (!response.ok) {
    alert(data.detail || 'Failed to import YouTube video');
    return;
  }
  youtubeVideoUrlInput.value = '';
  await loadPlaylists();
  if (data.playlist_id) {
    playlistSelect.value = data.playlist_id;
    persistFormState();
  }
  setPlaylistMeta(`Imported video into ${data.playlist_id}`);
}

async function pollJob(jobId) {
  const response = await fetch(`/api/jobs/${jobId}`);
  if (!response.ok) {
    throw new Error(`Failed to load job ${jobId}`);
  }
  const job = await response.json();
  setSummary(job);
  setFailures(job);
  setLogs(job);

  if (job.status === 'finished' || job.status === 'failed' || job.status === 'stopped') {
    clearInterval(pollHandle);
    pollHandle = null;
    localStorage.removeItem(STORAGE_KEYS.jobId);
  }
}

function persistCurrentJobId() {
  if (currentJobId) {
    localStorage.setItem(STORAGE_KEYS.jobId, currentJobId);
  }
}

async function attachToJob(jobId) {
  currentJobId = jobId;
  persistCurrentJobId();
  statusCard.classList.remove('hidden');
  if (pollHandle) clearInterval(pollHandle);
  await pollJob(currentJobId);
  pollHandle = setInterval(() => pollJob(currentJobId), 2000);
}

async function restoreActiveJobFromServerOrStorage() {
  try {
    const activeResp = await fetch('/api/active-job');
    if (activeResp.ok) {
      const active = await activeResp.json();
      if (active.active && active.job_id) {
        await attachToJob(active.job_id);
        return;
      }
    }
  } catch (_) {
    // Ignore and fallback to local storage.
  }

  const savedJobId = localStorage.getItem(STORAGE_KEYS.jobId);
  if (!savedJobId) return;
  try {
    await attachToJob(savedJobId);
  } catch (_) {
    localStorage.removeItem(STORAGE_KEYS.jobId);
  }
}

async function sendJobControl(action) {
  if (!currentJobId) {
    alert('No active job.');
    return;
  }
  const response = await fetch(`/api/jobs/${currentJobId}/control`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action }),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.detail || `Failed to ${action} job`);
    return;
  }
  await pollJob(currentJobId);
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  persistFormState();

  const selectedMode = document.getElementById('mode').value;
  const playlistId = (playlistSelect.value || '').trim();
  if (!playlistId) {
    alert('Select a playlist first.');
    return;
  }

  const payload = {
    mode: selectedMode,
    quality: document.getElementById('quality').value,
    output_dir: document.getElementById('output_dir').value.trim(),
    playlist_id: playlistId,
  };

  const response = await fetch('/api/download', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!response.ok) {
    alert(data.detail || 'Failed to start download');
    return;
  }

  currentJobId = data.job_id;
  persistCurrentJobId();
  statusCard.classList.remove('hidden');
  logs.textContent = '';
  failures.textContent = '';
  details.textContent = '';
  progressBar.style.width = '0%';

  if (pollHandle) {
    clearInterval(pollHandle);
  }

  await pollJob(currentJobId);
  pollHandle = setInterval(() => pollJob(currentJobId), 2000);
});

playlistSelect.addEventListener('change', () => {
  persistFormState();
  loadPlaylists();
});
document.getElementById('output_dir').addEventListener('input', persistFormState);
document.getElementById('mode').addEventListener('change', persistFormState);
document.getElementById('quality').addEventListener('change', persistFormState);
loadIssuesBtn.addEventListener('click', loadIssues);
resolveIssuesBtn.addEventListener('click', resolveSelectedIssues);
issuesList.addEventListener('change', onIssueToggle);
issuesList.addEventListener('click', onIssueClick);
pauseJobBtn.addEventListener('click', () => sendJobControl('pause'));
resumeJobBtn.addEventListener('click', () => sendJobControl('resume'));
stopJobBtn.addEventListener('click', () => sendJobControl('stop'));
refreshPlaylistsBtn.addEventListener('click', loadPlaylists);
importYoutubeVideoBtn.addEventListener('click', importYoutubeVideo);

pauseJobBtn.disabled = true;
resumeJobBtn.disabled = true;
stopJobBtn.disabled = true;

restoreFormState();
loadPlaylists();
restoreActiveJobFromServerOrStorage();
