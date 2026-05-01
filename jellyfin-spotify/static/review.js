const playlistSelect = document.getElementById('review_playlist_id');
const refreshBtn = document.getElementById('review_refresh_playlists');
const prevBtn = document.getElementById('review_prev');
const loadNextBtn = document.getElementById('review_load_next');
const meta = document.getElementById('review_meta');
const trackEl = document.getElementById('review_track');
const audioEl = document.getElementById('review_audio');
const autoplayEl = document.getElementById('review_autoplay');
const openUrlBtn = document.getElementById('review_open_url');
const approveBtn = document.getElementById('review_approve');
const manualUrlInput = document.getElementById('review_manual_url');
const manualTitleInput = document.getElementById('review_manual_title');
const manualBtn = document.getElementById('review_set_manual');
const notesInput = document.getElementById('review_notes');
const searchInput = document.getElementById('review_search_query');
const searchBtn = document.getElementById('review_search_btn');
const searchResults = document.getElementById('review_search_results');

let current = null;

function setMeta(text) {
  meta.textContent = text || '';
}

function renderSearchResults(items) {
  const rows = items || [];
  if (!searchResults) return;
  if (!rows.length) {
    searchResults.innerHTML = '';
    return;
  }
  searchResults.innerHTML = rows
    .map((item) => {
      const label = `${item.artist_name || 'unknown'} - ${item.track_name || 'unknown'}`;
      const sub = `#${item.position || '-'} | ${item.status || '-'} | review=${item.review_status || 'pending'}`;
      return `
        <div class="issue-row">
          <button type="button" class="manual-source-btn review-search-open" data-track-id="${item.track_id}">
            Load
          </button>
          <span>${label}</span><br />
          <span class="issue-sub">${sub}</span>
        </div>
      `;
    })
    .join('');
}

async function safeReadJson(response) {
  try {
    return await response.json();
  } catch (_) {
    try {
      const text = await response.text();
      return { detail: text ? String(text).slice(0, 200) : 'Non-JSON response' };
    } catch (__) {
      return { detail: 'Non-JSON response' };
    }
  }
}

async function loadPlaylists() {
  const response = await fetch('/api/playlists');
  const data = await safeReadJson(response);
  if (!response.ok) {
    setMeta(data.detail || `Failed to load playlists (HTTP ${response.status})`);
    return;
  }
  const items = data.items || [];
  const prev = (playlistSelect.value || '').trim();
  playlistSelect.innerHTML = items
    .map((p) => `<option value="${p.id}">${p.name || p.id} (${p.track_count || 0})</option>`)
    .join('');
  if (prev && items.some((p) => p.id === prev)) playlistSelect.value = prev;
  if (!playlistSelect.value && items.length) playlistSelect.value = items[0].id;
  renderSearchResults([]);
  setMeta('Select a playlist and click Load Next.');
}

async function loadSpecificTrack(trackId) {
  const playlistId = (playlistSelect.value || '').trim();
  if (!playlistId || !trackId) return;
  const response = await fetch(
    `/api/review/track?playlist_id=${encodeURIComponent(playlistId)}&track_id=${encodeURIComponent(trackId)}`
  );
  const data = await safeReadJson(response);
  if (!response.ok) {
    setMeta(data.detail || `Failed to load track (HTTP ${response.status})`);
    return;
  }
  renderTrack(data);
  setMeta(data ? 'Loaded searched track. You can review or override it here.' : 'Track not found.');
}

async function runSearch() {
  const playlistId = (playlistSelect.value || '').trim();
  const query = (searchInput?.value || '').trim();
  if (!playlistId) return;
  if (query.length < 2) {
    renderSearchResults([]);
    setMeta('Type at least 2 characters to search.');
    return;
  }
  const response = await fetch(
    `/api/review/search?playlist_id=${encodeURIComponent(playlistId)}&q=${encodeURIComponent(query)}`
  );
  const data = await safeReadJson(response);
  if (!response.ok) {
    setMeta(data.detail || `Failed to search (HTTP ${response.status})`);
    return;
  }
  renderSearchResults(data.items || []);
  setMeta(`Found ${data.count || 0} matching tracks.`);
}

function renderTrack(item) {
  current = item;
  if (!item) {
    trackEl.textContent = 'No more tracks to review for this playlist.';
    if (audioEl) {
      audioEl.pause();
      audioEl.removeAttribute('src');
      audioEl.load();
    }
    openUrlBtn.disabled = true;
    approveBtn.disabled = true;
    manualBtn.disabled = true;
    prevBtn.disabled = false;
    return;
  }
  openUrlBtn.disabled = !item.selected_url;
  approveBtn.disabled = false;
  manualBtn.disabled = false;
  prevBtn.disabled = false;
  const lines = [
    `#${item.position || '-'} ${item.artist_name || 'unknown'} - ${item.track_name || 'unknown'}`,
    `Album: ${item.album_name || '-'}`,
    `Status: ${item.status || '-'}`,
    `Downloaded URL: ${item.selected_url || '-'}`,
    `Downloaded Title: ${item.selected_title || '-'}`,
    `Destination: ${item.destination_path || '-'}`,
    `Updated: ${item.updated_at || '-'}`,
  ];
  trackEl.textContent = lines.join('\n');

  if (audioEl) {
    audioEl.pause();
    audioEl.src = `/api/review/file?playlist_id=${encodeURIComponent(item.playlist_id)}&track_id=${encodeURIComponent(item.track_id)}`;
    audioEl.load();
    if (autoplayEl && autoplayEl.checked) {
      audioEl.play().catch(() => {
        // Autoplay may be blocked until a user gesture; keep controls visible.
      });
    }
  }
}

async function loadNext() {
  const playlistId = (playlistSelect.value || '').trim();
  if (!playlistId) return;
  const response = await fetch(`/api/review/next?playlist_id=${encodeURIComponent(playlistId)}`);
  const data = await safeReadJson(response);
  if (!response.ok) {
    setMeta(data.detail || `Failed to load next (HTTP ${response.status})`);
    return;
  }
  renderTrack(data);
  setMeta(data ? 'Reviewing track. Approve or set a manual link.' : 'Review complete for this playlist.');
}

async function approveCurrent() {
  if (!current) return;
  const response = await fetch('/api/review/approve', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      playlist_id: current.playlist_id,
      track_id: current.track_id,
      notes: (notesInput.value || '').trim(),
    }),
  });
  const data = await safeReadJson(response);
  if (!response.ok) {
    alert(data.detail || 'Approve failed');
    return;
  }
  notesInput.value = '';
  await loadNext();
}

async function loadPrevious() {
  const playlistId = (playlistSelect.value || '').trim();
  if (!playlistId) return;
  const response = await fetch('/api/review/previous', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      playlist_id: playlistId,
      track_id: current?.track_id || 'previous',
      notes: '',
    }),
  });
  const data = await safeReadJson(response);
  if (!response.ok) {
    setMeta(data.detail || `Failed to load previous (HTTP ${response.status})`);
    return;
  }
  renderTrack(data);
  setMeta(data ? 'Moved back one track and cleared its review state.' : 'No previous reviewed track found.');
}

async function setManualAndFlag() {
  if (!current) return;
  const url = (manualUrlInput.value || '').trim();
  if (!url) {
    alert('Paste a YouTube URL first.');
    return;
  }
  const response = await fetch('/api/review/manual-source', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      playlist_id: current.playlist_id,
      track_id: current.track_id,
      youtube_url: url,
      title: (manualTitleInput.value || '').trim(),
      notes: (notesInput.value || '').trim(),
    }),
  });
  const data = await safeReadJson(response);
  if (!response.ok) {
    alert(data.detail || 'Manual source failed');
    return;
  }
  manualUrlInput.value = '';
  manualTitleInput.value = '';
  notesInput.value = '';
  await loadNext();
}

openUrlBtn.addEventListener('click', () => {
  if (!current || !current.selected_url) return;
  window.open(current.selected_url, '_blank', 'noopener,noreferrer');
});
approveBtn.addEventListener('click', approveCurrent);
manualBtn.addEventListener('click', setManualAndFlag);
refreshBtn.addEventListener('click', loadPlaylists);
prevBtn.addEventListener('click', loadPrevious);
loadNextBtn.addEventListener('click', loadNext);
searchBtn.addEventListener('click', runSearch);
searchResults.addEventListener('click', (event) => {
  const target = event.target;
  if (!target || !target.classList.contains('review-search-open')) return;
  const trackId = target.getAttribute('data-track-id');
  loadSpecificTrack(trackId);
});
searchInput.addEventListener('keydown', (event) => {
  if (event.key !== 'Enter') return;
  event.preventDefault();
  runSearch();
});

loadPlaylists();
renderTrack(null);
