let currentChannelName = null;
let isEditMode = false;
let refreshInterval = null;

// Open feed URL in new tab
function openFeedUrl(url) {
    window.open(url, '_blank');
}

// Rotate a channel's private feed token, then reload to show fresh URLs
async function rotateFeedToken(channelName, button) {
    if (!confirm('Rotate the feed token for this channel?\n\nExisting tokenized feed URLs for it will stop working.')) {
        return;
    }
    const originalText = button.textContent;
    try {
        button.textContent = '⏳ Rotating...';
        button.disabled = true;
        const response = await fetch(`/api/channels/${encodeURIComponent(channelName)}/rotate-token`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        });
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.error || 'Token rotation failed');
        }
        showToast('✅ Feed token rotated');
        window.location.reload();
    } catch (error) {
        console.error('Error rotating feed token:', error);
        showToast('❌ ' + (error.message || 'Token rotation failed'));
        button.textContent = originalText;
        button.disabled = false;
    }
}

// Copy feed URL functionality
async function copyFeedUrl(url, button) {
    try {
        await navigator.clipboard.writeText(url);
        
        // Update button
        const originalText = button.innerHTML;
        button.innerHTML = '✅ Copied!';
        button.classList.add('copied');
        
        // Show toast
        showToast('📋 Feed URL copied to clipboard!');
        
        // Reset after 2 seconds
        setTimeout(() => {
            button.innerHTML = originalText;
            button.classList.remove('copied');
        }, 2000);
        
    } catch (err) {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = url;
        document.body.appendChild(textArea);
        textArea.select();
        document.execCommand('copy');
        document.body.removeChild(textArea);
        
        // Show feedback
        button.innerHTML = '✅ Copied!';
        button.classList.add('copied');
        
        setTimeout(() => {
            button.innerHTML = '📋 Copy RSS Feed URL';
            button.classList.remove('copied');
        }, 2000);
    }
}

// Toast notification
function showToast(message) {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    
    // Remove existing type classes
    toast.classList.remove('success', 'error', 'warning');
    
    // Auto-detect message type based on emoji/content
    if (message.includes('✅') || message.toLowerCase().includes('success')) {
        toast.classList.add('success');
    } else if (message.includes('❌') || message.toLowerCase().includes('error') || message.toLowerCase().includes('failed')) {
        toast.classList.add('error');
    } else if (message.includes('⚠️') || message.toLowerCase().includes('warning')) {
        toast.classList.add('warning');
    }
    // Default style (neutral) if no specific type detected
    
    toast.classList.add('show');
    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}

// Modal management
function openAddChannelModal() {
    isEditMode = false;
    currentChannelName = null;
    document.getElementById('modalTitle').textContent = 'Add New Channel';
    document.getElementById('submitBtn').textContent = 'Add Channel';
    document.getElementById('dangerZone').style.display = 'none';
    document.getElementById('tombstoneRow').style.display = 'none';
    clearForm();

    // Pre-check sponsor-related categories
    document.getElementById('sponsor').checked = true;
    document.getElementById('selfpromo').checked = true;
    document.getElementById('interaction').checked = true;

    document.getElementById('channelModal').style.display = 'block';
    setTimeout(adjustTooltipPositions, 100);
}

async function openEditChannelModal(channelName) {
    isEditMode = true;
    currentChannelName = channelName;
    document.getElementById('modalTitle').textContent = 'Edit Channel';
    document.getElementById('submitBtn').textContent = 'Update Channel';
    document.getElementById('dangerZone').style.display = 'block';

    // Reset danger zone to collapsed state
    document.getElementById('dangerZoneContent').classList.remove('expanded');
    document.getElementById('dangerZoneToggle').classList.remove('rotated');

    // Load channel data
    try {
        const response = await fetch('/api/channels');
        const data = await response.json();
        const channel = data.channels.find(ch => ch.name === channelName);

        if (channel) {
            populateForm(channel);
            refreshTombstoneRow();
            document.getElementById('channelModal').style.display = 'block';
            setTimeout(adjustTooltipPositions, 100);
        } else {
            showToast('❌ Channel not found');
        }
    } catch (error) {
        console.error('Error loading channel data:', error);
        showToast('❌ Error loading channel data');
    }
}

function closeChannelModal() {
    document.getElementById('channelModal').style.display = 'none';
    clearForm();
}

function toggleDangerZone() {
    const content = document.getElementById('dangerZoneContent');
    const toggle = document.getElementById('dangerZoneToggle');
    
    if (content.classList.contains('expanded')) {
        content.classList.remove('expanded');
        toggle.classList.remove('rotated');
    } else {
        content.classList.add('expanded');
        toggle.classList.add('rotated');
    }
}

// Scheduler modal management
async function openScheduleModal() {
    try {
        const response = await fetch('/api/config/refresh-interval');
        const data = await response.json();

        if (response.ok) {
            document.getElementById('refreshInterval').value = data.refresh_interval_hours;
            document.getElementById('currentInterval').textContent = `${data.refresh_interval_hours} hours`;

            const nextRun = data.next_run ? formatDateTime(data.next_run) : 'Not scheduled';
            document.getElementById('nextRun').textContent = nextRun;

            await populateStorageSettings();
            await populateNotificationSettings();
            document.getElementById('scheduleModal').style.display = 'block';
        } else {
            showToast('❌ Failed to load scheduler settings');
        }
    } catch (error) {
        console.error('Error loading scheduler settings:', error);
        showToast('❌ Network error occurred');
    }
}

async function populateStorageSettings() {
    const storageResponse = await fetch('/api/config/storage');
    if (!storageResponse.ok) return;
    const storage = await storageResponse.json();
    document.getElementById('globalQuota').value = storage.global_quota_gb > 0 ? storage.global_quota_gb : '';
    document.getElementById('minFreeSpace').value = storage.min_free_space_gb;
    document.getElementById('retentionDays').value = storage.retention_days > 0 ? storage.retention_days : '';
}

function closeScheduleModal() {
    document.getElementById('scheduleModal').style.display = 'none';
}

// Form management
function clearForm() {
    document.getElementById('channelForm').reset();
    // Clear only the SponsorBlock checkboxes; the filter checkboxes
    // (skipShorts/skipLivestreams) default to false via form.reset().
    const checkboxes = document.querySelectorAll('#sponsorblockGroup input[type="checkbox"]');
    checkboxes.forEach(cb => cb.checked = false);
}

// Parse comma- or newline-separated title filter rules.
function parseFilterRules(text) {
    return text.split(/[\n,]+/).map(rule => rule.trim()).filter(Boolean);
}

// Convert a minutes input to whole seconds; 0/empty means "no limit".
function minutesToSeconds(text) {
    const minutes = parseFloat(text);
    return Number.isFinite(minutes) && minutes > 0 ? Math.round(minutes * 60) : 0;
}

// Convert stored seconds to a minutes input value; empty when disabled.
function secondsToMinutes(seconds) {
    return seconds > 0 ? seconds / 60 : '';
}

function populateForm(channel) {
    document.getElementById('channelDisplayName').value = channel.display_name || channel.name || '';
    document.getElementById('channelUrl').value = channel.url || '';
    document.getElementById('maxVideos').value = channel.max_episodes || '';
    document.getElementById('downloadDelay').value = channel.download_delay_hours || '';
    document.getElementById('format').value = channel.format || 'video';
    document.getElementById('quality').value = channel.quality || 'max';
    document.getElementById('channelQuota').value = channel.quota_gb > 0 ? channel.quota_gb : '';
    document.getElementById('channelRetention').value = channel.retention_days > 0 ? channel.retention_days : '';

    // Episode filters
    document.getElementById('titleInclude').value = (channel.title_include || []).join(', ');
    document.getElementById('titleExclude').value = (channel.title_exclude || []).join(', ');
    document.getElementById('minDurationMinutes').value = secondsToMinutes(channel.min_duration_seconds || 0);
    document.getElementById('maxDurationMinutes').value = secondsToMinutes(channel.max_duration_seconds || 0);
    document.getElementById('skipShorts').checked = channel.skip_shorts === true;
    document.getElementById('skipLivestreams').checked = channel.skip_livestreams === true;

    // Set SponsorBlock categories
    const categories = channel.sponsorblock_categories || [];
    const checkboxes = document.querySelectorAll('#sponsorblockGroup input[type="checkbox"]');
    checkboxes.forEach(cb => {
        cb.checked = categories.includes(cb.value);
    });
}

function getFormData() {
    const formData = {
        display_name: document.getElementById('channelDisplayName').value.trim(),
        url: document.getElementById('channelUrl').value.trim(),
        max_episodes: parseInt(document.getElementById('maxVideos').value) || 10,
        download_delay_hours: parseInt(document.getElementById('downloadDelay').value) || 6,
        format: document.getElementById('format').value || 'video',
        quality: document.getElementById('quality').value || 'max',
        quota_gb: parseFloat(document.getElementById('channelQuota').value) || 0,
        retention_days: parseInt(document.getElementById('channelRetention').value) || 0,
        title_include: parseFilterRules(document.getElementById('titleInclude').value),
        title_exclude: parseFilterRules(document.getElementById('titleExclude').value),
        min_duration_seconds: minutesToSeconds(document.getElementById('minDurationMinutes').value),
        max_duration_seconds: minutesToSeconds(document.getElementById('maxDurationMinutes').value),
        skip_shorts: document.getElementById('skipShorts').checked,
        skip_livestreams: document.getElementById('skipLivestreams').checked,
        sponsorblock_categories: []
    };

    // Get selected SponsorBlock categories
    const checkboxes = document.querySelectorAll('#sponsorblockGroup input[type="checkbox"]:checked');
    formData.sponsorblock_categories = Array.from(checkboxes).map(cb => cb.value);

    return formData;
}

// API calls
async function handleSubmit(event) {
    event.preventDefault();
    
    const formData = getFormData();
    
    // Validate required fields
    if (!formData.display_name || !formData.url) {
        showToast('❌ Please fill in all required fields');
        return;
    }
    
    try {
        let response;
        if (isEditMode) {
            response = await fetch(`/api/channels/${currentChannelName}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(formData)
            });
        } else {
            response = await fetch('/api/channels', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(formData)
            });
        }
        
        const result = await response.json();
        
        if (response.ok) {
            showToast(isEditMode ? '✅ Channel updated successfully!' : '✅ Channel added successfully!');
            closeChannelModal();
            setTimeout(() => {
                window.location.reload();
            }, 1000);
        } else {
            showToast(`❌ ${result.error || 'Unknown error occurred'}`);
        }
    } catch (error) {
        console.error('Error:', error);
        showToast('❌ Network error occurred');
    }
}

async function deleteChannel(channelName) {
    if (!confirm(`Are you sure you want to delete the channel "${channelName}"? This action cannot be undone.`)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/channels/${channelName}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showToast('✅ Channel deleted successfully!');
            setTimeout(() => {
                window.location.reload();
            }, 1000);
        } else {
            showToast(`❌ ${result.error || 'Failed to delete channel'}`);
        }
    } catch (error) {
        console.error('Error:', error);
        showToast('❌ Network error occurred');
    }
}

async function purgeEpisodes() {
    if (!currentChannelName) {
        showToast('❌ No channel selected');
        return;
    }
    
    if (!confirm(`Are you sure you want to purge all downloaded episodes for "${currentChannelName}"? This will delete all video files and the RSS feed will be regenerated on next refresh. This action cannot be undone.`)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/channels/${currentChannelName}/purge`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showToast('✅ Episodes purged successfully!');
            closeChannelModal();
            setTimeout(() => {
                window.location.reload();
            }, 1000);
        } else {
            showToast(`❌ ${result.error || 'Failed to purge episodes'}`);
        }
    } catch (error) {
        console.error('Error:', error);
        showToast('❌ Network error occurred');
    }
}

async function deleteChannelFromDangerZone() {
    if (!currentChannelName) {
        showToast('❌ No channel selected');
        return;
    }

    await deleteChannel(currentChannelName);
}

// Tombstones: manually deleted episodes remembered so refreshes skip them.
async function refreshTombstoneRow() {
    const row = document.getElementById('tombstoneRow');
    if (!row || !currentChannelName) return;
    try {
        const response = await fetch(`/api/channels/${encodeURIComponent(currentChannelName)}/tombstones`);
        if (!response.ok) {
            row.style.display = 'none';
            return;
        }
        const data = await response.json();
        const count = data.total_count || 0;
        document.getElementById('tombstoneCount').textContent = count;
        row.style.display = count > 0 ? 'block' : 'none';
    } catch (error) {
        console.error('Error loading remembered deletions:', error);
        row.style.display = 'none';
    }
}

async function clearTombstones() {
    if (!currentChannelName) {
        showToast('❌ No channel selected');
        return;
    }

    if (!confirm('Forget all remembered deletions for this channel?\n\nThe next refresh may download episodes you deleted manually again.')) {
        return;
    }

    try {
        const response = await fetch(`/api/channels/${encodeURIComponent(currentChannelName)}/tombstones`, {
            method: 'DELETE'
        });
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.error || 'Failed to forget remembered deletions');
        }
        showToast('🧹 Remembered deletions forgotten');
        refreshTombstoneRow();
    } catch (error) {
        console.error('Error forgetting remembered deletions:', error);
        showToast('❌ ' + (error.message || 'Failed to forget remembered deletions'));
    }
}

// Scheduler form submission
async function handleScheduleSubmit(event) {
    event.preventDefault();

    const refreshInterval = parseInt(document.getElementById('refreshInterval').value);

    if (!refreshInterval || refreshInterval < 1 || refreshInterval > 168) {
        showToast('❌ Refresh interval must be between 1 and 168 hours');
        return;
    }

    const storagePayload = {
        global_quota_gb: parseFloat(document.getElementById('globalQuota').value) || 0,
        min_free_space_gb: parseFloat(document.getElementById('minFreeSpace').value),
        retention_days: parseInt(document.getElementById('retentionDays').value) || 0,
    };
    if (!Number.isFinite(storagePayload.min_free_space_gb) || storagePayload.min_free_space_gb < 0) {
        showToast('❌ Minimum free space must be a non-negative number');
        return;
    }

    try {
        const intervalResponse = await fetch('/api/config/refresh-interval', {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ refresh_interval_hours: refreshInterval })
        });
        const intervalResult = await intervalResponse.json();
        if (!intervalResponse.ok) {
            throw new Error(intervalResult.error || 'Failed to update refresh interval');
        }

        const storageResponse = await fetch('/api/config/storage', {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(storagePayload)
        });
        if (!storageResponse.ok) {
            const storageResult = await storageResponse.json();
            throw new Error(storageResult.error || 'Failed to update storage settings');
        }

        showToast('✅ Settings updated successfully!');

        // Update the display
        document.getElementById('currentInterval').textContent = `${refreshInterval} hours`;
        const nextRun = intervalResult.next_run ? formatDateTime(intervalResult.next_run) : 'Not scheduled';
        document.getElementById('nextRun').textContent = nextRun;

        closeScheduleModal();
    } catch (error) {
        console.error('Error updating schedule settings:', error);
        showToast(`❌ ${error.message || 'Network error occurred'}`);
    }
}

// Date formatting utility
function formatDateTime(isoString) {
    try {
        const date = new Date(isoString);
        return date.toLocaleString();
    } catch (error) {
        return 'Invalid date';
    }
}

// Event listeners
document.getElementById('channelForm').addEventListener('submit', handleSubmit);
document.getElementById('scheduleForm').addEventListener('submit', handleScheduleSubmit);

// Episodes Modal Functions
async function openEpisodesModal(channelName, displayName) {
    try {
        const modal = document.getElementById('episodesModal');
        const titleElement = document.getElementById('episodesModalTitle');
        const contentElement = document.getElementById('episodesContent');
        
        // Set title and show modal
        titleElement.textContent = `${displayName} - Episodes`;
        modal.style.display = 'block';
        
        // Show loading state
        contentElement.innerHTML = `
            <div class="episodes-loading">
                <div class="spinner"></div>
                Loading episodes...
            </div>
        `;
        
        // Fetch episodes data
        const response = await fetch(`/api/channels/${channelName}/episodes`);
        const data = await response.json();
        
        if (response.ok) {
            if (data.episodes && data.episodes.length > 0) {
                // Bind actions directly: titles and metadata are text, never JavaScript.
                const list = document.createElement('div');
                list.className = 'episodes-list';
                for (const episode of data.episodes) {
                    const item = document.createElement('div');
                    item.className = 'episode-item';
                    const content = document.createElement('div');
                    content.className = 'episode-content';
                    const header = document.createElement('div');
                    header.className = 'episode-header';
                    const title = document.createElement('h3');
                    title.className = 'episode-title';
                    title.textContent = episode.title;
                    const meta = document.createElement('div');
                    meta.className = 'episode-meta';
                    for (const [className, value] of [['episode-date', episode.date], ['episode-duration', episode.duration]]) {
                        const field = document.createElement('div');
                        field.className = className;
                        field.textContent = value;
                        meta.append(field);
                    }
                    header.append(title, meta);
                    if (episode.pinned) {
                        const badge = document.createElement('span');
                        badge.className = 'pin-badge';
                        badge.textContent = '📌';
                        header.append(badge);
                    }
                    const description = document.createElement('div');
                    description.className = 'episode-description';
                    description.textContent = episode.description;
                    content.append(header, description);
                    const actions = document.createElement('div');
                    actions.className = 'episode-actions';
                    const extension = episode.file_extension || '.mp4';
                    const definitions = [
                        ['play-btn', '▶️ Play', () => openEpisodeMedia(channelName, episode.id, extension)],
                        ['download-audio-btn', '🎵 Download Audio', button => downloadAsMP3(channelName, episode.id, extension, button)],
                        ['download-original-btn', '📁 Download Original', button => downloadOriginalFile(channelName, episode.id, extension, button)],
                        ['redownload-btn', '🔄 Redownload', button => redownloadEpisode(channelName, episode.id, episode.title, button)],
                        ['delete-btn', '🗑️ Delete', button => deleteEpisode(channelName, episode.id, episode.title, button)],
                        ['pin-btn', episode.pinned ? '📍 Unpin' : '📌 Pin', button => togglePin(channelName, episode.id, Boolean(episode.pinned), button)],
                    ];
                    for (const [className, label, action] of definitions) {
                        const button = document.createElement('button');
                        button.className = className;
                        button.textContent = label;
                        button.addEventListener('click', () => action(button));
                        actions.append(button);
                    }
                    item.append(content, actions);
                    list.append(item);
                }
                contentElement.replaceChildren(list);
            } else {
                // No episodes found
                contentElement.innerHTML = `
                    <div class="episodes-empty">
                        <h3>No Episodes Found</h3>
                        <p>This channel doesn't have any downloaded episodes yet. Try refreshing the podcasts to download some content.</p>
                    </div>
                `;
            }
        } else {
            // Error occurred
            contentElement.innerHTML = `
                <div class="episodes-empty">
                    <h3>Error Loading Episodes</h3>
                    <p>${escapeHtml(data.error || 'Failed to load episodes')}</p>
                </div>
            `;
        }
        
    } catch (error) {
        console.error('Error opening episodes modal:', error);
        const contentElement = document.getElementById('episodesContent');
        contentElement.innerHTML = `
            <div class="episodes-empty">
                <h3>Network Error</h3>
                <p>Failed to connect to the server. Please try again.</p>
            </div>
        `;
    }
}

function closeEpisodesModal() {
    const modal = document.getElementById('episodesModal');
    modal.style.display = 'none';
}

function openEpisodeMedia(channelName, episodeId, fileExtension) {
    // Construct the media URL
    const mediaUrl = `/podcasts/${encodeURIComponent(channelName)}/${encodeURIComponent(episodeId)}${fileExtension}`;
    
    // Open in new tab
    window.open(mediaUrl, '_blank');
}

async function downloadAsMP3(channelName, episodeId, fileExtension, button) {
    const originalText = button.textContent;
    
    try {
        // Show loading state
        button.textContent = '⏳ Converting...';
        button.disabled = true;
        
        // Make request to conversion endpoint
        const response = await fetch(`/api/channels/${encodeURIComponent(channelName)}/episodes/${encodeURIComponent(episodeId)}/download-mp3`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Conversion failed');
        }
        
        // Create download link
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        
        // Get filename from Content-Disposition header or default based on original extension
        const contentDisposition = response.headers.get('content-disposition') || response.headers.get('Content-Disposition');
        let filename = `${episodeId}.mp3`; // default
        if (contentDisposition) {
            console.log('Content-Disposition header:', contentDisposition);
            // Try different filename patterns
            const filenameMatch = contentDisposition.match(/filename[*]?=['"]?([^'";]+)['"]?/i);
            if (filenameMatch && filenameMatch[1]) {
                filename = filenameMatch[1];
                console.log('Extracted filename:', filename);
            }
        }
        console.log('Final download filename:', filename);
        
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        // Show success state briefly
        button.textContent = '✅ Downloaded!';
        setTimeout(() => {
            button.textContent = originalText;
            button.disabled = false;
        }, 2000);
        
    } catch (error) {
        console.error('Error downloading MP3:', error);
        button.textContent = '❌ Failed';
        setTimeout(() => {
            button.textContent = originalText;
            button.disabled = false;
        }, 3000);
    }
}

async function downloadOriginalFile(channelName, episodeId, fileExtension, button) {
    const originalText = button.textContent;
    
    try {
        // Show loading state
        button.textContent = '⏳ Downloading...';
        button.disabled = true;
        
        // Make request to download endpoint
        const response = await fetch(`/api/channels/${encodeURIComponent(channelName)}/episodes/${encodeURIComponent(episodeId)}/download-original`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Download failed');
        }
        
        // Create download link
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        
        // Get filename from Content-Disposition header or fallback
        const contentDisposition = response.headers.get('content-disposition') || response.headers.get('Content-Disposition');
        let filename = `${episodeId}${fileExtension}`; // fallback
        if (contentDisposition) {
            console.log('Content-Disposition header:', contentDisposition);
            // Try different filename patterns
            const filenameMatch = contentDisposition.match(/filename[*]?=['"]?([^'";]+)['"]?/i);
            if (filenameMatch && filenameMatch[1]) {
                filename = filenameMatch[1];
                console.log('Extracted filename:', filename);
            }
        }
        console.log('Final download filename:', filename);
        
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        // Show success state briefly
        button.textContent = '✅ Downloaded!';
        setTimeout(() => {
            button.textContent = originalText;
            button.disabled = false;
        }, 2000);
        
    } catch (error) {
        console.error('Error downloading original file:', error);
        button.textContent = '❌ Failed';
        setTimeout(() => {
            button.textContent = originalText;
            button.disabled = false;
        }, 3000);
    }
}

async function deleteEpisode(channelName, episodeId, episodeTitle, button) {
    const originalText = button.textContent;

    // Confirm deletion
    if (!confirm(`Are you sure you want to delete "${episodeTitle}"?\n\nThis action cannot be undone.`)) {
        return;
    }

    try {
        // Show loading state
        button.textContent = '⏳ Deleting...';
        button.disabled = true;

        // Make request to delete endpoint
        const response = await fetch(`/api/channels/${encodeURIComponent(channelName)}/episodes/${encodeURIComponent(episodeId)}/delete`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
            }
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || 'Delete failed');
        }

        // Show success state briefly
        button.textContent = '✅ Deleted!';

        // Remove the episode from the UI after a short delay
        setTimeout(() => {
            const episodeItem = button.closest('.episode-item');
            if (episodeItem) {
                episodeItem.style.transition = 'opacity 0.3s ease-out';
                episodeItem.style.opacity = '0';
                setTimeout(() => {
                    episodeItem.remove();

                    // Check if there are no more episodes
                    const episodesList = document.querySelector('.episodes-list');
                    if (episodesList && episodesList.children.length === 0) {
                        document.getElementById('episodesContent').innerHTML = `
                            <div class="episodes-empty">
                                <h3>No Episodes Found</h3>
                                <p>This channel doesn't have any downloaded episodes yet. Try refreshing the podcasts to download some content.</p>
                            </div>
                        `;
                    }
                }, 300);
            }
        }, 1000);

    } catch (error) {
        console.error('Error deleting episode:', error);
        button.textContent = '❌ Failed';
        setTimeout(() => {
            button.textContent = originalText;
            button.disabled = false;
        }, 3000);
    }
}

async function redownloadEpisode(channelName, episodeId, episodeTitle, button) {
    const originalText = button.textContent;

    // Confirm redownload
    if (!confirm(`Redownload "${episodeTitle}"?\n\nThe current file will be replaced after the download succeeds.`)) {
        return;
    }

    try {
        // Show loading state
        button.textContent = '⏳ Redownloading...';
        button.disabled = true;

        // Make request to redownload endpoint
        const response = await fetch(`/api/channels/${encodeURIComponent(channelName)}/episodes/${encodeURIComponent(episodeId)}/redownload`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || 'Redownload failed');
        }

        // Show success state briefly
        button.textContent = '✅ Redownloaded!';
        setTimeout(() => {
            button.textContent = originalText;
            button.disabled = false;
        }, 2000);

    } catch (error) {
        console.error('Error redownloading episode:', error);
        button.textContent = '❌ Failed';
        setTimeout(() => {
            button.textContent = originalText;
            button.disabled = false;
        }, 3000);
    }
}

async function togglePin(channelName, episodeId, pinned, button) {
    const originalText = button.textContent;

    try {
        button.textContent = '⏳ Saving...';
        button.disabled = true;

        const response = await fetch(`/api/channels/${encodeURIComponent(channelName)}/episodes/${encodeURIComponent(episodeId)}/pin`, {
            method: pinned ? 'DELETE' : 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || 'Pin toggle failed');
        }

        // Flip the button and the badge in place so the list needs no reload.
        button.textContent = pinned ? '📌 Pin' : '📍 Unpin';
        button.disabled = false;
        const item = button.closest('.episode-item');
        const badge = item ? item.querySelector('.pin-badge') : null;
        if (!pinned && !badge && item) {
            const header = item.querySelector('.episode-header');
            const marker = document.createElement('span');
            marker.className = 'pin-badge';
            marker.textContent = '📌';
            header.append(marker);
        } else if (pinned && badge) {
            badge.remove();
        }
        showToast(pinned ? '📌 Episode unpinned' : '📌 Episode pinned — protected from cleanup');
    } catch (error) {
        console.error('Error toggling pin:', error);
        button.textContent = originalText;
        button.disabled = false;
        showToast('❌ ' + (error.message || 'Pin toggle failed'));
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Close modal when clicking outside
window.onclick = function(event) {
    const channelModal = document.getElementById('channelModal');
    const scheduleModal = document.getElementById('scheduleModal');
    const episodesModal = document.getElementById('episodesModal');
    
    if (event.target === channelModal) {
        closeChannelModal();
    } else if (event.target === scheduleModal) {
        closeScheduleModal();
    } else if (event.target === episodesModal) {
        closeEpisodesModal();
    }
}

// Close modal with Escape key
document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape') {
        closeChannelModal();
        closeScheduleModal();
        closeEpisodesModal();
    }
});

// Refresh functionality
async function triggerRefresh() {
    try {
        const response = await fetch('/api/refresh', {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showToast('✅ Refresh started successfully!');
            startRefreshMonitoring();
        } else {
            showToast(`❌ ${result.error || 'Failed to start refresh'}`);
        }
    } catch (error) {
        console.error('Error:', error);
        showToast('❌ Network error occurred');
    }
}

async function checkRefreshStatus() {
    try {
        const response = await fetch('/api/refresh/status');
        const status = await response.json();
        
        updateRefreshButton(status);
        updateLogDisplay(status.logs || []);
        
        if (!status.running && refreshInterval) {
            clearInterval(refreshInterval);
            refreshInterval = null;
            showToast('✅ Refresh completed!');
            
            // Reload page after 2 seconds to show updated data
            setTimeout(() => {
                window.location.reload();
            }, 2000);
        }
        
    } catch (error) {
        console.error('Error checking refresh status:', error);
    }
}

function updateRefreshButton(status) {
    const refreshBtn = document.getElementById('refreshBtn');
    
    if (status.running) {
        refreshBtn.disabled = true;
        refreshBtn.classList.add('refreshing');
        refreshBtn.innerHTML = `<span class="spinner"></span>Refresh in progress (${status.duration}s)`;
    } else {
        refreshBtn.disabled = false;
        refreshBtn.classList.remove('refreshing');
        refreshBtn.innerHTML = '🔄 Refresh Podcasts';
    }
}

function updateLogDisplay(logs) {
    const logContainer = document.getElementById('refreshLogs');
    if (!logContainer) return;
    
    if (logs.length === 0) {
        logContainer.style.display = 'none';
        return;
    }
    
    // Show log container but keep content collapsed by default
    logContainer.style.display = 'block';
    const logContent = document.getElementById('refreshLogContent');
    
    // Convert logs to HTML, showing most recent at top
    const logsHtml = logs.slice(-20).reverse().map(log => {
        const timestamp = new Date(log.timestamp).toLocaleTimeString();
        const message = escapeHtml(log.message);
        return `<div class="log-entry">[${timestamp}] ${message}</div>`;
    }).join('');
    
    logContent.innerHTML = logsHtml;
    
    // Auto-scroll to top to show latest log (only if expanded)
    if (logContent.style.display !== 'none') {
        logContent.scrollTop = 0;
    }
}

function toggleLogDisplay() {
    const logContainer = document.getElementById('refreshLogs');
    const logContent = document.getElementById('refreshLogContent');
    const toggleBtn = document.querySelector('.logs-collapse');
    
    if (logContent.style.display === 'none' || logContent.style.display === '') {
        logContent.style.display = 'block';
        toggleBtn.textContent = '−';
        // Scroll to top when opening
        logContent.scrollTop = 0;
    } else {
        logContent.style.display = 'none';
        toggleBtn.textContent = '+';
    }
}

function startRefreshMonitoring() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
    }

    refreshInterval = setInterval(checkRefreshStatus, 1000); // Check every second
    checkRefreshStatus(); // Check immediately
}

// Download queue panel
let queuePollTimer = null;
let queuePanelExpanded = false;

function formatSpeed(bytesPerSecond) {
    if (typeof bytesPerSecond !== 'number' || bytesPerSecond <= 0) return null;
    if (bytesPerSecond >= 1024 * 1024) {
        return (bytesPerSecond / (1024 * 1024)).toFixed(1) + ' MB/s';
    }
    return (bytesPerSecond / 1024).toFixed(0) + ' KB/s';
}

function formatEta(etaSeconds) {
    if (typeof etaSeconds !== 'number' || etaSeconds < 0) return null;
    const minutes = Math.floor(etaSeconds / 60);
    const seconds = Math.floor(etaSeconds % 60);
    if (minutes >= 60) {
        const hours = Math.floor(minutes / 60);
        return `${hours}h ${minutes % 60}m`;
    }
    if (minutes > 0) return `${minutes}m ${seconds.toString().padStart(2, '0')}s`;
    return `${seconds}s`;
}

async function refreshQueuePanel() {
    let data;
    try {
        const response = await fetch('/api/queue');
        if (response.status === 401) {
            // Authentication required; the panel stays hidden until login.
            renderQueue(null);
            return;
        }
        if (!response.ok) return;
        data = await response.json();
    } catch (error) {
        console.error('Error fetching download queue:', error);
        return;
    }
    renderQueue(data);
}

function renderQueue(data) {
    const panel = document.getElementById('downloadQueue');
    if (!panel) return;

    const jobs = data ? data.jobs || [] : [];
    const visible = jobs.filter(job =>
        job.state === 'running' || job.state === 'pending' || job.state === 'failed'
    );
    if (visible.length === 0) {
        panel.style.display = 'none';
        return;
    }

    const summary = (data && data.summary) || {};
    const summaryParts = [];
    if (summary.running) summaryParts.push(`${summary.running} running`);
    if (summary.pending) summaryParts.push(`${summary.pending} pending`);
    if (summary.failed) summaryParts.push(`${summary.failed} failed`);
    if (summary.completed) summaryParts.push(`${summary.completed} completed`);
    const summaryElement = document.getElementById('queueSummary');
    if (summaryElement) summaryElement.textContent = summaryParts.join(' · ');

    panel.style.display = 'block';
    const content = document.getElementById('queueContent');
    content.style.display = queuePanelExpanded ? 'block' : 'none';
    if (!queuePanelExpanded) return;

    const running = visible.filter(job => job.state === 'running');
    const pending = visible.filter(job => job.state === 'pending');
    const failed = visible.filter(job => job.state === 'failed');
    const list = document.createElement('div');
    list.className = 'queue-list';
    for (const job of [...running, ...pending, ...failed]) {
        list.append(buildQueueItem(job));
    }
    content.replaceChildren(list);
}

function buildQueueItem(job) {
    const item = document.createElement('div');
    item.className = 'queue-item queue-item-' + job.state;

    const header = document.createElement('div');
    header.className = 'queue-item-header';
    const title = document.createElement('span');
    title.className = 'queue-item-title';
    title.textContent = job.title || job.video_id;
    const stateTag = document.createElement('span');
    stateTag.className = 'queue-item-state';
    stateTag.textContent = job.state;
    header.append(title, stateTag);
    item.append(header);

    const metaParts = [job.channel];
    if (job.state === 'running' && job.progress) {
        const speed = formatSpeed(job.progress.speed);
        if (speed) metaParts.push(speed);
        const eta = formatEta(job.progress.eta);
        if (eta) metaParts.push('ETA ' + eta);
    } else if (job.state === 'pending' && job.attempts > 0) {
        metaParts.push(`retrying (attempt ${job.attempts}/${job.max_attempts})`);
    }
    const meta = document.createElement('div');
    meta.className = 'queue-item-meta';
    meta.textContent = metaParts.filter(Boolean).join(' · ');
    item.append(meta);

    if (job.error && (job.state === 'failed' || job.state === 'pending')) {
        const error = document.createElement('div');
        error.className = 'queue-item-error';
        error.textContent = '❌ ' + job.error;
        item.append(error);
    }

    if (job.state === 'running') {
        const percent = job.progress && typeof job.progress.percent === 'number'
            ? job.progress.percent
            : 0;
        const bar = document.createElement('div');
        bar.className = 'queue-progress';
        const fill = document.createElement('div');
        fill.className = 'queue-progress-fill';
        fill.style.width = Math.min(100, Math.max(0, percent)) + '%';
        const label = document.createElement('span');
        label.className = 'queue-progress-text';
        label.textContent = Math.round(percent) + '%';
        bar.append(fill, label);
        item.append(bar);
    }

    const actions = document.createElement('div');
    actions.className = 'queue-item-actions';
    if (job.state === 'running' || job.state === 'pending') {
        actions.append(queueActionButton('✖️ Cancel', () => cancelQueueJob(job.id)));
    }
    if (job.state === 'failed') {
        actions.append(queueActionButton('🔄 Retry', () => retryQueueJob(job.id)));
        actions.append(queueActionButton('🗑️ Dismiss', () => dismissQueueJob(job.id)));
    }
    if (actions.children.length > 0) item.append(actions);
    return item;
}

function queueActionButton(label, handler) {
    const button = document.createElement('button');
    button.className = 'queue-action-btn';
    button.textContent = label;
    button.addEventListener('click', () => handler(button));
    return button;
}

async function queueAction(url, successMessage) {
    try {
        const response = await fetch(url, { method: 'POST' });
        if (!response.ok) {
            const data = await response.json();
            throw new Error(data.error || 'Queue action failed');
        }
        showToast(successMessage);
        await refreshQueuePanel();
    } catch (error) {
        console.error('Queue action failed:', error);
        showToast('❌ ' + (error.message || 'Queue action failed'));
    }
}

function cancelQueueJob(jobId) {
    return queueAction(`/api/queue/${encodeURIComponent(jobId)}/cancel`, '🛑 Job cancelled');
}

function retryQueueJob(jobId) {
    return queueAction(`/api/queue/${encodeURIComponent(jobId)}/retry`, '🔄 Job requeued');
}

function dismissQueueJob(jobId) {
    return queueAction(`/api/queue/${encodeURIComponent(jobId)}/dismiss`, '🗑️ Job dismissed');
}

function clearCompletedJobs(event) {
    if (event) event.stopPropagation();
    return queueAction('/api/queue/clear-completed', '🧹 Completed jobs cleared');
}

function toggleQueuePanel() {
    queuePanelExpanded = !queuePanelExpanded;
    const content = document.getElementById('queueContent');
    const toggleBtn = document.querySelector('#downloadQueue .logs-collapse');
    if (content) content.style.display = queuePanelExpanded ? 'block' : 'none';
    if (toggleBtn) toggleBtn.textContent = queuePanelExpanded ? '−' : '+';
    if (queuePanelExpanded) refreshQueuePanel();
}

function startQueuePolling() {
    if (queuePollTimer) return;
    refreshQueuePanel();
    queuePollTimer = setInterval(refreshQueuePanel, 2000);
}

// Storage panel
let storagePanelExpanded = false;
const BYTES_PER_GB = 1024 * 1024 * 1024;

function formatGb(bytes) {
    if (typeof bytes !== 'number' || bytes < 0) return '—';
    if (bytes >= BYTES_PER_GB) {
        return (bytes / BYTES_PER_GB).toFixed(bytes >= 10 * BYTES_PER_GB ? 1 : 2) + ' GB';
    }
    return (bytes / (1024 * 1024)).toFixed(0) + ' MB';
}

async function refreshStoragePanel() {
    let data;
    try {
        const response = await fetch('/api/storage');
        if (response.status === 401) {
            // Authentication required; the panel stays hidden until login.
            renderStorage(null);
            return;
        }
        if (!response.ok) return;
        data = await response.json();
    } catch (error) {
        console.error('Error fetching storage info:', error);
        return;
    }
    renderStorage(data);
}

function storageRow(label, usedBytes, quotaGb, extra, over) {
    const row = document.createElement('div');
    row.className = 'storage-row';
    const name = document.createElement('span');
    name.className = 'storage-label';
    name.textContent = label;
    const track = document.createElement('div');
    track.className = 'storage-bar-track';
    const fill = document.createElement('div');
    fill.className = 'storage-bar-fill' + (over ? ' over' : '');
    let percent = null;
    if (quotaGb > 0) {
        percent = quotaGb * BYTES_PER_GB > 0 ? (usedBytes / (quotaGb * BYTES_PER_GB)) * 100 : 0;
        fill.style.width = Math.min(100, Math.max(0, percent)) + '%';
    }
    track.append(fill);
    const text = document.createElement('span');
    text.className = 'storage-text';
    const quotaText = quotaGb > 0 ? ` / ${quotaGb} GB` : '';
    text.textContent = `${formatGb(usedBytes)}${quotaText}${extra ? ' · ' + extra : ''}`;
    row.append(name, track, text);
    return { row, percent };
}

function renderStorage(data) {
    const panel = document.getElementById('storagePanel');
    if (!panel) return;
    if (!data) {
        panel.style.display = 'none';
        return;
    }

    const settings = data.settings || {};
    const channels = data.channels || [];
    const minFreeGb = settings.min_free_space_gb || 0;
    const lowFree = typeof data.free_bytes === 'number'
        && minFreeGb > 0
        && data.free_bytes < minFreeGb * BYTES_PER_GB;
    const anyQuota = (settings.global_quota_gb || 0) > 0
        || channels.some(channel => channel.quota_overridden);
    const anyRetention = (settings.retention_days || 0) > 0
        || channels.some(channel => channel.retention_overridden);
    const anyOver = channels.some(channel => channel.over_quota);
    const totals = data.totals || {};

    const summary = document.getElementById('storageSummary');
    if (summary) {
        const parts = [`${formatGb(totals.used_bytes)} used`, `${formatGb(data.free_bytes)} free`];
        if (lowFree) parts.push('⚠️ low space');
        summary.textContent = parts.join(' · ');
    }

    // Stay hidden until something storage-related deserves attention
    // (or the user expanded the panel manually).
    if (!storagePanelExpanded && !anyQuota && !anyRetention && !lowFree && !anyOver) {
        panel.style.display = 'none';
        return;
    }

    panel.style.display = 'block';
    const content = document.getElementById('storageContent');
    content.style.display = storagePanelExpanded ? 'block' : 'none';
    if (!storagePanelExpanded) return;

    const list = document.createElement('div');
    list.className = 'storage-list';
    const globalQuota = settings.global_quota_gb || 0;
    list.append(storageRow('Total media', totals.used_bytes || 0, globalQuota,
        `${totals.episodes || 0} episodes · ${totals.pinned || 0} 📌`, anyOver).row);

    const freeRow = document.createElement('div');
    freeRow.className = 'storage-row' + (lowFree ? ' storage-warning' : '');
    const freeLabel = document.createElement('span');
    freeLabel.className = 'storage-label';
    freeLabel.textContent = 'Free space';
    const freeTrack = document.createElement('div');
    freeTrack.className = 'storage-bar-track';
    const freeText = document.createElement('span');
    freeText.className = 'storage-text';
    freeText.textContent = lowFree
        ? `${formatGb(data.free_bytes)} — below ${minFreeGb} GB minimum`
        : formatGb(data.free_bytes);
    freeRow.append(freeLabel, freeTrack, freeText);
    list.append(freeRow);

    for (const channel of channels) {
        if (!channel.quota_overridden) continue;
        list.append(storageRow(
            channel.display_name || channel.name,
            channel.used_bytes,
            channel.quota_gb,
            `${channel.episodes} episodes · ${channel.pinned} 📌${channel.over_quota ? ' · over quota' : ''}`,
            channel.over_quota,
        ).row);
    }
    content.replaceChildren(list);
}

function toggleStoragePanel() {
    storagePanelExpanded = !storagePanelExpanded;
    const content = document.getElementById('storageContent');
    const toggleBtn = document.querySelector('#storagePanel .logs-collapse');
    if (content) content.style.display = storagePanelExpanded ? 'block' : 'none';
    if (toggleBtn) toggleBtn.textContent = storagePanelExpanded ? '−' : '+';
    if (storagePanelExpanded) refreshStoragePanel();
}

// Relative time formatting
function formatRelativeTime(isoTimestamp) {
    try {
        const date = new Date(isoTimestamp);
        const now = new Date();
        const diffMs = now - date;
        
        // Convert to different units
        const diffSeconds = Math.floor(diffMs / 1000);
        const diffMinutes = Math.floor(diffSeconds / 60);
        const diffHours = Math.floor(diffMinutes / 60);
        const diffDays = Math.floor(diffHours / 24);
        const diffWeeks = Math.floor(diffDays / 7);
        const diffMonths = Math.floor(diffDays / 30);
        const diffYears = Math.floor(diffDays / 365);
        
        // Return appropriate format
        if (diffSeconds < 60) {
            return 'just now';
        } else if (diffMinutes < 60) {
            return `${diffMinutes}m ago`;
        } else if (diffHours < 24) {
            return `${diffHours}h ago`;
        } else if (diffDays < 7) {
            return `${diffDays}d ago`;
        } else if (diffWeeks < 4) {
            return `${diffWeeks}w ago`;
        } else if (diffMonths < 12) {
            return `${diffMonths}mo ago`;
        } else {
            return `${diffYears}y ago`;
        }
    } catch (error) {
        console.error('Error formatting relative time:', error);
        return 'unknown';
    }
}

function updateRelativeTimes() {
    const refreshTags = document.querySelectorAll('.last-refresh-tag[data-timestamp]');
    refreshTags.forEach(tag => {
        const timestamp = tag.getAttribute('data-timestamp');
        if (timestamp) {
            tag.textContent = 'Refreshed: ' + formatRelativeTime(timestamp);
        }
    });
}

// Tooltip positioning logic
function adjustTooltipPositions() {
    const tooltips = document.querySelectorAll('.tooltip');
    const modal = document.querySelector('.modal-content');
    
    if (!modal) return;
    
    tooltips.forEach(tooltip => {
        const rect = tooltip.getBoundingClientRect();
        const modalRect = modal.getBoundingClientRect();
        
        // Reset classes
        tooltip.classList.remove('tooltip-left', 'tooltip-right');
        
        // Check if tooltip would overflow to the right
        const tooltipContent = tooltip.querySelector('.tooltip-content');
        if (tooltipContent) {
            const tooltipWidth = 260; // Default tooltip width
            const tooltipCenter = rect.left + (rect.width / 2);
            const tooltipLeft = tooltipCenter - (tooltipWidth / 2);
            const tooltipRight = tooltipCenter + (tooltipWidth / 2);
            
            // If tooltip would overflow right edge of modal
            if (tooltipRight > modalRect.right - 20) {
                tooltip.classList.add('tooltip-right');
            }
            // If tooltip would overflow left edge of modal
            else if (tooltipLeft < modalRect.left + 20) {
                tooltip.classList.add('tooltip-left');
            }
        }
    });
}

// Single channel refresh function
async function refreshSingleChannel(channelName, button) {
    const originalText = button.innerHTML;
    
    try {
        // Update button to show loading state
        button.disabled = true;
        button.classList.add('refreshing');
        
        const response = await fetch(`/api/channels/${channelName}/refresh`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showToast(`✅ Refresh started for ${channelName}`);
            
            // Start monitoring single channel refresh completion
            const startTime = Date.now();
            const monitorInterval = setInterval(async () => {
                try {
                    const statusResponse = await fetch('/api/refresh/status');
                    const status = await statusResponse.json();
                    
                    // Update log display during single channel refresh
                    updateLogDisplay(status.logs || []);
                    
                    // Check if refresh has completed
                    if (!status.running) {
                        clearInterval(monitorInterval);
                        button.disabled = false;
                        button.classList.remove('refreshing');
                        showToast(`✅ ${channelName} refresh completed!`);
                        
                        // Reload page after 2 seconds to show updated data
                        setTimeout(() => {
                            window.location.reload();
                        }, 2000);
                    } else {
                        // Show periodic updates via global notifications
                        const duration = Math.floor((Date.now() - startTime) / 1000);
                        if (duration % 30 === 0 && duration > 0) { // Every 30 seconds
                            showToast(`⏳ ${channelName} still refreshing (${duration}s)...`);
                        }
                    }
                } catch (error) {
                    console.error('Error checking refresh status:', error);
                    // Continue monitoring despite error
                }
            }, 1000); // Check every second
            
            // Safety timeout to reset button after 10 minutes
            setTimeout(() => {
                clearInterval(monitorInterval);
                if (button.disabled) {
                    button.disabled = false;
                    button.classList.remove('refreshing');
                    showToast(`⚠️ ${channelName} refresh monitoring timed out`);
                }
            }, 600000); // 10 minutes
            
        } else {
            showToast(`❌ ${result.error || 'Failed to start refresh for ' + channelName}`);
            button.disabled = false;
            button.classList.remove('refreshing');
        }
    } catch (error) {
        console.error('Error:', error);
        showToast(`❌ Network error occurred while refreshing ${channelName}`);
        button.disabled = false;
        button.classList.remove('refreshing');
    }
}

// ---------- Health monitoring and notifications ----------

let healthPollTimer = null;

function toggleHealthPanel() {
    const content = document.getElementById('healthContent');
    const toggle = document.getElementById('healthToggle');
    if (!content) return;
    const visible = content.style.display !== 'none';
    content.style.display = visible ? 'none' : 'block';
    if (toggle) toggle.textContent = visible ? '+' : '−';
}

function formatHealthTime(value) {
    if (!value) return 'never';
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
}

function healthRow(channelName, state) {
    const row = document.createElement('div');
    const failures = Number(state.consecutive_failures || 0);
    row.className = 'health-row ' + (state.unavailable ? 'unavailable' : failures > 0 ? 'degraded' : 'healthy');

    const name = document.createElement('span');
    name.className = 'health-name';
    name.textContent = channelName;

    const status = document.createElement('span');
    status.className = 'health-status';
    if (state.unavailable) {
        status.textContent = 'unavailable';
    } else if (failures > 0) {
        status.textContent = `${failures} consecutive failure(s)`;
    } else {
        status.textContent = 'healthy';
    }

    const detail = document.createElement('span');
    detail.className = 'health-detail';
    const parts = [`last success: ${formatHealthTime(state.last_success_at)}`];
    if (state.last_failure_at) {
        parts.push(`last failure: ${formatHealthTime(state.last_failure_at)}`);
    }
    detail.textContent = parts.join(' · ');
    if (state.last_error) {
        const error = document.createElement('span');
        error.className = 'health-error';
        error.textContent = state.last_error;
        detail.appendChild(error);
    }

    row.append(name, status, detail);
    return row;
}

async function refreshHealthPanel() {
    const panel = document.getElementById('healthPanel');
    const grid = document.getElementById('healthChannels');
    if (!panel || !grid) return;
    try {
        const response = await fetch('/api/health');
        if (!response.ok) return;
        const data = await response.json();
        const states = data.channels || {};
        const names = Object.keys(states).sort();
        const issues = (data.pending_events || []).length;

        const summary = document.getElementById('healthSummary');
        if (summary) {
            summary.textContent = names.length === 0
                ? 'no refreshes yet'
                : issues === 0 ? 'all channels healthy' : `${issues} issue(s)`;
            summary.className = 'health-summary ' + (issues === 0 ? 'healthy' : 'degraded');
        }

        const disk = document.getElementById('healthDisk');
        if (disk) {
            disk.textContent = '';
            const free = data.disk && data.disk.free_bytes;
            const minimum = data.disk && data.disk.min_free_space_gb;
            if (Number.isFinite(free)) {
                const freeGb = (free / (1024 ** 3)).toFixed(2);
                disk.textContent = Number(minimum) > 0
                    ? `Free disk space: ${freeGb} GB (minimum ${minimum} GB)`
                    : `Free disk space: ${freeGb} GB`;
                disk.className = 'health-disk' + (Number(minimum) > 0 && free < Number(minimum) * (1024 ** 3) ? ' low' : '');
            } else {
                disk.textContent = 'Free disk space: unavailable';
            }
        }

        grid.textContent = '';
        if (names.length === 0) {
            const empty = document.createElement('div');
            empty.className = 'health-empty';
            empty.textContent = 'No refresh history yet. Run a refresh to start tracking channel health.';
            grid.appendChild(empty);
        } else {
            names.forEach(name => grid.appendChild(healthRow(name, states[name])));
        }

        const notifications = document.getElementById('healthNotifications');
        if (notifications) {
            notifications.textContent = '';
            const settings = (data.notifications && data.notifications.settings) || {};
            const line = document.createElement('div');
            line.className = 'health-notification-line';
            line.textContent = settings.webhook_url
                ? 'Webhook configured · alerts: repeated failures, unavailable channels' + (settings.notify_low_space ? ', low disk space' : '')
                : 'No webhook configured — alerts stay in this panel only.';
            notifications.appendChild(line);
            const lastError = data.notifications && data.notifications.last_error;
            if (lastError) {
                const error = document.createElement('div');
                error.className = 'health-error';
                error.textContent = `Last delivery error: ${lastError}`;
                notifications.appendChild(error);
            }
            (data.pending_events || []).forEach(event => {
                const pending = document.createElement('div');
                pending.className = 'health-pending';
                pending.textContent = `⚠️ ${event.message}`;
                notifications.appendChild(pending);
            });
        }
    } catch (error) {
        console.error('Error loading health state:', error);
    }
}

function startHealthPolling() {
    clearInterval(healthPollTimer);
    refreshHealthPanel();
    healthPollTimer = setInterval(refreshHealthPanel, 15000);
}

async function runHealthCheck() {
    try {
        const response = await fetch('/api/health/check', { method: 'POST' });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || 'Health check failed');
        const events = (data.events || []).length;
        showToast(events === 0 ? '✅ Health check: no issues' : `⚠️ Health check found ${events} issue(s)`);
        refreshHealthPanel();
    } catch (error) {
        showToast('❌ ' + (error.message || 'Health check failed'));
    }
}

async function populateNotificationSettings() {
    try {
        const response = await fetch('/api/config/notifications');
        if (!response.ok) return;
        const settings = await response.json();
        document.getElementById('webhookUrl').value = settings.webhook_url || '';
        document.getElementById('failureThreshold').value = settings.failure_threshold;
        document.getElementById('cooldownMinutes').value = settings.cooldown_minutes;
        document.getElementById('notifyLowSpace').checked = settings.notify_low_space !== false;
    } catch (error) {
        console.error('Error loading notification settings:', error);
    }
}

async function saveNotificationSettings() {
    const payload = {
        webhook_url: document.getElementById('webhookUrl').value.trim(),
        failure_threshold: parseInt(document.getElementById('failureThreshold').value) || 3,
        cooldown_minutes: parseInt(document.getElementById('cooldownMinutes').value) || 0,
        notify_low_space: document.getElementById('notifyLowSpace').checked,
    };
    try {
        const response = await fetch('/api/config/notifications', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || 'Failed to save notification settings');
        showToast('✅ Notification settings saved');
        refreshHealthPanel();
    } catch (error) {
        showToast('❌ ' + (error.message || 'Failed to save notification settings'));
    }
}

async function testNotification() {
    try {
        const response = await fetch('/api/notifications/test', { method: 'POST' });
        const data = await response.json();
        if (!response.ok || !data.ok) {
            throw new Error((data.result && data.result.error) || data.error || 'Test notification failed');
        }
        showToast('✅ Test notification delivered');
    } catch (error) {
        showToast('❌ ' + (error.message || 'Test notification failed'));
    }
}

// Check refresh status on page load
document.addEventListener('DOMContentLoaded', function() {
    checkRefreshStatus();
    startQueuePolling();
    startHealthPolling();
    refreshStoragePanel();
    setInterval(refreshStoragePanel, 30000);
    updateRelativeTimes();
    
    // Update relative times every minute
    setInterval(updateRelativeTimes, 60000);
    
    // Adjust tooltip positions when modal is opened
    const observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
            if (mutation.type === 'attributes' && mutation.attributeName === 'style') {
                const modal = document.getElementById('channelModal');
                if (modal && modal.style.display === 'block') {
                    // Wait a bit for the modal to fully render
                    setTimeout(adjustTooltipPositions, 100);
                }
            }
        });
    });
    
    const channelModal = document.getElementById('channelModal');
    if (channelModal) {
        observer.observe(channelModal, { attributes: true });
    }
});