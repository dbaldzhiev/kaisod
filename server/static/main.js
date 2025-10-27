const postJson = async (url, payload) => {
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || resp.statusText);
  }
  return resp.json().catch(() => ({}));
};

const updateDescendantToggles = (node, selector, checked, skip) => {
  node.querySelectorAll(selector).forEach((child) => {
    if (child === skip) return;
    child.indeterminate = false;
    child.checked = checked;
    child.dataset.state = checked ? 'all' : 'none';
  });
};

const disableCounterpart = (node, selector) => {
  const counterpart = node.querySelector(selector);
  if (counterpart) {
    counterpart.checked = false;
    counterpart.indeterminate = false;
    counterpart.dataset.state = 'none';
  }
};

const updateDashboard = (data) => {
  const stateEl = document.getElementById('dashboard-scan-state');
  if (stateEl) {
    stateEl.textContent = data.status ? data.status.charAt(0).toUpperCase() + data.status.slice(1) : 'Idle';
  }
  const messageEl = document.getElementById('dashboard-scan-message');
  if (messageEl) {
    messageEl.textContent = data.progress?.message || 'Idle';
  }
  const pathEl = document.getElementById('dashboard-scan-path');
  if (pathEl) {
    pathEl.textContent = data.progress?.current_path || '—';
  }
  const lastScanEl = document.getElementById('dashboard-last-scan');
  if (lastScanEl && data.last_scan_at !== undefined) {
    lastScanEl.textContent = data.last_scan_at ? formatDisplayDate(data.last_scan_at) : '—';
  }
  const nextScanEl = document.getElementById('dashboard-next-scan');
  if (nextScanEl && data.next_scan_at !== undefined) {
    nextScanEl.textContent = data.next_scan_at ? formatDisplayDate(data.next_scan_at) : '—';
  }
  const resultEl = document.getElementById('dashboard-last-result');
  if (resultEl) {
    const result = data.last_result;
    if (result) {
      resultEl.textContent = `Last result — new: ${result.new}, updated: ${result.updated}, unchanged: ${result.unchanged}, errors: ${result.errors}`;
    } else {
      resultEl.textContent = 'Last result — n/a';
    }
  }
  const button = document.getElementById('dashboard-scan-button');
  if (button) {
    button.disabled = data.status === 'running';
  }
};

const formatDisplayDate = (isoString) => {
  try {
    const dt = new Date(isoString);
    const pad = (n) => n.toString().padStart(2, '0');
    return `${pad(dt.getDate())}.${pad(dt.getMonth() + 1)}.${dt.getFullYear()} ${pad(dt.getHours())}:${pad(dt.getMinutes())}`;
  } catch (err) {
    return isoString;
  }
};

const updateScanStatusBanner = (data) => {
  const banner = document.getElementById('scan-status-banner');
  if (!banner) return;
  const running = data.status === 'running';
  banner.dataset.running = running ? '1' : '0';
  banner.classList.toggle('running', running);
  const messageEl = banner.querySelector('[data-role="message"]');
  if (messageEl) {
    messageEl.textContent = data.progress?.message || 'Idle';
  }
  const countsEl = banner.querySelector('[data-role="counts"]');
  if (countsEl) {
    const processed = data.progress?.processed;
    const total = data.progress?.total;
    if (total && total > 0) {
      countsEl.textContent = `${processed ?? 0}/${total}`;
    } else {
      countsEl.textContent = '';
    }
  }
  const pathEl = banner.querySelector('[data-role="path"]');
  if (pathEl) {
    pathEl.textContent = data.progress?.current_path || '';
  }
};

const pollScanStatus = async () => {
  try {
    const resp = await fetch('/scan/status');
    if (!resp.ok) return;
    const data = await resp.json();
    updateScanStatusBanner(data);
    updateDashboard(data);
  } catch (err) {
    console.warn('Failed to refresh scan status', err);
  } finally {
    window.setTimeout(pollScanStatus, 5000);
  }
};

document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.monitor-toggle').forEach((checkbox) => {
    if (checkbox.dataset.state === 'partial') {
      checkbox.indeterminate = true;
    }
    checkbox.addEventListener('change', async (event) => {
      const target = event.target;
      const checked = target.checked;
      const sectionPath = target.dataset.sectionPath;
      const itemId = target.dataset.itemId;
      const node = target.closest('.tree-node');
      try {
        if (sectionPath) {
          await postJson('/sections/monitor', { path: sectionPath, monitored: checked });
          if (node) {
            updateDescendantToggles(node, '.monitor-toggle', checked, target);
            if (checked) {
              node.querySelectorAll('.ignore-toggle').forEach((ignoreToggle) => {
                ignoreToggle.checked = false;
                ignoreToggle.indeterminate = false;
                ignoreToggle.dataset.state = 'none';
              });
            }
          }
        } else if (itemId) {
          await postJson(`/items/${itemId}/monitor`, { monitored: checked });
          if (checked && node) {
            disableCounterpart(node, '.ignore-toggle');
          }
        }
        target.indeterminate = false;
        target.dataset.state = checked ? 'all' : 'none';
      } catch (err) {
        console.error('Failed to update monitor flag', err);
        target.checked = !checked;
        target.indeterminate = false;
      }
    });
  });

  document.querySelectorAll('.ignore-toggle').forEach((checkbox) => {
    if (checkbox.dataset.state === 'partial') {
      checkbox.indeterminate = true;
    }
    checkbox.addEventListener('change', async (event) => {
      const target = event.target;
      const checked = target.checked;
      const sectionPath = target.dataset.sectionPath;
      const itemId = target.dataset.itemId;
      const node = target.closest('.tree-node');
      try {
        if (sectionPath) {
          await postJson('/sections/ignore', { path: sectionPath, ignored: checked });
          if (node) {
            updateDescendantToggles(node, '.ignore-toggle', checked, target);
            if (checked) {
              node.querySelectorAll('.monitor-toggle').forEach((monitorToggle) => {
                monitorToggle.checked = false;
                monitorToggle.indeterminate = false;
                monitorToggle.dataset.state = 'none';
              });
            }
          }
        } else if (itemId) {
          await postJson(`/items/${itemId}/ignore`, { ignored: checked });
          if (checked && node) {
            disableCounterpart(node, '.monitor-toggle');
          }
        }
        target.indeterminate = false;
        target.dataset.state = checked ? 'all' : 'none';
      } catch (err) {
        console.error('Failed to update ignore flag', err);
        target.checked = !checked;
        target.indeterminate = false;
      }
    });
  });

  const saveButton = document.getElementById('save-interval');
  if (saveButton) {
    saveButton.addEventListener('click', async () => {
      const select = document.getElementById('interval-select');
      if (!select) return;
      const value = select.value;
      try {
        await postJson('/settings/interval', { value });
        alert('Interval updated');
      } catch (err) {
        alert('Failed to update interval');
      }
    });
  }

  pollScanStatus();
});
