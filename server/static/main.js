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
  const collapseStorageKey = 'itemsTreeCollapse';
  let collapseState = {};
  try {
    collapseState = JSON.parse(localStorage.getItem(collapseStorageKey) || '{}');
  } catch (err) {
    collapseState = {};
  }

  const pendingChanges = new Map();
  let saveInProgress = false;
  const pendingIndicator = document.getElementById('pending-count');
  const saveMonitoringButton = document.getElementById('save-monitoring');

  const updatePendingDisplay = () => {
    const count = pendingChanges.size;
    if (pendingIndicator) {
      if (count === 0) {
        pendingIndicator.textContent = 'All changes saved';
        pendingIndicator.classList.remove('has-pending');
      } else {
        pendingIndicator.textContent = `${count} pending ${count === 1 ? 'change' : 'changes'}`;
        pendingIndicator.classList.add('has-pending');
      }
    }
    if (saveMonitoringButton) {
      saveMonitoringButton.disabled = saveInProgress || count === 0;
    }
  };

  const refreshDirectoryPending = () => {
    document.querySelectorAll('.tree-node').forEach((node) => node.classList.remove('has-pending'));
    document.querySelectorAll('.tree-row.pending-change').forEach((row) => {
      let current = row.closest('.tree-node');
      while (current) {
        current.classList.add('has-pending');
        current = current.parentElement?.closest('.tree-node');
      }
    });
  };

  const markToggleDirty = (toggle) => {
    const itemId = toggle.dataset.itemId;
    if (!itemId) return;
    const original = toggle.dataset.originalMonitored;
    if (original === undefined) return;
    const desired = toggle.checked ? '1' : '0';
    const row = toggle.closest('.tree-row');
    if (desired === original) {
      pendingChanges.delete(itemId);
      if (row) {
        row.classList.remove('pending-change');
      }
    } else {
      pendingChanges.set(itemId, toggle.checked);
      if (row) {
        row.classList.add('pending-change');
      }
    }
  };

  const updateAncestorStates = (node) => {
    let current = node?.parentElement?.closest('.tree-node');
    while (current) {
      const toggle = current.querySelector(':scope > .tree-row .monitor-toggle');
      const children = Array.from(
        current.querySelectorAll(':scope > .tree-children > .tree-node > .tree-row .monitor-toggle')
      );
      if (!toggle || children.length === 0) {
        current = current.parentElement?.closest('.tree-node');
        continue;
      }
      const checkedCount = children.filter((child) => child.checked).length;
      if (checkedCount === 0) {
        toggle.indeterminate = false;
        toggle.checked = false;
        toggle.dataset.state = 'none';
      } else if (checkedCount === children.length) {
        toggle.indeterminate = false;
        toggle.checked = true;
        toggle.dataset.state = 'all';
      } else {
        toggle.checked = true;
        toggle.indeterminate = true;
        toggle.dataset.state = 'partial';
      }
      current = current.parentElement?.closest('.tree-node');
    }
  };

  const updateDescendantToggles = (node, checked, skip) => {
    const affected = [];
    node.querySelectorAll('.tree-node .monitor-toggle').forEach((child) => {
      if (child === skip) return;
      child.indeterminate = false;
      child.checked = checked;
      child.dataset.state = checked ? 'all' : 'none';
      affected.push(child);
    });
    return affected;
  };

  const setNodeCollapse = (node, collapsed) => {
    const button = node.querySelector('.collapse-toggle');
    node.classList.toggle('collapsed', collapsed);
    if (button) {
      button.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
      button.textContent = collapsed ? '▸' : '▾';
      button.title = collapsed ? 'Expand section' : 'Collapse section';
      button.setAttribute('aria-label', collapsed ? 'Expand section' : 'Collapse section');
    }
  };

  const persistCollapseState = () => {
    try {
      if (Object.keys(collapseState).length === 0) {
        localStorage.removeItem(collapseStorageKey);
      } else {
        localStorage.setItem(collapseStorageKey, JSON.stringify(collapseState));
      }
    } catch (err) {
      console.warn('Unable to persist collapse state', err);
    }
  };

  const rememberNodeCollapse = (node, collapsed, persist = false) => {
    setNodeCollapse(node, collapsed);
    const path = node.dataset.path;
    if (path) {
      if (collapsed) {
        collapseState[path] = true;
      } else {
        delete collapseState[path];
      }
      if (persist) {
        persistCollapseState();
      }
    }
  };

  const expandAncestors = (node) => {
    let current = node?.parentElement?.closest('.tree-node.directory');
    while (current) {
      if (current.classList.contains('collapsed')) {
        rememberNodeCollapse(current, false);
      }
      current = current.parentElement?.closest('.tree-node.directory');
    }
  };

  document.querySelectorAll('.tree-node.directory').forEach((node) => {
    const path = node.dataset.path;
    const button = node.querySelector('.collapse-toggle');
    if (!button) return;
    const initial = path && Object.prototype.hasOwnProperty.call(collapseState, path) ? collapseState[path] : false;
    setNodeCollapse(node, Boolean(initial));
    button.addEventListener('click', () => {
      const collapsed = !node.classList.contains('collapsed');
      rememberNodeCollapse(node, collapsed, true);
    });
  });

  document.querySelectorAll('.monitor-toggle').forEach((checkbox) => {
    if (checkbox.dataset.state === 'partial') {
      checkbox.indeterminate = true;
    }
    checkbox.addEventListener('change', (event) => {
      const target = event.target;
      const checked = target.checked;
      const node = target.closest('.tree-node');
      target.indeterminate = false;
      target.dataset.state = checked ? 'all' : 'none';

      const affected = [];
      if (node && node.classList.contains('directory')) {
        updateDescendantToggles(node, checked, target).forEach((toggle) => affected.push(toggle));
      }
      affected.push(target);
      const unique = Array.from(new Set(affected));
      unique.forEach((toggle) => markToggleDirty(toggle));

      if (node) {
        updateAncestorStates(node);
      }
      refreshDirectoryPending();
      updatePendingDisplay();
    });
  });

  if (saveMonitoringButton) {
    saveMonitoringButton.addEventListener('click', async () => {
      if (pendingChanges.size === 0 || saveInProgress) {
        return;
      }
      saveInProgress = true;
      updatePendingDisplay();
      const changeEntries = Array.from(pendingChanges.entries());
      const payload = changeEntries.map(([itemId, monitored]) => ({
        item_id: Number(itemId),
        monitored,
      }));
      try {
        const resp = await postJson('/items/bulk-monitor', { changes: payload });
        changeEntries.forEach(([itemId, monitored]) => {
          const toggle = document.querySelector(`.monitor-toggle[data-item-id="${itemId}"]`);
          if (!toggle) return;
          toggle.dataset.originalMonitored = monitored ? '1' : '0';
          toggle.dataset.originalState = monitored ? 'all' : 'none';
          toggle.dataset.state = monitored ? 'all' : 'none';
          toggle.checked = monitored;
          toggle.indeterminate = false;
          const row = toggle.closest('.tree-row');
          if (row) {
            row.classList.remove('pending-change');
          }
          const node = toggle.closest('.tree-node');
          if (node) {
            updateAncestorStates(node);
          }
        });
        pendingChanges.clear();
        refreshDirectoryPending();
        const downloadsStarted = resp?.downloads_started ?? 0;
        const errors = Array.isArray(resp?.errors) ? resp.errors : [];
        const messages = ['Monitoring changes saved.'];
        if (downloadsStarted > 0) {
          messages.push(`${downloadsStarted} download${downloadsStarted === 1 ? '' : 's'} started automatically.`);
        }
        if (errors.length > 0) {
          messages.push(`${errors.length} error${errors.length === 1 ? '' : 's'} occurred. Check logs for details.`);
        }
        alert(messages.join(' '));
      } catch (err) {
        console.error('Failed to save monitoring changes', err);
        alert('Failed to save monitoring changes.');
      } finally {
        saveInProgress = false;
        updatePendingDisplay();
      }
    });
  }

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

  const expandAllButton = document.getElementById('expand-all');
  if (expandAllButton) {
    expandAllButton.addEventListener('click', () => {
      collapseState = {};
      document.querySelectorAll('.tree-node.directory').forEach((node) => {
        rememberNodeCollapse(node, false);
      });
      persistCollapseState();
    });
  }

  const collapseAllButton = document.getElementById('collapse-all');
  if (collapseAllButton) {
    collapseAllButton.addEventListener('click', () => {
      collapseState = {};
      document.querySelectorAll('.tree-node.directory').forEach((node) => {
        rememberNodeCollapse(node, true);
      });
      persistCollapseState();
    });
  }

  const expandDirectoriesByAttr = (attr) => {
    let changed = false;
    document.querySelectorAll('.tree-node.directory').forEach((node) => {
      const value = Number(node.dataset[attr] || '0');
      if (value > 0) {
        rememberNodeCollapse(node, false);
        expandAncestors(node);
        changed = true;
      }
    });
    if (changed) {
      persistCollapseState();
    }
  };

  const expandFilesBySyncState = (states) => {
    let changed = false;
    document.querySelectorAll('.tree-node.file').forEach((node) => {
      const state = node.dataset.syncState;
      if (states.includes(state)) {
        expandAncestors(node);
        changed = true;
      }
    });
    if (changed) {
      persistCollapseState();
    }
  };

  const expandMonitoredButton = document.getElementById('expand-monitored');
  if (expandMonitoredButton) {
    expandMonitoredButton.addEventListener('click', () => {
      expandDirectoriesByAttr('monitoredCount');
    });
  }

  const expandUnsyncedButton = document.getElementById('expand-unsynced');
  if (expandUnsyncedButton) {
    expandUnsyncedButton.addEventListener('click', () => {
      expandDirectoriesByAttr('unsyncedCount');
      expandFilesBySyncState(['missing', 'outdated']);
    });
  }

  const resetTreeStateButton = document.getElementById('reset-tree-state');
  if (resetTreeStateButton) {
    resetTreeStateButton.addEventListener('click', () => {
      collapseState = {};
      localStorage.removeItem(collapseStorageKey);
      document.querySelectorAll('.tree-node.directory').forEach((node) => {
        rememberNodeCollapse(node, false);
      });
    });
  }

  refreshDirectoryPending();
  updatePendingDisplay();

  pollScanStatus();
});
