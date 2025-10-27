document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.monitor-toggle').forEach((checkbox) => {
    checkbox.addEventListener('change', async (event) => {
      const target = event.target;
      const row = target.closest('tr[data-item-id]');
      if (!row) return;
      const itemId = row.dataset.itemId;
      try {
        await fetch(`/items/${itemId}/monitor`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ monitored: target.checked })
        });
      } catch (err) {
        console.error('Failed to update monitor flag', err);
        target.checked = !target.checked;
      }
    });
  });

  document.querySelectorAll('.ignore-toggle').forEach((checkbox) => {
    checkbox.addEventListener('change', async (event) => {
      const target = event.target;
      const row = target.closest('tr[data-item-id]');
      if (!row) return;
      const itemId = row.dataset.itemId;
      try {
        await fetch(`/items/${itemId}/ignore`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ignored: target.checked })
        });
      } catch (err) {
        console.error('Failed to update ignore flag', err);
        target.checked = !target.checked;
      }
    });
  });

  const saveButton = document.getElementById('save-interval');
  if (saveButton) {
    saveButton.addEventListener('click', async () => {
      const select = document.getElementById('interval-select');
      if (!select) return;
      const value = select.value;
      const resp = await fetch('/settings/interval', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value })
      });
      if (!resp.ok) {
        alert('Failed to update interval');
      } else {
        alert('Interval updated');
      }
    });
  }
});
