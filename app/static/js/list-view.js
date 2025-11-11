(function () {
  'use strict';

  const parseNumericValue = (value, columnType) => {
    if (value === null || value === undefined || value === '—') {
      return null;
    }

    if (columnType === 'currency') {
      let number = value.replace(/[€$£¥,]/g, '').trim();
      const multipliers = {
        thousand: 1_000,
        k: 1_000,
        million: 1_000_000,
        M: 1_000_000,
        billion: 1_000_000_000,
        B: 1_000_000_000,
        trillion: 1_000_000_000_000,
        T: 1_000_000_000_000,
      };

      let multiplier = 1;
      for (const [suffix, factor] of Object.entries(multipliers)) {
        const pattern = new RegExp(`${suffix}$`, 'i');
        if (pattern.test(number)) {
          number = number.replace(pattern, '').trim();
          multiplier = factor;
          break;
        }
      }

      const numericValue = parseFloat(number) * multiplier;
      return Number.isNaN(numericValue) ? null : numericValue;
    }

    if (typeof value === 'string') {
      const numericValue = parseFloat(value);
      return Number.isNaN(numericValue) ? value.toLowerCase() : numericValue;
    }

    return value;
  };

  const sortRows = (rows, headers, column, direction) => {
    const header = headers.find((node) => node.dataset.columnKey === column);
    const columnType = header?.dataset.columnType || 'text';

    return [...rows].sort((a, b) => {
      const aCell = a.querySelector(`[data-column-key="${column}"]`);
      const bCell = b.querySelector(`[data-column-key="${column}"]`);

      const aValue = parseNumericValue(aCell?.textContent?.trim(), columnType);
      const bValue = parseNumericValue(bCell?.textContent?.trim(), columnType);

      if (aValue === null && bValue === null) return 0;
      if (aValue === null) return direction === 'asc' ? 1 : -1;
      if (bValue === null) return direction === 'asc' ? -1 : 1;

      if (aValue < bValue) return direction === 'asc' ? -1 : 1;
      if (aValue > bValue) return direction === 'asc' ? 1 : -1;
      return 0;
    });
  };

  const updateSortIndicators = (headers, currentSort) => {
    headers.forEach((header) => {
      const column = header.dataset.columnKey;
      header.classList.remove('list-view__cell--sort-asc', 'list-view__cell--sort-desc');

      if (currentSort.column === column) {
        header.classList.add(`list-view__cell--sort-${currentSort.direction}`);
      }
    });
  };

  const initListView = (root) => {
    if (!root || root.dataset.listViewReady === 'true') {
      return;
    }

    const searchInput = root.querySelector('[data-list-view-search]');
    const rows = Array.from(root.querySelectorAll('[data-list-view-row]'));
    const emptyRow = root.querySelector('[data-list-view-empty]');
    const countNode = root.querySelector('[data-list-view-count]');
    const table = root.querySelector('[data-list-view-table]');
    const headers = Array.from(root.querySelectorAll('th[data-column-key]'));

    const defaultSort = headers.find((header) => header.dataset.columnKey === 'name');
    const currentSort = {
      column: defaultSort ? 'name' : null,
      direction: 'asc',
    };

    const update = () => {
      const term = (searchInput?.value || '').trim().toLowerCase();
      let visibleCount = 0;

      let filtered = rows.filter((row) => {
        const searchValue = (row.dataset.searchText || '').toLowerCase();
        const matches = term.length === 0 || searchValue.includes(term);
        if (matches) {
          visibleCount += 1;
        }
        return matches;
      });

      if (currentSort.column) {
        filtered = sortRows(filtered, headers, currentSort.column, currentSort.direction);
      }

      const tbody = table?.querySelector('tbody');
      rows.forEach((row) => {
        row.hidden = true;
        row.classList.add('list-view__row--hidden');
      });

      filtered.forEach((row, index) => {
        row.hidden = false;
        row.classList.remove('list-view__row--hidden');

        if (tbody) {
          if (index === 0) {
            tbody.insertBefore(row, tbody.firstChild);
          } else {
            const previous = filtered[index - 1];
            if (previous?.nextSibling !== row) {
              tbody.insertBefore(row, previous.nextSibling);
            }
          }
        }
      });

      if (emptyRow) {
        emptyRow.hidden = visibleCount > 0;
      }

      if (countNode) {
        countNode.textContent = String(visibleCount);
      }
    };

    headers.forEach((header) => {
      header.style.cursor = 'pointer';
      header.setAttribute('title', 'Click to sort');
      header.addEventListener('click', () => {
        const column = header.dataset.columnKey;
        if (currentSort.column === column) {
          currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
        } else {
          currentSort.column = column;
          currentSort.direction = 'asc';
        }
        updateSortIndicators(headers, currentSort);
        update();
      });
    });

    if (searchInput) {
      searchInput.addEventListener('input', update);
    }

    updateSortIndicators(headers, currentSort);
    update();

    root.dataset.listViewReady = 'true';
  };

  const bootstrap = (scope) => {
    (scope || document)
      .querySelectorAll('[data-list-view]')
      .forEach((root) => initListView(root));
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => bootstrap(document));
  } else {
    bootstrap(document);
  }

  if (window.htmx) {
    document.body.addEventListener('htmx:afterSwap', (event) => {
      bootstrap(event.target);
    });
  }
})();
