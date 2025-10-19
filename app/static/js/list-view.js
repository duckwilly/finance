(function () {
  "use strict";

  const initListView = (root) => {
    const searchInput = root.querySelector("[data-list-view-search]");
    const rows = Array.from(root.querySelectorAll("[data-list-view-row]"));
    const emptyRow = root.querySelector("[data-list-view-empty]");
    const countNode = root.querySelector("[data-list-view-count]");
    const table = root.querySelector("[data-list-view-table]");
    const headers = Array.from(root.querySelectorAll("th[data-column-key]"));

    let currentSort = { column: null, direction: "asc" };
    let filteredRows = [...rows];

    // Default sort by name column if it exists
    const nameColumn = headers.find(h => h.dataset.columnKey === "name");
    if (nameColumn) {
      currentSort.column = "name";
      currentSort.direction = "asc";
    }

    const parseValue = (value, columnType) => {
      if (value === null || value === undefined || value === "—") return null;
      
      if (columnType === "currency") {
        // Remove currency symbols and parse as number, handling suffixes like "thousand", "million", etc.
        let numStr = value.replace(/[€$£¥,]/g, "").trim();
        
        // Handle common suffixes (both long and short formats)
        const multipliers = {
          'thousand': 1000, 'k': 1000,
          'million': 1000000, 'M': 1000000,
          'billion': 1000000000, 'B': 1000000000,
          'trillion': 1000000000000, 'T': 1000000000000
        };
        
        let multiplier = 1;
        for (const [suffix, mult] of Object.entries(multipliers)) {
          if (numStr.toLowerCase().includes(suffix)) {
            numStr = numStr.toLowerCase().replace(suffix, '').trim();
            multiplier = mult;
            break;
          }
        }
        
        const num = parseFloat(numStr) * multiplier;
        return isNaN(num) ? null : num;
      }
      
      if (typeof value === "string") {
        const num = parseFloat(value);
        return isNaN(num) ? value.toLowerCase() : num;
      }
      
      return value;
    };

    const sortRows = (rows, column, direction) => {
      const header = headers.find(h => h.dataset.columnKey === column);
      const columnType = header?.dataset.columnType || "text";
      
      return [...rows].sort((a, b) => {
        const aCell = a.querySelector(`[data-column-key="${column}"]`);
        const bCell = b.querySelector(`[data-column-key="${column}"]`);
        
        const aValue = parseValue(aCell?.textContent?.trim(), columnType);
        const bValue = parseValue(bCell?.textContent?.trim(), columnType);
        
        if (aValue === null && bValue === null) return 0;
        if (aValue === null) return direction === "asc" ? 1 : -1;
        if (bValue === null) return direction === "asc" ? -1 : 1;
        
        if (aValue < bValue) return direction === "asc" ? -1 : 1;
        if (aValue > bValue) return direction === "asc" ? 1 : -1;
        return 0;
      });
    };

    const updateSortIndicators = () => {
      headers.forEach(header => {
        const column = header.dataset.columnKey;
        header.classList.remove("list-view__cell--sort-asc", "list-view__cell--sort-desc");
        
        if (currentSort.column === column) {
          header.classList.add(`list-view__cell--sort-${currentSort.direction}`);
        }
      });
    };

    const update = () => {
      const term = (searchInput?.value || "").trim().toLowerCase();
      let visibleCount = 0;

      // Filter rows
      filteredRows = rows.filter(row => {
        const searchValue = (row.dataset.searchText || "").toLowerCase();
        const matches = term.length === 0 || searchValue.includes(term);
        if (matches) {
          visibleCount += 1;
        }
        return matches;
      });

      // Sort filtered rows
      if (currentSort.column) {
        filteredRows = sortRows(filteredRows, currentSort.column, currentSort.direction);
      }

      // Update DOM - hide all rows first
      rows.forEach(row => {
        row.hidden = true;
        row.classList.add("list-view__row--hidden");
      });

      // Show and reorder visible rows
      const tbody = table.querySelector("tbody");
      filteredRows.forEach((row, index) => {
        row.hidden = false;
        row.classList.remove("list-view__row--hidden");
        
        // Move to correct position in DOM
        if (index === 0) {
          tbody.insertBefore(row, tbody.firstChild);
        } else {
          const prevRow = filteredRows[index - 1];
          tbody.insertBefore(row, prevRow.nextSibling);
        }
      });

      if (emptyRow) {
        emptyRow.hidden = visibleCount > 0;
      }

      if (countNode) {
        countNode.textContent = String(visibleCount);
      }
    };

    // Add click handlers to sortable headers
    headers.forEach(header => {
      header.style.cursor = "pointer";
      header.setAttribute("title", "Click to sort");
      
      header.addEventListener("click", () => {
        const column = header.dataset.columnKey;
        
        if (currentSort.column === column) {
          currentSort.direction = currentSort.direction === "asc" ? "desc" : "asc";
        } else {
          currentSort.column = column;
          currentSort.direction = "asc";
        }
        
        updateSortIndicators();
        update();
      });
    });

    if (searchInput) {
      searchInput.addEventListener("input", update);
    }

    // Initialize with default sort
    updateSortIndicators();
    update();
  };

  const bootstrap = () => {
    document
      .querySelectorAll("[data-list-view]")
      .forEach((root) => initListView(root));
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootstrap);
  } else {
    bootstrap();
  }
})();
