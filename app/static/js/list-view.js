(function () {
  "use strict";

  const initListView = (root) => {
    const searchInput = root.querySelector("[data-list-view-search]");
    const rows = Array.from(root.querySelectorAll("[data-list-view-row]"));
    const emptyRow = root.querySelector("[data-list-view-empty]");
    const countNode = root.querySelector("[data-list-view-count]");

    const update = () => {
      const term = (searchInput?.value || "").trim().toLowerCase();
      let visibleCount = 0;

      rows.forEach((row) => {
        const searchValue = (row.dataset.searchText || "").toLowerCase();
        const matches = term.length === 0 || searchValue.includes(term);
        row.hidden = !matches;
        row.classList.toggle("list-view__row--hidden", !matches);
        if (matches) {
          visibleCount += 1;
        }
      });

      if (emptyRow) {
        emptyRow.hidden = visibleCount > 0;
      }

      if (countNode) {
        countNode.textContent = String(visibleCount);
      }
    };

    if (searchInput) {
      searchInput.addEventListener("input", update);
    }

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
