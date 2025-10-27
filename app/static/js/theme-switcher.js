/**
 * Theme Switcher
 * Handles light/dark mode toggle and persists preference to localStorage
 */

(function() {
  const THEME_KEY = 'finance-app-theme';
  const LIGHT = 'light';
  const DARK = 'dark';

  /**
   * Get the current theme from localStorage or system preference
   */
  function getCurrentTheme() {
    const stored = localStorage.getItem(THEME_KEY);
    if (stored) {
      return stored;
    }
    
    // Check system preference
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return DARK;
    }
    
    return LIGHT;
  }

  /**
   * Apply theme to the document
   */
  function applyTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem(THEME_KEY, theme);
    
    // Update toggle button if it exists
    const toggleBtn = document.getElementById('theme-toggle');
    if (toggleBtn) {
      const icon = toggleBtn.querySelector('.theme-toggle__icon');
      const label = toggleBtn.querySelector('.theme-toggle__label');
      
      if (theme === DARK) {
        if (icon) icon.textContent = '☀️';
        if (label) label.textContent = 'Light mode';
        toggleBtn.setAttribute('aria-label', 'Switch to light mode');
      } else {
        if (icon) icon.textContent = '🌙';
        if (label) label.textContent = 'Dark mode';
        toggleBtn.setAttribute('aria-label', 'Switch to dark mode');
      }
    }
  }

  /**
   * Toggle between light and dark themes
   */
  function toggleTheme() {
    const current = getCurrentTheme();
    const next = current === LIGHT ? DARK : LIGHT;
    applyTheme(next);
  }

  /**
   * Initialize theme on page load
   */
  function initTheme() {
    const theme = getCurrentTheme();
    applyTheme(theme);
    
    // Listen for system theme changes
    if (window.matchMedia) {
      window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
        // Only auto-switch if user hasn't set a preference
        if (!localStorage.getItem(THEME_KEY)) {
          applyTheme(e.matches ? DARK : LIGHT);
        }
      });
    }
  }

  /**
   * Set up event listeners
   */
  function setupListeners() {
    const toggleBtn = document.getElementById('theme-toggle');
    if (toggleBtn) {
      toggleBtn.addEventListener('click', toggleTheme);
    }
  }

  // Initialize on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      initTheme();
      setupListeners();
    });
  } else {
    initTheme();
    setupListeners();
  }

  // Expose for manual control if needed
  window.themeToggle = {
    set: applyTheme,
    toggle: toggleTheme,
    get: getCurrentTheme
  };
})();

