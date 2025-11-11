(function () {
  if (typeof window === 'undefined') {
    return;
  }

  const palette = ['#6d5dfc', '#8b7ffc', '#ab9dfc', '#c4b8ff', '#ded2ff'];

  const getThemeColors = () => {
    const root = document.documentElement;
    const computedStyle = getComputedStyle(root);
    const isDark = root.getAttribute('data-theme') === 'dark';
    
    return {
      text: computedStyle.getPropertyValue('--text-primary').trim() || (isDark ? '#f8fafc' : '#2e2a4a'),
      textSecondary: computedStyle.getPropertyValue('--text-secondary').trim() || (isDark ? '#e2e8f0' : '#5a5673'),
      textMuted: computedStyle.getPropertyValue('--text-muted').trim() || (isDark ? '#94a3b8' : '#8a86a3'),
      border: computedStyle.getPropertyValue('--border').trim() || (isDark ? 'rgba(148, 163, 184, 0.2)' : 'rgba(94, 101, 255, 0.2)'),
      surface: computedStyle.getPropertyValue('--surface-alt').trim() || (isDark ? '#131b2e' : '#fff'),
    };
  };

  const chartInstances = new WeakMap();

  const updateChartColors = (chart) => {
    if (!chart) return;
    const colors = getThemeColors();
    
    if (chart.options.plugins.legend) {
      chart.options.plugins.legend.labels.color = colors.text;
    }
    if (chart.options.plugins.tooltip) {
      chart.options.plugins.tooltip.backgroundColor = colors.surface;
      chart.options.plugins.tooltip.titleColor = colors.text;
      chart.options.plugins.tooltip.bodyColor = colors.textSecondary;
      chart.options.plugins.tooltip.borderColor = colors.border;
    }
    if (chart.options.scales) {
      if (chart.options.scales.x) {
        chart.options.scales.x.ticks.color = colors.textMuted;
        chart.options.scales.x.grid.color = colors.border;
      }
      if (chart.options.scales.y) {
        chart.options.scales.y.ticks.color = colors.textMuted;
        chart.options.scales.y.grid.color = colors.border;
      }
    }
    chart.update();
  };

  const renderChart = (wrapper) => {
    const canvas = wrapper.querySelector('canvas');
    if (!canvas || typeof Chart === 'undefined') {
      return;
    }

    // If chart already exists, update its colors
    const existingChart = chartInstances.get(canvas);
    if (existingChart) {
      updateChartColors(existingChart);
      return;
    }

    // If already marked as ready but no instance, skip
    if (wrapper.dataset.chartReady === 'true') {
      return;
    }

    let config;
    try {
      config = JSON.parse(wrapper.dataset.chartConfig || '{}');
    } catch (error) {
      console.error('Invalid chart configuration', error);
      return;
    }

    if (!Array.isArray(config.labels) || !Array.isArray(config.values)) {
      return;
    }

    const context = canvas.getContext('2d');
    const type = wrapper.dataset.chartType;
    const colors = getThemeColors();

    const dataset = {
      label: config.series_label || config.title,
      data: config.values,
      backgroundColor: type === 'pie' ? palette : 'rgba(109, 93, 252, 0.25)',
      borderColor: '#6d5dfc',
      borderWidth: 2,
      fill: type === 'line',
      tension: type === 'line' ? 0.35 : 0,
      pointRadius: type === 'line' ? 0 : undefined,
    };

    const options = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: type === 'pie',
          labels: {
            color: colors.text,
            font: {
              size: 12,
            },
            padding: 12,
            usePointStyle: true,
          },
        },
        tooltip: {
          backgroundColor: colors.surface,
          titleColor: colors.text,
          bodyColor: colors.textSecondary,
          borderColor: colors.border,
          borderWidth: 1,
          padding: 10,
          titleFont: {
            size: 13,
            weight: '600',
          },
          bodyFont: {
            size: 12,
          },
        },
      },
    };

    if (type === 'line') {
      options.scales = {
        x: {
          ticks: {
            autoSkip: true,
            color: colors.textMuted,
            font: {
              size: 11,
            },
          },
          grid: {
            color: colors.border,
          },
        },
        y: {
          ticks: {
            callback: (value) => value.toLocaleString(),
            color: colors.textMuted,
            font: {
              size: 11,
            },
          },
          grid: {
            color: colors.border,
          },
        },
      };
    }

    const chart = new Chart(context, {
      type,
      data: { labels: config.labels, datasets: [dataset] },
      options,
    });

    chartInstances.set(canvas, chart);
    wrapper.dataset.chartReady = 'true';
  };

  const init = (root) => {
    (root || document)
      .querySelectorAll('[data-chart]')
      .forEach((wrapper) => renderChart(wrapper));
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => init(document));
  } else {
    init(document);
  }

  if (window.htmx) {
    document.body.addEventListener('htmx:afterSwap', (event) => {
      init(event.target);
    });
  }

  // Listen for theme changes and update all charts
  const observer = new MutationObserver(() => {
    document.querySelectorAll('[data-chart]').forEach((wrapper) => {
      const canvas = wrapper.querySelector('canvas');
      if (canvas) {
        const chart = chartInstances.get(canvas);
        if (chart) {
          updateChartColors(chart);
        }
      }
    });
  });

  observer.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ['data-theme'],
  });
})();
