(function () {
  if (typeof window === 'undefined') {
    return;
  }

  const palette = ['#6d5dfc', '#8b7ffc', '#ab9dfc', '#c4b8ff', '#ded2ff'];

  const renderChart = (wrapper) => {
    if (!wrapper || wrapper.dataset.chartReady === 'true') {
      return;
    }

    const canvas = wrapper.querySelector('canvas');
    if (!canvas || typeof Chart === 'undefined') {
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
        legend: { display: type === 'pie' },
      },
    };

    if (type === 'line') {
      options.scales = {
        x: { ticks: { autoSkip: true } },
        y: {
          ticks: {
            callback: (value) => value.toLocaleString(),
          },
        },
      };
    }

    new Chart(context, {
      type,
      data: { labels: config.labels, datasets: [dataset] },
      options,
    });

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
})();
