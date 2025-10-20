(function () {
  if (typeof window === 'undefined') {
    return;
  }

  const chartCards = document.querySelectorAll('[data-chart-type]');
  if (!chartCards.length || typeof Chart === 'undefined') {
    return;
  }

  const styles = getComputedStyle(document.documentElement);
  const primary = (styles.getPropertyValue('--primary') || '#5f6afc').trim();
  const primarySoft = (styles.getPropertyValue('--primary-soft') || '#a7b4ff').trim();
  const accent = (styles.getPropertyValue('--accent') || '#ffb8d2').trim();
  const textColor = (styles.getPropertyValue('--text-secondary') || '#5a5673').trim();
  const gridColor = (styles.getPropertyValue('--border-soft') || 'rgba(94, 101, 255, 0.2)').trim();

  const basePalette = [primary, primarySoft, accent, '#c4f0e8', '#ffd9ec', '#d4ccff'];

  const ensurePalette = (count) => {
    const colors = [];
    for (let i = 0; i < count; i += 1) {
      colors.push(basePalette[i % basePalette.length]);
    }
    return colors;
  };

  const createPieChart = (canvas, config) => {
    const ctx = canvas.getContext('2d');
    const sliceColors = ensurePalette(config.labels.length || 1);

    return new Chart(ctx, {
      type: 'pie',
      data: {
        labels: config.labels,
        datasets: [
          {
            label: config.title,
            data: config.values,
            backgroundColor: sliceColors,
            borderColor: '#ffffff',
            borderWidth: 1,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        layout: {
          padding: 8,
        },
        plugins: {
          legend: {
            position: 'bottom',
            labels: {
              color: textColor,
              usePointStyle: true,
            },
          },
          tooltip: {
            callbacks: {
              label(context) {
                const label = context.label || '';
                const value = context.parsed;
                return `${label}: ${value.toLocaleString()}`;
              },
            },
          },
        },
      },
    });
  };

  const createLineChart = (canvas, config) => {
    const ctx = canvas.getContext('2d');

    return new Chart(ctx, {
      type: 'line',
      data: {
        labels: config.labels,
        datasets: [
          {
            label: config.series_label || config.title,
            data: config.values,
            borderColor: primary,
            backgroundColor: primarySoft,
            pointBackgroundColor: accent,
            pointBorderColor: '#ffffff',
            pointRadius: 3,
            tension: 0.35,
            fill: true,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: {
            ticks: {
              color: textColor,
            },
            grid: {
              color: gridColor,
              drawBorder: false,
            },
          },
          y: {
            ticks: {
              color: textColor,
              callback(value) {
                return `$${Number(value).toLocaleString()}`;
              },
            },
            grid: {
              color: gridColor,
              drawBorder: false,
            },
          },
        },
        plugins: {
          legend: {
            display: false,
          },
          tooltip: {
            callbacks: {
              label(context) {
                const value = context.parsed.y;
                return `${config.series_label || 'Value'}: $${Number(value).toLocaleString()}`;
              },
            },
          },
        },
      },
    });
  };

  chartCards.forEach((card) => {
    const canvas = card.querySelector('canvas');
    if (!canvas) {
      return;
    }

    const { chartType, chartConfig } = card.dataset;
    if (!chartConfig) {
      return;
    }

    let config;
    try {
      config = JSON.parse(chartConfig);
    } catch (error) {
      console.error('Unable to parse chart configuration', error);
      return;
    }

    if (!config || !Array.isArray(config.labels) || !Array.isArray(config.values)) {
      return;
    }

    if ((config.labels.length === 0 || config.values.length === 0) && chartType !== 'line') {
      card.setAttribute('data-chart-empty', 'true');
      return;
    }

    if (chartType === 'pie') {
      createPieChart(canvas, config);
    } else if (chartType === 'line') {
      createLineChart(canvas, config);
    }
  });
})();
