(function () {
  if (typeof window === 'undefined') {
    return;
  }

  const chartCards = document.querySelectorAll('[data-chart-type]');
  if (!chartCards.length || typeof Chart === 'undefined') {
    return;
  }

  const styles = getComputedStyle(document.documentElement);
  const textColor = (styles.getPropertyValue('--text-secondary') || '#5a5673').trim();
  const gridColor = (styles.getPropertyValue('--border-soft') || 'rgba(94, 101, 255, 0.2)').trim();

  // Vibrant purple/blue palette matching card values (light to dark)
  const basePalette = ['#a7b4ff', '#9aa4ff', '#8b94fd', '#7c85fd', '#6f78fc', '#5f6afc'];

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
    const purplePrimary = '#9961c1';
    const purpleSoft = 'rgba(216, 181, 232, 0.3)';
    const purpleAccent = '#d8b5e8';

    return new Chart(ctx, {
      type: 'line',
      data: {
        labels: config.labels,
        datasets: [
          {
            label: config.series_label || config.title,
            data: config.values,
            borderColor: purplePrimary,
            backgroundColor: purpleSoft,
            pointBackgroundColor: purpleAccent,
            pointBorderColor: '#ffffff',
            pointRadius: 0,
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
                return `€${Number(value).toLocaleString()}`;
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
                return `${config.series_label || 'Value'}: €${Number(value).toLocaleString()}`;
              },
            },
          },
        },
      },
    });
  };

  let stockChart = null;

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
      stockChart = createLineChart(canvas, config);
      // Store canvas and card for later updates
      stockChart._canvas = canvas;
      stockChart._card = card;
    }
  });

  // Stock selector functionality
  const stockSelector = document.getElementById('stock-selector');
  if (stockSelector && stockChart) {
    // Fetch available stocks
    fetch('/dashboard/api/stocks')
      .then((response) => response.json())
      .then((data) => {
        const stocks = data.stocks || [];
        
        // Populate the selector
        stocks.forEach((stock) => {
          const option = document.createElement('option');
          option.value = stock.id;
          option.textContent = stock.label;
          stockSelector.appendChild(option);
        });

        // Add change event listener
        stockSelector.addEventListener('change', async (e) => {
          const instrumentId = e.target.value;
          if (!instrumentId) return;

          try {
            const response = await fetch(`/dashboard/api/stocks/${instrumentId}/prices`);
            const priceData = await response.json();

            // Update the chart title
            const titleElement = stockChart._card.querySelector('.chart-card__title');
            if (titleElement) {
              titleElement.textContent = priceData.title;
            }

            // Update the hint
            const hintElement = stockChart._card.querySelector('.chart-card__hint');
            if (hintElement && priceData.hint) {
              hintElement.textContent = priceData.hint;
            }

            // Update chart data
            stockChart.data.labels = priceData.labels;
            stockChart.data.datasets[0].data = priceData.values;
            stockChart.data.datasets[0].label = priceData.series_label;
            stockChart.update();
          } catch (error) {
            console.error('Failed to load stock price data:', error);
          }
        });
      })
      .catch((error) => {
        console.error('Failed to load stock list:', error);
      });
  }
})();
