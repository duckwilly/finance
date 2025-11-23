(() => {
  const root = document.querySelector('[data-presentation-root]');
  if (!root) return;

  const slides = JSON.parse(root.dataset.slides || '[]');
  let activeSlug = root.dataset.activeSlide;
  const endpointTemplate = root.dataset.slideEndpointTemplate;
  const stage = document.querySelector('#slide-stage');
  const nav = document.querySelector('[data-slide-nav]');
  const navToggle = document.querySelector('[data-nav-toggle]');
  const progressLabel = document.querySelector('[data-progress-label]');
  const prevButton = document.querySelector('[data-prev]');
  const nextButton = document.querySelector('[data-next]');

  const slugIndex = (slug) => slides.findIndex((item) => item.slug === slug);
  const endpointFor = (slug) => endpointTemplate.replace('__SLUG__', slug);

  const updateProgress = () => {
    const index = slugIndex(activeSlug);
    const total = slides.length;
    const slide = slides[index];
    if (progressLabel && slide) {
      const paddedIndex = String(index + 1).padStart(2, '0');
      const paddedTotal = String(total).padStart(2, '0');
      progressLabel.textContent = `${paddedIndex} / ${paddedTotal} — ${slide.title}`;
    }
    updateNav();
    updateArrows();
  };

  const updateNav = () => {
    if (!nav) return;
    const navLinks = nav.querySelectorAll('[data-slide]');
    navLinks.forEach((link) => {
      const isActive = link.dataset.slide === activeSlug;
      link.setAttribute('aria-current', isActive ? 'true' : 'false');
    });
  };

  const updateArrows = () => {
    const index = slugIndex(activeSlug);
    const prev = slides[index - 1];
    const next = slides[index + 1];

    if (prevButton) {
      prevButton.disabled = !prev;
      prevButton.dataset.slide = prev ? prev.slug : '';
      prevButton.setAttribute('hx-get', prev ? endpointFor(prev.slug) : '');
    }
    if (nextButton) {
      nextButton.disabled = !next;
      nextButton.dataset.slide = next ? next.slug : '';
      nextButton.setAttribute('hx-get', next ? endpointFor(next.slug) : '');
    }
  };

  const loadSlide = (slug) => {
    if (!slug || slug === activeSlug) return;
    const index = slugIndex(slug);
    if (index === -1) return;

    activeSlug = slug;
    updateProgress();

    if (window.htmx && stage) {
      window.htmx.ajax('GET', endpointFor(slug), {
        target: '#slide-stage',
        swap: 'innerHTML transition:true',
      });
    }
  };

  const handleNavClick = (event) => {
    const button = event.target.closest('[data-slide]');
    if (!button || button.disabled) return;
    const targetSlug = button.dataset.slide;
    loadSlide(targetSlug);
  };

  const handleKeydown = (event) => {
    if (event.key === 'ArrowLeft') {
      event.preventDefault();
      const prev = prevButton?.dataset.slide;
      if (prev) loadSlide(prev);
    }
    if (event.key === 'ArrowRight') {
      event.preventDefault();
      const next = nextButton?.dataset.slide;
      if (next) loadSlide(next);
    }
  };

  const toggleNav = () => {
    if (!nav || !navToggle) return;
    const isCollapsed = nav.classList.toggle('is-collapsed');
    navToggle.textContent = isCollapsed ? 'Expand' : 'Collapse';
    navToggle.setAttribute('aria-expanded', isCollapsed ? 'false' : 'true');
  };

  if (nav) {
    nav.addEventListener('click', handleNavClick);
  }
  if (navToggle) {
    navToggle.addEventListener('click', toggleNav);
  }
  if (prevButton) {
    prevButton.addEventListener('click', () => {
      const prev = prevButton.dataset.slide;
      if (prev) loadSlide(prev);
    });
  }
  if (nextButton) {
    nextButton.addEventListener('click', () => {
      const next = nextButton.dataset.slide;
      if (next) loadSlide(next);
    });
  }

  document.addEventListener('keydown', handleKeydown);

  if (window.htmx && stage) {
    window.htmx.on(stage, 'htmx:afterSwap', () => {
      updateProgress();
    });
  }

  updateProgress();
})();
