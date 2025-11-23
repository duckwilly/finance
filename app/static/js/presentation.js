(() => {
  const root = document.querySelector('[data-presentation-root]');
  if (!root) return;

  const slides = JSON.parse(root.dataset.slides || '[]');
  let activeSlug = root.dataset.activeSlide;
  const storageKey = 'finance:presentation:active-slide';
  const endpointTemplate = root.dataset.slideEndpointTemplate;
  const stage = document.querySelector('#slide-stage');
  const stageContainer = document.querySelector('.presentation__stage');
  const nav = document.querySelector('[data-slide-nav]');
  const navList = nav?.querySelector('.presentation__nav-list');
  const navHeader = nav?.querySelector('.presentation__nav-header');
  const layout = document.querySelector('.presentation__layout');
  const navToggle = document.querySelector('[data-nav-toggle]');
  const progressLabel = document.querySelector('[data-progress-label]');
  const prevButton = document.querySelector('[data-prev]');
  const nextButton = document.querySelector('[data-next]');
  const EMBED_SELECTOR = '.slide__embed iframe';

  const slugIndex = (slug) => slides.findIndex((item) => item.slug === slug);
  const endpointFor = (slug) => endpointTemplate.replace('__SLUG__', slug);
  const getActiveSlide = () => slides[slugIndex(activeSlug)];
  const getRoot = (order, fallbackIndex) => {
    const value = order || String(fallbackIndex);
    return value.split('.')[0];
  };

  const syncNavHeight = () => {
    if (!nav) return;
    const reference = stageContainer || stage;
    if (!reference) return;
    const stageHeight = reference.getBoundingClientRect().height;
    if (!stageHeight) return;
    nav.style.maxHeight = `${stageHeight}px`;
    nav.style.height = `${stageHeight}px`;
    if (navList && navHeader) {
      const styles = getComputedStyle(nav);
      const paddingY = parseFloat(styles.paddingTop || '0') + parseFloat(styles.paddingBottom || '0');
      const available = stageHeight - navHeader.offsetHeight - paddingY - 8;
      navList.style.maxHeight = `${Math.max(available, 120)}px`;
    }
  };

  const updateProgress = () => {
    const index = slugIndex(activeSlug);
    const total = slides.length;
    const slide = slides[index];
    if (progressLabel && slide) {
      const activeOrder = slide.order || String(index + 1);
      const finalOrder = slides[total - 1]?.order || String(total);
      const displayIndex = activeOrder.includes('.') ? activeOrder : activeOrder.padStart(2, '0');
      const displayTotal = finalOrder.includes('.') ? finalOrder : String(total).padStart(2, '0');
      progressLabel.textContent = `${displayIndex} / ${displayTotal} — ${slide.title}`;
    }
    updateNav();
    updateArrows();
    syncNavHeight();
  };

  const updateNav = () => {
    if (!nav) return;
    const active = getActiveSlide();
    const activeIndex = slugIndex(activeSlug);
    const activeOrder = active?.order || String(activeIndex + 1);
    const activeRoot = getRoot(activeOrder, activeIndex + 1);
    const navLinks = nav.querySelectorAll('[data-slide]');
    navLinks.forEach((link) => {
      const isActive = link.dataset.slide === activeSlug;
      link.setAttribute('aria-current', isActive ? 'true' : 'false');
      const isSub = link.dataset.isSub === 'true';
      const linkRoot = link.dataset.root || '';
      const shouldShow = !isSub || linkRoot === activeRoot;
      const item = link.closest('li');
      if (item) item.hidden = !shouldShow;
      link.tabIndex = shouldShow ? 0 : -1;
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

  const readStoredSlide = () => {
    try {
      return localStorage.getItem(storageKey);
    } catch {
      return null;
    }
  };

  const persistActiveSlide = (slug) => {
    if (!slug) return;
    try {
      localStorage.setItem(storageKey, slug);
    } catch {
      /* ignore storage errors */
    }
  };

  const loadSlide = (slug) => {
    if (!slug || slug === activeSlug) return;
    const index = slugIndex(slug);
    if (index === -1) return;

    activeSlug = slug;
    persistActiveSlide(slug);
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

  const setNavCollapsed = (collapsed) => {
    if (!nav || !navToggle) return;
    nav.classList.toggle('is-collapsed', collapsed);
    nav.hidden = collapsed;
    navToggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    navToggle.setAttribute('title', collapsed ? 'Show navigation' : 'Hide navigation');
    layout?.classList.toggle('is-nav-collapsed', collapsed);
  };

  const resizeIframe = (iframe) => {
    if (!iframe?.contentWindow || !iframe?.contentDocument) return;
    try {
      const doc = iframe.contentDocument.documentElement;
      const height = doc.scrollHeight;
      if (height) {
        iframe.style.height = `${height + 24}px`;
      }
    } catch (err) {
      console.warn('Could not resize embedded iframe', err);
    }
  };

  const wireEmbeds = (scope = document) => {
    const embeds = scope.querySelectorAll(EMBED_SELECTOR);
    embeds.forEach((frame) => {
      frame.addEventListener('load', () => resizeIframe(frame), { once: true });
      if (frame.contentDocument?.readyState === 'complete') {
        resizeIframe(frame);
      }
    });
  };

  const toggleNav = () => {
    const isCollapsed = nav?.classList.contains('is-collapsed');
    setNavCollapsed(!isCollapsed);
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
      wireEmbeds(stage);
      requestAnimationFrame(syncNavHeight);
    });
  }

  const stored = readStoredSlide();
  const shouldRestore = stored && slugIndex(stored) !== -1 && stored !== activeSlug;
  if (shouldRestore) {
    loadSlide(stored);
  } else {
    persistActiveSlide(activeSlug);
    updateProgress();
  }
  setNavCollapsed(false);
  window.addEventListener('resize', syncNavHeight);
  window.addEventListener('load', syncNavHeight, { once: true });
  wireEmbeds(document);
})();
