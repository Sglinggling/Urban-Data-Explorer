
// Carte choroplèthe interactive des arrondissements parisiens (MapLibre GL).
// Gère le rendu par indicateur, l'animation temporelle, le mode comparaison et les popups.
// Exposé via window.initMap, window.setMapIndicator, window.setMapYear, window.setMapHighlight.

(function () {
  'use strict';

  // Constantes

  const API_BASE = 'http://localhost:8000/api';
  const API_KEY = 'urban-data-explorer-dev-key';
  const GEOJSON_PATH = 'data/arrondissements.geojson';

  const MAP_STYLES = {
    dark:  'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
    light: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
  };

  // Registre de tous les indicateurs disponibles : endpoint API, clé de valeur, palette et formateur d'affichage
  const MAP_INDICATORS = {
    prix_m2: {
      label: 'Prix médian / m²',
      endpoint: '/prix',
      yrParam: 'annee',
      valKey: 'prix_m2_median',
      hasYear: true,
      pal: 'seq',
      fmt: (v) => `${Math.round(v).toLocaleString('fr-FR')} €/m²`,
    },
    variation_pct: {
      label: 'Variation prix / m²',
      endpoint: '_computed',
      valKey: '_computed',
      hasYear: true,
      pal: 'div',
      fmt: (v) => (v >= 0 ? '+' : '') + v.toFixed(1) + ' %',
      minYear: 2021,
    },
    logements_sociaux_pct: {
      label: 'Logements sociaux',
      endpoint: '/logements_sociaux',
      yrParam: 'annee',
      valKey: 'logements_sociaux_pct',
      hasYear: true,
      pal: 'seq',
      fmt: (v) => v.toFixed(1) + ' %',
    },
    part_studio_pct: {
      label: 'Part studios',
      endpoint: '/typologie',
      yrParam: 'annee',
      valKey: 'part_studio_pct',
      hasYear: true,
      pal: 'seq',
      fmt: (v) => v.toFixed(1) + ' %',
    },
    part_T2_pct: {
      label: 'Part T2',
      endpoint: '/typologie',
      yrParam: 'annee',
      valKey: 'part_T2_pct',
      hasYear: true,
      pal: 'seq',
      fmt: (v) => v.toFixed(1) + ' %',
    },
    part_T3plus_pct: {
      label: 'Taille des logements',
      endpoint: '/typologie',
      yrParam: 'annee',
      valKey: 'part_T3plus_pct',
      hasYear: true,
      pal: 'seq',
      fmt: (v) => v.toFixed(1) + ' %',
    },
    iti: {
      label: 'Tension Immobilière (ITI)',
      endpoint: '/iti',
      yrParam: 'annee',
      valKey: 'iti',
      hasYear: true,
      minYear: 2022,
      pal: 'iti',
      fmt: (v) => v.toFixed(1) + ' / 100',
    },
    iqv: {
      label: 'Qualité de vie (IQV)',
      endpoint: '/iqv',
      yrParam: 'annee',
      valKey: 'iqv_score',
      hasYear: true,
      pal: 'seq',
      fmt: (v) => v.toFixed(1) + ' / 100',
    },
    iam: {
      label: 'Activité du marché (IAM)',
      endpoint: '/iam',
      yrParam: 'annee',
      valKey: 'iam_score',
      hasYear: true,
      pal: 'seq',
      fmt: (v) => v.toFixed(1) + ' / 100',
    },
    ipr: {
      label: 'Pression résidentielle (IPR)',
      endpoint: '/ipr',
      yrParam: 'annee',
      valKey: 'ipr_score',
      hasYear: true,
      pal: 'iti',
      fmt: (v) => v.toFixed(1) + ' / 100',
    },
    // Indicateurs statiques conservés pour la vue détail arrondissement
    surface_ev: {
      label: 'Espaces verts (ha)',
      endpoint: '/espaces_verts',
      valKey: 'surface_totale_m2',
      hasYear: false,
      logScale: true,
      pal: 'seq',
      fmt: (v) => `${(v / 10000).toFixed(0)} ha`,
    },
    nb_abribacs: {
      label: 'Nb abribacs / PAVDA',
      endpoint: '/abribacs',
      valKey: 'nb_abribacs',
      hasYear: false,
      pal: 'seq',
      fmt: (v) => `${Math.round(v)} bacs`,
    },
  };

  const MAP_PALETTES = {
    seq: ['#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c', '#fc4e2a', '#e31a1c', '#b10026'],
    div: ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850'],
    // jaune pâle (détendu) → orange → rouge foncé (très tendu) — échelle fixe 0-100
    iti: ['#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c', '#fc4e2a', '#e31a1c', '#b10026'],
  };

  // State

  const mapS = {
    map: null,
    popup: null,
    hoveredId: null,
    geoJson: null,
    centroids: {},
    cache: {},
    allPrixRaw: null,
    itiRows: {},        // { year: { arrNum: {prix_norm, variation_norm, sociaux_inv_norm, iti} } }
    indicator: 'prix_m2',
    year: 2024,
    playing: false,
    playTimer: null,
    compareMode: false,
    compareArr: [],
    layers: { abribacs: false },
  };

  // Verrou pour éviter les boucles de synchronisation entre le slider et le dropdown année
  let _yearSyncInProgress = false;

  // Public entry point

  window.initMap = async function initMap() {
    const _initTheme = localStorage.getItem('urban-data-theme') || 'dark';
    mapS.map = new maplibregl.Map({
      container: 'map',
      style: MAP_STYLES[_initTheme] || MAP_STYLES.dark,
      center: [2.3522, 48.8566],
      zoom: 11.5,
      minZoom: 11,
      maxZoom: 16,
      maxBounds: [[2.20, 48.78], [2.50, 48.92]],
      attributionControl: false,
    });

    mapS.map.addControl(
      new maplibregl.AttributionControl({ compact: true }),
      'bottom-left'
    );

    // 'idle' est plus fiable que 'load' : attend que le style,
    // les tuiles, les glyphs et les sprites soient entièrement rendus.
    mapS.map.once('idle', async () => {
      await mapLoadGeoJson();
      mapAddLayers();
      mapSetupInteractions();
      await mapPrefetchStatic();
      await mapUpdateChoropleth();
      // await mapInitPoints();
      mapSetupControls();
    });
  };

  // Charge le GeoJSON des arrondissements, enrichit chaque feature avec son numéro et son centroïde
  async function mapLoadGeoJson() {
    try {
      const resp = await fetch(GEOJSON_PATH);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const raw = await resp.json();

      raw.features.forEach((f) => {
        const arrNum = f.properties.c_ar;
        f.id = arrNum;
        f.properties.arrNum = arrNum;
        f.properties.arrName = f.properties.l_ar || `${arrNum}e Ardt`;
        f.properties.mapValue = null;

        const gxy = f.properties.geom_x_y;
        if (gxy && gxy.lon !== undefined && gxy.lat !== undefined) {
          mapS.centroids[arrNum] = [gxy.lon, gxy.lat];
        }
      });

      mapS.geoJson = raw;

      mapS.map.addSource('arrondissements', {
        type: 'geojson',
        data: raw,
        promoteId: 'c_ar',
      });
    } catch (err) {
      console.warn('[map] mapLoadGeoJson error:', err);
    }
  }

  // Ajoute les calques MapLibre : remplissage choroplèthe, contours, surbrillance comparaison et étiquettes
  function mapAddLayers() {
    const _theme = document.documentElement.getAttribute('data-theme') || 'dark';
    const _isLight = _theme === 'light';

    // Fill layer
    mapS.map.addLayer({
      id: 'arr-fill',
      type: 'fill',
      source: 'arrondissements',
      paint: {
        'fill-color': '#888888',
        'fill-opacity': [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          0.92,
          0.72,
        ],
      },
    });

    // Outline layer
    mapS.map.addLayer({
      id: 'arr-line',
      type: 'line',
      source: 'arrondissements',
      paint: {
        'line-color': [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          '#fbbf24',
          '#ffffff',
        ],
        'line-width': [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          2,
          0.5,
        ],
      },
    });

    // Compare highlight layer
    mapS.map.addLayer({
      id: 'arr-compare',
      type: 'line',
      source: 'arrondissements',
      paint: {
        'line-color': '#38bdf8',
        'line-width': 3,
      },
      filter: ['==', ['get', 'arrNum'], -1],
    });

    // Arrondissement number labels
    mapS.map.addLayer({
      id: 'arr-labels',
      type: 'symbol',
      source: 'arrondissements',
      layout: {
        'text-field': ['to-string', ['get', 'c_ar']],
        'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
        'text-size': [
          'interpolate', ['linear'], ['zoom'],
          10, 11,
          13, 18,
        ],
        'text-anchor': 'center',
        'text-allow-overlap': false,
        'text-ignore-placement': false,
      },
      paint: {
        'text-color': _isLight ? '#1e293b' : '#ffffff',
        'text-halo-color': _isLight ? 'rgba(255,255,255,0.85)' : 'rgba(15, 30, 58, 0.85)',
        'text-halo-width': 1.5,
        'text-halo-blur': 0.5,
        'text-opacity': 0.92,
      },
    });
  }

  // Effectue un appel API authentifié et retourne le JSON, ou un tableau vide en cas d'erreur
  async function mapFetch(path, params = {}) {
    try {
      const url = new URL(API_BASE + path);
      Object.entries(params).forEach(([k, v]) => {
        if (v !== undefined && v !== null && v !== '') {
          url.searchParams.append(k, v);
        }
      });
      const resp = await fetch(url, { headers: { 'X-API-Key': API_KEY } });
      if (!resp.ok) throw new Error(`HTTP ${resp.status} on ${path}`);
      return await resp.json();
    } catch (err) {
      console.warn('[map] mapFetch error:', err);
      return [];
    }
  }

  // Transforme un tableau de lignes API en dictionnaire { arrondissement → valeur numérique }
  function mapBuildValueMap(rows, cfg) {
    const result = {};
    if (!Array.isArray(rows)) return result;
    rows.forEach((row) => {
      const arr = Number(row.arrondissement);
      if (arr && row[cfg.valKey] !== undefined && row[cfg.valKey] !== null) {
        result[arr] = Number(row[cfg.valKey]);
      }
    });
    return result;
  }

  // Calcule la variation annuelle du prix au m² par arrondissement :
  // variation(arr, année) = (prix[année] - prix[année-1]) / prix[année-1] × 100
  function mapComputeVariation(year) {
    const raw = mapS.allPrixRaw;
    if (!Array.isArray(raw) || raw.length === 0) return {};

    const byArr = {};
    raw.forEach((row) => {
      const arr = Number(row.arrondissement);
      const yr = Number(row.annee);
      if (!byArr[arr]) byArr[arr] = {};
      byArr[arr][yr] = Number(row.prix_m2_median);
    });

    const result = {};
    Object.entries(byArr).forEach(([arr, years]) => {
      const pCurr = years[year];
      const pPrev = years[year - 1];
      if (pCurr !== undefined && pPrev !== undefined && pPrev !== 0) {
        result[Number(arr)] = ((pCurr - pPrev) / pPrev) * 100;
      }
    });
    return result;
  }

  // Pré-charge en parallèle les indicateurs statiques et l'historique complet des prix
  async function mapPrefetchStatic() {
    const staticKeys = Object.keys(MAP_INDICATORS).filter(
      (k) => !MAP_INDICATORS[k].hasYear && MAP_INDICATORS[k].endpoint !== '_computed'
    );

    const fetches = staticKeys.map(async (key) => {
      const cfg = MAP_INDICATORS[key];
      const cacheKey = `${key}_all`;
      if (!mapS.cache[cacheKey]) {
        const rows = await mapFetch(cfg.endpoint);
        mapS.cache[cacheKey] = mapBuildValueMap(rows, cfg);
      }
    });

    // Fetch all prix (no params) for variation computation
    const fetchAllPrix = async () => {
      const rows = await mapFetch('/prix');
      mapS.allPrixRaw = Array.isArray(rows) ? rows : [];
    };

    await Promise.all([...fetches, fetchAllPrix()]);

    // Pre-warm cache for current year
    await mapFetchForIndicator('prix_m2', mapS.year);
  }

  // Retourne le dictionnaire { arrondissement → valeur } pour un indicateur et une année,
  // en exploitant le cache pour éviter les appels API redondants
  async function mapFetchForIndicator(indicator, year) {
    const cfg = MAP_INDICATORS[indicator];
    if (!cfg) return {};

    // variation_pct est calculé côté client à partir de allPrixRaw, pas depuis l'API
    if (indicator === 'variation_pct') {
      const cacheKey = `variation_pct_${year}`;
      if (!mapS.cache[cacheKey]) {
        mapS.cache[cacheKey] = mapComputeVariation(year);
      }
      return mapS.cache[cacheKey];
    }

    // Static indicators — year is irrelevant
    if (!cfg.hasYear) {
      const cacheKey = `${indicator}_all`;
      // Re-fetch si absent OU si le cache est vide (prefetch raté)
      const cached = mapS.cache[cacheKey];
      if (!cached || Object.keys(cached).length === 0) {
        const rows = await mapFetch(cfg.endpoint);
        mapS.cache[cacheKey] = mapBuildValueMap(rows, cfg);
      }
      return mapS.cache[cacheKey];
    }

    // Year-dependent indicators
    const cacheKey = `${indicator}_${year}`;
    const cachedYear = mapS.cache[cacheKey];
    if (!cachedYear || Object.keys(cachedYear).length === 0) {
      const params = cfg.yrParam ? { [cfg.yrParam]: year } : {};
      const rows = await mapFetch(cfg.endpoint, params);
      mapS.cache[cacheKey] = mapBuildValueMap(rows, cfg);
      // Stocke les lignes ITI brutes pour afficher la décomposition dans le tooltip
      if (indicator === 'iti' && Array.isArray(rows)) {
        mapS.itiRows[year] = {};
        rows.forEach((r) => {
          mapS.itiRows[year][Number(r.arrondissement)] = r;
        });
      }
    }
    return mapS.cache[cacheKey];
  }

  // Met à jour la choroplèthe : récupère les données, colorie les polygones et rafraîchit la légende
  async function mapUpdateChoropleth() {
    if (!mapS.map || !mapS.geoJson) return;

    const cfg = MAP_INDICATORS[mapS.indicator];
    if (!cfg) return;

    // Repli sur l'année minimale si l'indicateur n'est pas disponible avant une certaine date
    const effectiveYear =
      cfg.minYear && mapS.year < cfg.minYear ? cfg.minYear : mapS.year;

    const valueMap = await mapFetchForIndicator(mapS.indicator, effectiveYear);

    const annotated = mapAnnotateGeoJson(valueMap);
    const source = mapS.map.getSource('arrondissements');
    if (source) source.setData(annotated);

    const values = Object.values(valueMap).filter((v) => typeof v === 'number' && isFinite(v));
    const colorExpr = mapBuildColorExpression(values, cfg.pal, cfg.logScale || false);
    mapS.map.setPaintProperty('arr-fill', 'fill-color', colorExpr);

    mapUpdateLegend(values, cfg);

    // Update all year UI elements
    const headerEl = document.getElementById('map-section-title');
    if (headerEl) headerEl.textContent = 'Carte — ' + cfg.label;

    const yearDisplayVal = cfg.hasYear ? mapS.year : '—';
    const yearEl = document.getElementById('year-display');
    if (yearEl) yearEl.textContent = yearDisplayVal;

    // Show/hide static badge
    const staticBadge = document.getElementById('map-static-badge');
    if (staticBadge) staticBadge.classList.toggle('hidden', !!cfg.hasYear);
  }

  // Injecte la valeur mapValue dans les propriétés de chaque feature GeoJSON pour le rendu MapLibre
  function mapAnnotateGeoJson(valueMap) {
    if (!mapS.geoJson) return { type: 'FeatureCollection', features: [] };
    return {
      ...mapS.geoJson,
      features: mapS.geoJson.features.map((f) => ({
        ...f,
        properties: {
          ...f.properties,
          mapValue: valueMap[f.properties.arrNum] !== undefined
            ? valueMap[f.properties.arrNum]
            : null,
        },
      })),
    };
  }

  // Génère l'expression de couleur MapLibre GL pour la choroplèthe selon le type de palette :
  // séquentielle linéaire, divergente symétrique autour de 0, logarithmique ou échelle fixe ITI
  function mapBuildColorExpression(values, palType, logScale) {
    const noDataColor = '#4b5563';

    if (!values || values.length === 0) {
      return noDataColor;
    }

    let minVal = Math.min(...values);
    let maxVal = Math.max(...values);

    // Ensure range is meaningful
    if (Math.abs(maxVal - minVal) < 0.01) {
      minVal = minVal - 0.005;
      maxVal = maxVal + 0.005;
    }

    const palette = MAP_PALETTES[palType] || MAP_PALETTES.seq;
    const stops = [];

    if (palType === 'iti') {
      // Échelle fixe 0-100 pour l'ITI (pas de distorsion par min-max)
      const step = 100 / (palette.length - 1);
      palette.forEach((color, i) => {
        stops.push(i * step, color);
      });
    } else if (palType === 'div') {
      // Palette divergente à 9 arrêts, symétrique autour de 0 :
      // stops[i] = -absMax + i × (2 × absMax / (n-1))
      const absMax = Math.max(Math.abs(minVal), Math.abs(maxVal));
      const safeAbsMax = absMax < 0.005 ? 0.005 : absMax;
      const step = (safeAbsMax * 2) / (palette.length - 1);
      palette.forEach((color, i) => {
        stops.push(-safeAbsMax + i * step, color);
      });
    } else if (logScale) {
      // Positions logarithmiques : stops[i] = 10^(logMin + i/(n-1) × logRange) - 1
      const safeMin = Math.max(0, minVal);
      const logMin = Math.log10(safeMin + 1);
      const logMax = Math.log10(maxVal + 1);
      const logRange = logMax - logMin || 0.001;
      palette.forEach((color, i) => {
        const logPos = logMin + (i / (palette.length - 1)) * logRange;
        stops.push(Math.pow(10, logPos) - 1, color);
      });
    } else {
      // 8-stop sequential, linear from min to max
      const step = (maxVal - minVal) / (palette.length - 1);
      palette.forEach((color, i) => {
        stops.push(minVal + i * step, color);
      });
    }

    return [
      'case',
      ['!=', ['typeof', ['get', 'mapValue']], 'number'],
      noDataColor,
      ['interpolate', ['linear'], ['get', 'mapValue'], ...stops],
    ];
  }

  // Construit et injecte la légende HTML (min / médiane / max) pour l'indicateur actif
  function mapUpdateLegend(values, cfg) {
    const el = document.getElementById('map-legend');
    if (!el) return;

    if (!values || values.length === 0) {
      el.innerHTML = '<span style="color:#94a3b8;font-size:0.8rem;">Aucune donnée</span>';
      return;
    }

    const sorted = [...values].sort((a, b) => a - b);
    const minVal = sorted[0];
    const maxVal = sorted[sorted.length - 1];
    const medianVal = sorted[Math.floor(sorted.length / 2)];

    const gradient = cfg.pal === 'div'
      ? 'linear-gradient(to right, #dc2626, #facc15, #16a34a)'
      : 'linear-gradient(to right, #fef3c7, #fb923c, #b91c1c)';

    const logNote = cfg.logScale
      ? '<div class="map-legend__note">Échelle logarithmique</div>'
      : '';
    el.innerHTML = `
      <div class="map-legend__title">${cfg.label}</div>
      ${logNote}
      <div class="map-legend__gradient" style="background:${gradient};"></div>
      <div class="map-legend__scale">
        <div class="map-legend__tick">
          <span class="map-legend__label">Min</span>
          <span class="map-legend__value">${cfg.fmt(minVal)}</span>
        </div>
        <div class="map-legend__tick">
          <span class="map-legend__label">Médian</span>
          <span class="map-legend__value">${cfg.fmt(medianVal)}</span>
        </div>
        <div class="map-legend__tick">
          <span class="map-legend__label">Max</span>
          <span class="map-legend__value">${cfg.fmt(maxVal)}</span>
        </div>
      </div>
    `;
  }

  // Initialise les calques de points (abribacs) positionnés sur les centroïdes des arrondissements
  async function mapInitPoints() {
    try {
      const abribacsData = await mapFetch('/abribacs');

      const pointDefs = [
        {
          id: 'abribacs',
          color: '#34d399',
          countKey: 'nb_abribacs',
          label: 'Abribacs / PAVDA',
          rows: abribacsData,
        },
      ];

      pointDefs.forEach((def) => {
        const geojson = mapBuildCentroidGeoJson(def.rows, def.countKey, def.label);
        const srcId = `pts-${def.id}`;
        const layerId = `pts-layer-${def.id}`;

        mapS.map.addSource(srcId, { type: 'geojson', data: geojson });
        mapS.map.addLayer({
          id: layerId,
          type: 'circle',
          source: srcId,
          layout: { visibility: 'none' },
          paint: {
            'circle-color': def.color,
            'circle-opacity': 0.82,
            'circle-stroke-width': 1,
            'circle-stroke-color': '#ffffff',
            // Rayon du cercle proportionnel au nombre de bacs (interpolation linéaire)
            'circle-radius': [
              'interpolate',
              ['linear'],
              ['get', 'count'],
              0, 4,
              15, 7,
              50, 12,
              100, 18,
            ],
          },
        });

        // Hover popup for point layers
        mapS.map.on('mouseenter', layerId, (e) => {
          mapS.map.getCanvas().style.cursor = 'pointer';
          const feat = e.features && e.features[0];
          if (!feat) return;
          const coords = feat.geometry.coordinates.slice();
          const props = feat.properties;
          if (mapS.popup) mapS.popup.remove();
          mapS.popup = new maplibregl.Popup({ closeButton: false, offset: 10 })
            .setLngLat(coords)
            .setHTML(`
              <div style="font-family:sans-serif;font-size:0.85rem;">
                <strong style="color:#f8fafc;">${props.type}</strong><br/>
                <span style="color:#9ba9c2;">${props.arrName}</span><br/>
                <span style="color:#fbbf24;font-weight:700;">${Math.round(props.count)}</span> bacs
              </div>
            `)
            .addTo(mapS.map);
        });

        mapS.map.on('mouseleave', layerId, () => {
          mapS.map.getCanvas().style.cursor = '';
          if (mapS.popup) {
            mapS.popup.remove();
            mapS.popup = null;
          }
        });
      });
    } catch (err) {
      console.warn('[map] mapInitPoints error:', err);
    }
  }

  // Construit un GeoJSON de points en positionnant chaque arrondissement sur son centroïde
  function mapBuildCentroidGeoJson(rows, countKey, type) {
    const features = [];
    if (!Array.isArray(rows)) return { type: 'FeatureCollection', features };

    rows.forEach((row) => {
      const arr = Number(row.arrondissement);
      const coords = mapS.centroids[arr];
      if (!coords) return;
      const count = row[countKey] !== undefined ? Number(row[countKey]) : 0;
      const arrName = row.l_ar || `${arr}e Ardt`;
      features.push({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: coords },
        properties: { arrNum: arr, arrName, count, type },
      });
    });

    return { type: 'FeatureCollection', features };
  }

  function mapSetPointVisible(layerId, visible) {
    if (!mapS.map) return;
    try {
      mapS.map.setLayoutProperty(
        layerId,
        'visibility',
        visible ? 'visible' : 'none'
      );
    } catch (err) {
      console.warn('[map] mapSetPointVisible error:', err);
    }
  }

  function mapSetupInteractions() {
    mapS.map.on('mousemove', 'arr-fill', mapOnHover);
    mapS.map.on('mouseleave', 'arr-fill', mapOnLeave);
    mapS.map.on('click', 'arr-fill', mapOnClick);

    mapS.map.on('mouseenter', 'arr-fill', () => {
      mapS.map.getCanvas().style.cursor = 'pointer';
    });
    mapS.map.on('mouseleave', 'arr-fill', () => {
      mapS.map.getCanvas().style.cursor = '';
    });
  }

  // Gère le survol d'un arrondissement : met à jour le feature-state hover et affiche le popup contextuel
  function mapOnHover(e) {
    if (!e.features || e.features.length === 0) return;

    const feat = e.features[0];
    const arrNum = feat.properties.arrNum;
    const arrName = feat.properties.arrName;
    const mapValue = feat.properties.mapValue;

    // Réinitialise le hover sur le précédent arrondissement survolé
    if (mapS.hoveredId !== null && mapS.hoveredId !== arrNum) {
      mapS.map.setFeatureState(
        { source: 'arrondissements', id: mapS.hoveredId },
        { hover: false }
      );
    }
    mapS.hoveredId = arrNum;
    mapS.map.setFeatureState(
      { source: 'arrondissements', id: arrNum },
      { hover: true }
    );

    const cfg = MAP_INDICATORS[mapS.indicator];
    const valStr =
      mapValue !== null && mapValue !== undefined && typeof mapValue === 'number'
        ? cfg.fmt(mapValue)
        : '—';

    const ctx = mapGetContextStats(arrNum);

    // Bloc de décomposition ITI (prix norm., variation norm., impact social) affiché uniquement pour l'indicateur ITI
    let itiDecomp = '';
    if (mapS.indicator === 'iti') {
      const itiRow = (mapS.itiRows[mapS.year] || {})[arrNum];
      if (itiRow) {
        const itiColor = itiRow.iti > 70 ? '#ef4444' : itiRow.iti >= 40 ? '#fb923c' : '#4ade80';
        itiDecomp = `
          <div style="font-size:0.76rem;color:#9ba9c2;border-top:1px solid rgba(255,255,255,0.1);padding-top:5px;margin-top:5px;display:grid;gap:2px;">
            <div>Prix norm. : <span style="color:#fbbf24;">${itiRow.prix_norm.toFixed(1)}</span></div>
            <div>Variation norm. : <span style="color:#fbbf24;">${itiRow.variation_norm.toFixed(1)}</span></div>
            <div>Impact social (inv.) : <span style="color:#fbbf24;">${itiRow.sociaux_inv_norm.toFixed(1)}</span></div>
            <div style="margin-top:3px;">ITI : <span style="color:${itiColor};font-weight:700;">${itiRow.iti.toFixed(1)} / 100</span></div>
          </div>`;
      }
    }

    let compareNote = '';
    if (mapS.compareMode) {
      if (mapS.compareArr.includes(arrNum)) {
        compareNote = `<div style="margin-top:6px;color:#38bdf8;font-size:0.78rem;">Sélectionné pour comparaison</div>`;
      } else {
        compareNote = `<div style="margin-top:6px;color:#9ba9c2;font-size:0.78rem;font-style:italic;">Cliquez pour sélectionner</div>`;
      }
    }

    const html = `
      <div style="font-family:sans-serif;min-width:180px;">
        <div style="font-weight:700;font-size:0.95rem;color:#f8fafc;margin-bottom:6px;">${arrName}</div>
        <div style="margin-bottom:8px;">
          <span style="color:#9ba9c2;font-size:0.8rem;">${cfg.label}</span><br/>
          <span style="color:#fbbf24;font-weight:700;font-size:1rem;">${valStr}</span>
        </div>
        ${itiDecomp}
        <div style="font-size:0.78rem;color:#9ba9c2;border-top:1px solid rgba(255,255,255,0.1);margin-top:8px;padding-top:8px;display:grid;gap:4px;line-height:1.6;">
          <div>Prix/m² : <span style="color:#e5e7eb;">${ctx.prix}</span></div>
          <div>Log. sociaux : <span style="color:#e5e7eb;">${ctx.log}</span></div>
          <div>Espaces verts : <span style="color:#e5e7eb;">${ctx.ev}</span></div>
          <div>Abribacs : <span style="color:#e5e7eb;">${ctx.abri}</span></div>
        </div>
        ${compareNote}
      </div>
    `;

    if (mapS.popup) mapS.popup.remove();
    mapS.popup = new maplibregl.Popup({
      closeButton: false,
      closeOnClick: false,
      offset: 12,
      maxWidth: '260px',
    })
      .setLngLat(e.lngLat)
      .setHTML(html)
      .addTo(mapS.map);
  }

  function mapOnLeave() {
    if (mapS.hoveredId !== null) {
      mapS.map.setFeatureState(
        { source: 'arrondissements', id: mapS.hoveredId },
        { hover: false }
      );
      mapS.hoveredId = null;
    }
    if (mapS.popup) {
      mapS.popup.remove();
      mapS.popup = null;
    }
    mapS.map.getCanvas().style.cursor = '';
  }

  // Délègue le clic vers le mode comparaison ou vers le callback main.js selon l'état courant
  function mapOnClick(e) {
    if (!e.features || e.features.length === 0) return;
    const arr = e.features[0].properties.arrNum;

    if (mapS.compareMode) {
      mapHandleCompareClick(arr);
    } else {
      // Notify main.js if the callback is available
      if (typeof window.onMapArrondissementClick === 'function') {
        window.onMapArrondissementClick(arr);
      }
      mapHighlightArr(arr);
    }
  }

  function mapHighlightArr(arr) {
    // Réinitialise le hover sur tous les arrondissements puis l'applique à l'arrondissement cliqué
    if (mapS.geoJson) {
      mapS.geoJson.features.forEach((f) => {
        mapS.map.setFeatureState(
          { source: 'arrondissements', id: f.id },
          { hover: false }
        );
      });
    }
    mapS.map.setFeatureState(
      { source: 'arrondissements', id: arr },
      { hover: true }
    );
    mapS.hoveredId = arr;
  }

  // Lit les valeurs actuelles depuis le cache pour afficher un résumé multi-indicateurs dans le popup
  function mapGetContextStats(arr) {
    const prixCache = mapS.cache[`prix_m2_${mapS.year}`] || {};
    const logCache = mapS.cache[`logements_sociaux_pct_${mapS.year}`] || {};
    const evCache = mapS.cache['surface_ev_all'] || {};
    const abriCache = mapS.cache['nb_abribacs_all'] || {};

    const fmtPrix = MAP_INDICATORS.prix_m2.fmt;
    const fmtLog = MAP_INDICATORS.logements_sociaux_pct.fmt;
    const fmtEv = MAP_INDICATORS.surface_ev.fmt;
    const fmtAbri = MAP_INDICATORS.nb_abribacs.fmt;

    const safe = (map, key, fmt) => {
      const v = map[key];
      return v !== undefined && v !== null ? fmt(v) : '—';
    };

    return {
      prix: safe(prixCache, arr, fmtPrix),
      log: safe(logCache, arr, fmtLog),
      ev: safe(evCache, arr, fmtEv),
      abri: safe(abriCache, arr, fmtAbri),
    };
  }

  // Gère la sélection/désélection d'un arrondissement en mode comparaison (max 2, FIFO si dépassé)
  function mapHandleCompareClick(arr) {
    const idx = mapS.compareArr.indexOf(arr);
    if (idx !== -1) {
      // Toggle off
      mapS.compareArr.splice(idx, 1);
    } else {
      if (mapS.compareArr.length >= 2) {
        // Replace the first one (shift)
        mapS.compareArr.shift();
      }
      mapS.compareArr.push(arr);
    }

    // Met à jour le filtre du calque de surbrillance pour les deux arrondissements sélectionnés
    const a1 = mapS.compareArr[0] !== undefined ? mapS.compareArr[0] : -1;
    const a2 = mapS.compareArr[1] !== undefined ? mapS.compareArr[1] : -1;
    mapS.map.setFilter('arr-compare', [
      'any',
      ['==', ['get', 'arrNum'], a1],
      ['==', ['get', 'arrNum'], a2],
    ]);

    if (mapS.compareArr.length === 2) {
      mapRenderComparison(mapS.compareArr[0], mapS.compareArr[1]);
    }
  }

  // Affiche le panneau de comparaison côte-à-côte et synchronise les sélecteurs Chart.js
  async function mapRenderComparison(arr1, arr2) {
    const panel = document.getElementById('comparison-panel');
    if (panel) panel.style.display = 'block';

    const ctx1 = mapGetContextStats(arr1);
    const ctx2 = mapGetContextStats(arr2);

    const name1 = mapS.geoJson
      ? (mapS.geoJson.features.find((f) => f.properties.arrNum === arr1) || {}).properties?.arrName || `${arr1}e`
      : `${arr1}e`;
    const name2 = mapS.geoJson
      ? (mapS.geoJson.features.find((f) => f.properties.arrNum === arr2) || {}).properties?.arrName || `${arr2}e`
      : `${arr2}e`;

    const renderCard = (ctx, name) => `
      <div style="font-family:sans-serif;">
        <div style="font-weight:700;font-size:1rem;color:#fbbf24;margin-bottom:8px;">${name}</div>
        <div style="font-size:0.85rem;display:grid;gap:4px;color:#e5e7eb;">
          <div><span style="color:#9ba9c2;">Prix/m² (${mapS.year}) :</span> ${ctx.prix}</div>
          <div><span style="color:#9ba9c2;">Log. sociaux :</span> ${ctx.log}</div>
          <div><span style="color:#9ba9c2;">Espaces verts :</span> ${ctx.ev}</div>
          <div><span style="color:#9ba9c2;">Abribacs :</span> ${ctx.abri}</div>
        </div>
      </div>
    `;

    const colA = document.getElementById('compare-col-a');
    if (colA) colA.innerHTML = renderCard(ctx1, name1);
    const colB = document.getElementById('compare-col-b');
    if (colB) colB.innerHTML = renderCard(ctx2, name2);

    // Synchronise les selects de comparaison Chart.js et déclenche le recalcul
    const selA = document.getElementById('arr-compare-a');
    const selB = document.getElementById('arr-compare-b');
    const btnCmp = document.getElementById('btn-compare');

    if (selA) selA.value = String(arr1);
    if (selB) selB.value = String(arr2);
    if (btnCmp) btnCmp.click();
  }

  function mapEnterCompareMode() {
    mapS.compareMode = true;
    mapS.compareArr = [];

    // Reset compare layer filter
    if (mapS.map) {
      mapS.map.setFilter('arr-compare', ['==', ['get', 'arrNum'], -1]);
    }

    const banner = document.getElementById('map-compare-banner');
    if (banner) banner.style.display = 'flex';

    const btn = document.getElementById('btn-compare-mode');
    if (btn) {
      btn.textContent = 'Quitter comparaison';
      btn.style.borderColor = '#38bdf8';
      btn.style.boxShadow = '0 0 0 2px rgba(56, 189, 248, 0.25)';
    }
  }

  function mapExitCompareMode() {
    mapS.compareMode = false;
    mapS.compareArr = [];

    if (mapS.map) {
      mapS.map.setFilter('arr-compare', ['==', ['get', 'arrNum'], -1]);
    }

    const banner = document.getElementById('map-compare-banner');
    if (banner) banner.style.display = 'none';

    const panel = document.getElementById('comparison-panel');
    if (panel) panel.style.display = 'none';

    const btn = document.getElementById('btn-compare-mode');
    if (btn) {
      btn.textContent = 'Mode comparaison';
      btn.style.borderColor = '';
      btn.style.boxShadow = '';
    }
  }

  // Active ou désactive visuellement les contrôles temporels selon que l'indicateur possède une dimension année
  function updateTemporalControls(isTemporal) {
    const yearControl = document.getElementById('year-control');
    if (yearControl) {
      yearControl.style.opacity = isTemporal ? '1' : '0.4';
      yearControl.style.pointerEvents = isTemporal ? '' : 'none';
    }

    const slider = document.getElementById('map-year-slider');
    if (slider) slider.disabled = !isTemporal;

    const btnPlay = document.getElementById('btn-play-map');
    if (btnPlay) btnPlay.disabled = !isTemporal;

    const anneeInput = document.getElementById('annee-input');
    if (anneeInput) {
      anneeInput.disabled = !isTemporal;
      anneeInput.style.opacity = isTemporal ? '' : '0.4';
      anneeInput.style.cursor = isTemporal ? '' : 'not-allowed';
    }

    const anneeLabel = document.querySelector('label[for="annee-input"]');
    if (anneeLabel) anneeLabel.style.opacity = isTemporal ? '' : '0.4';
  }

  // Change l'indicateur affiché sur la carte et met à jour les contrôles temporels en conséquence
  window.setMapIndicator = function(indicator) {
    mapS.indicator = indicator;
    const cfg = MAP_INDICATORS[indicator];
    const isTemporal = !!(cfg && cfg.hasYear);

    if (!isTemporal && mapS.playing) mapStopPlay();

    updateTemporalControls(isTemporal);

    const staticBadge = document.getElementById('map-static-badge');
    if (staticBadge) staticBadge.classList.toggle('hidden', isTemporal);

    //mapSetPointVisible('pts-layer-abribacs', indicator === 'nb_abribacs');
    mapUpdateChoropleth();
  };

  window.getMapIndicator = function() {
    return mapS.indicator;
  };

  // Synchronise l'année courante entre la carte et le dropdown Chart.js sans provoquer de boucle d'événements
  window.setMapYear = function(year) {
    if (_yearSyncInProgress) return;
    _yearSyncInProgress = true;
    mapS.year = year;
    const slider = document.getElementById('map-year-slider');
    if (slider) slider.value = year;
    const disp = document.getElementById('year-display');
    if (disp) disp.textContent = year;
    mapUpdateChoropleth();
    _yearSyncInProgress = false;
  };

  // Met en surbrillance jusqu'à deux arrondissements depuis l'extérieur (ex. : graphique comparaison)
  window.setMapHighlight = function(arrNums) {
    if (!mapS.map) return;
    mapS.compareArr = Array.isArray(arrNums) ? arrNums.slice() : [];
    const a1 = mapS.compareArr[0] !== undefined ? mapS.compareArr[0] : -1;
    const a2 = mapS.compareArr[1] !== undefined ? mapS.compareArr[1] : -1;
    mapS.map.setFilter('arr-compare', [
      'any',
      ['==', ['get', 'arrNum'], a1],
      ['==', ['get', 'arrNum'], a2],
    ]);
  };

  // Branche les événements UI : slider année, bouton lecture, mode comparaison et boutons de sortie
  function mapSetupControls() {
    // Year slider — synchronisé avec #annee-input (dropdown charts)
    const slider = document.getElementById('map-year-slider');
    if (slider) {
      slider.value = mapS.year;
      slider.addEventListener('input', () => {
        if (_yearSyncInProgress) return;
        _yearSyncInProgress = true;
        mapS.year = Number(slider.value);
        const disp = document.getElementById('year-display');
        if (disp) disp.textContent = mapS.year;
        mapUpdateChoropleth();
        const anneeInput = document.getElementById('annee-input');
        if (anneeInput) {
          anneeInput.value = String(mapS.year);
          anneeInput.dispatchEvent(new Event('change'));
        }
        _yearSyncInProgress = false;
      });
    }

    // Play button
    const btnPlay = document.getElementById('btn-play-map');
    if (btnPlay) {
      btnPlay.addEventListener('click', () => {
        if (mapS.playing) {
          mapStopPlay();
        } else {
          mapStartPlay();
        }
      });
    }

    // Compare mode button
    const btnCmpMode = document.getElementById('btn-compare-mode');
    if (btnCmpMode) {
      btnCmpMode.addEventListener('click', () => {
        if (mapS.compareMode) {
          mapExitCompareMode();
        } else {
          mapEnterCompareMode();
        }
      });
    }

    //mapSetPointVisible('pts-layer-abribacs', mapS.indicator === 'nb_abribacs');

    // Exit compare buttons
    const btnExit = document.getElementById('btn-exit-compare');
    if (btnExit) btnExit.addEventListener('click', mapExitCompareMode);

    const btnClose = document.getElementById('btn-close-compare');
    if (btnClose) btnClose.addEventListener('click', mapExitCompareMode);
  }

  // Lance l'animation temporelle : avance d'une année toutes les 1,2 s entre 2020 et 2025
  function mapStartPlay() {
    if (mapS.playing) return;
    mapS.playing = true;

    const slider = document.getElementById('map-year-slider');
    const minYear = 2020;
    const maxYear = 2025;

    // Start from beginning if at end
    if (mapS.year >= maxYear) {
      mapS.year = minYear;
      if (slider) slider.value = mapS.year;
    }

    const btnPlay = document.getElementById('btn-play-map');
    if (btnPlay) btnPlay.textContent = '⏸';

    mapS.playTimer = setInterval(() => {
      if (mapS.year >= maxYear) {
        mapStopPlay();
        return;
      }
      mapS.year += 1;
      if (slider) slider.value = mapS.year;
      const disp = document.getElementById('year-display');
      if (disp) disp.textContent = mapS.year;
      mapUpdateChoropleth();
    }, 1200);
  }

  function mapStopPlay() {
    mapS.playing = false;
    if (mapS.playTimer) {
      clearInterval(mapS.playTimer);
      mapS.playTimer = null;
    }
    const btnPlay = document.getElementById('btn-play-map');
    if (btnPlay) btnPlay.textContent = '▶';
  }

})();
