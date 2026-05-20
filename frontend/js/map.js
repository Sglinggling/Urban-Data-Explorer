
(function () {
  'use strict';

  // Constantes

  const API_BASE = 'http://localhost:8000/api';
  const GEOJSON_PATH = 'data/arrondissements.geojson';

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
      valKey: 'logements_sociaux_pct',
      hasYear: false,
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
      label: 'Part T3+',
      endpoint: '/typologie',
      yrParam: 'annee',
      valKey: 'part_T3plus_pct',
      hasYear: true,
      pal: 'seq',
      fmt: (v) => v.toFixed(1) + ' %',
    },
    nb_total_ecoles: {
      label: 'Nb total écoles',
      endpoint: '/etablissements_scolaires',
      valKey: 'nb_total_ecoles',
      hasYear: false,
      pal: 'seq',
      fmt: (v) => `${Math.round(v)} étab.`,
    },
    surface_ev: {
      label: 'Espaces verts (ha)',
      endpoint: '/espaces_verts',
      valKey: 'surface_totale_m2',
      hasYear: false,
      pal: 'seq',
      fmt: (v) => `${(v / 10000).toFixed(0)} ha`,
    },
    nb_abribacs: {
      label: 'Nb abribacs / PAVDA',
      endpoint: '/abribacs',
      valKey: 'nb_abribacs',
      hasYear: false,
      pal: 'seq',
      fmt: (v) => `${Math.round(v)} pts`,
    },
  };

  const MAP_PALETTES = {
    seq: ['#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c', '#fc4e2a', '#e31a1c', '#b10026'],
    div: ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850'],
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
    indicator: 'prix_m2',
    year: 2024,
    playing: false,
    playTimer: null,
    compareMode: false,
    compareArr: [],
    layers: { maternelles: true, elementaires: true, colleges: true, abribacs: true },
  };

  // Public entry point

  window.initMap = async function initMap() {
    mapS.map = new maplibregl.Map({
      container: 'map',
      style: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
      center: [2.3522, 48.8566],
      zoom: 11.3,
      attributionControl: false,
    });

    mapS.map.addControl(
      new maplibregl.AttributionControl({ compact: true }),
      'bottom-left'
    );

    mapS.map.on('load', async () => {
      await mapLoadGeoJson();
      mapAddLayers();
      mapSetupInteractions();
      await mapPrefetchStatic();
      await mapUpdateChoropleth();
      await mapInitPoints();
      mapSetupControls();
    });
  };

  // chargement du GEOJson

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

  // Layer setup

  function mapAddLayers() {
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
  }

  // Data fetching helpers

  async function mapFetch(path, params = {}) {
    try {
      const url = new URL(API_BASE + path);
      Object.entries(params).forEach(([k, v]) => {
        if (v !== undefined && v !== null && v !== '') {
          url.searchParams.append(k, v);
        }
      });
      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`HTTP ${resp.status} on ${path}`);
      return await resp.json();
    } catch (err) {
      console.warn('[map] mapFetch error:', err);
      return [];
    }
  }

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

  async function mapPrefetchStatic() {
    // Fetch all static (hasYear:false) indicators in parallel
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

  async function mapFetchForIndicator(indicator, year) {
    const cfg = MAP_INDICATORS[indicator];
    if (!cfg) return {};

    // variation_pct is computed from allPrixRaw
    if (indicator === 'variation_pct') {
      const cacheKey = `variation_pct_${year}`;
      if (!mapS.cache[cacheKey]) {
        mapS.cache[cacheKey] = mapComputeVariation(year);
      }
      return mapS.cache[cacheKey];
    }

    // Static indicators
    if (!cfg.hasYear) {
      const cacheKey = `${indicator}_all`;
      if (!mapS.cache[cacheKey]) {
        const rows = await mapFetch(cfg.endpoint);
        mapS.cache[cacheKey] = mapBuildValueMap(rows, cfg);
      }
      return mapS.cache[cacheKey];
    }

    // Year-dependent indicators
    const cacheKey = `${indicator}_${year}`;
    if (!mapS.cache[cacheKey]) {
      const params = cfg.yrParam ? { [cfg.yrParam]: year } : {};
      const rows = await mapFetch(cfg.endpoint, params);
      mapS.cache[cacheKey] = mapBuildValueMap(rows, cfg);
    }
    return mapS.cache[cacheKey];
  }

  // ─── Choropleth update ────────────────────────────────────────────────────

  async function mapUpdateChoropleth() {
    if (!mapS.map || !mapS.geoJson) return;

    const cfg = MAP_INDICATORS[mapS.indicator];
    if (!cfg) return;

    const effectiveYear =
      cfg.minYear && mapS.year < cfg.minYear ? cfg.minYear : mapS.year;

    const valueMap = await mapFetchForIndicator(mapS.indicator, effectiveYear);

    const annotated = mapAnnotateGeoJson(valueMap);
    const source = mapS.map.getSource('arrondissements');
    if (source) source.setData(annotated);

    const values = Object.values(valueMap).filter((v) => typeof v === 'number' && isFinite(v));
    const colorExpr = mapBuildColorExpression(values, cfg.pal);
    mapS.map.setPaintProperty('arr-fill', 'fill-color', colorExpr);

    mapUpdateLegend(values, cfg);

    // Update header label if element exists
    const headerEl = document.getElementById('map-section-title');
    if (headerEl) headerEl.textContent = 'Carte — ' + cfg.label;

    const yearEl = document.getElementById('year-display');
    if (yearEl) yearEl.textContent = cfg.hasYear ? mapS.year : '—';
  }

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

  function mapBuildColorExpression(values, palType) {
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

    if (palType === 'div') {
      // 9-stop divergent, symmetric around 0
      const absMax = Math.max(Math.abs(minVal), Math.abs(maxVal));
      const safeAbsMax = absMax < 0.005 ? 0.005 : absMax;
      const step = (safeAbsMax * 2) / (palette.length - 1);
      palette.forEach((color, i) => {
        stops.push(-safeAbsMax + i * step, color);
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

  // Legendes

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

    el.innerHTML = `
      <div class="map-legend__title">${cfg.label}</div>
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

  // Point layers (schools + abribacs)

  async function mapInitPoints() {
    try {
      const [ecolesData, abribacsData] = await Promise.all([
        mapFetch('/etablissements_scolaires'),
        mapFetch('/abribacs'),
      ]);

      const pointDefs = [
        {
          id: 'maternelles',
          color: '#f472b6',
          countKey: 'nb_maternelles',
          label: 'Maternelles',
          rows: ecolesData,
        },
        {
          id: 'elementaires',
          color: '#fb923c',
          countKey: 'nb_elementaires',
          label: 'Élémentaires',
          rows: ecolesData,
        },
        {
          id: 'colleges',
          color: '#c084fc',
          countKey: 'nb_colleges',
          label: 'Collèges',
          rows: ecolesData,
        },
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
          layout: { visibility: 'visible' },
          paint: {
            'circle-color': def.color,
            'circle-opacity': 0.82,
            'circle-stroke-width': 1,
            'circle-stroke-color': '#ffffff',
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
                <span style="color:#fbbf24;font-weight:700;">${Math.round(props.count)}</span> établissements
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

  // Interactions

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

  function mapOnHover(e) {
    if (!e.features || e.features.length === 0) return;

    const feat = e.features[0];
    const arrNum = feat.properties.arrNum;
    const arrName = feat.properties.arrName;
    const mapValue = feat.properties.mapValue;

    // Manage feature-state hover
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

    // Build popup content
    const cfg = MAP_INDICATORS[mapS.indicator];
    const valStr =
      mapValue !== null && mapValue !== undefined && typeof mapValue === 'number'
        ? cfg.fmt(mapValue)
        : '—';

    const ctx = mapGetContextStats(arrNum);

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
        <div style="font-size:0.78rem;color:#9ba9c2;border-top:1px solid rgba(255,255,255,0.1);padding-top:6px;display:grid;gap:2px;">
          <div>Prix/m² : <span style="color:#e5e7eb;">${ctx.prix}</span></div>
          <div>Log. sociaux : <span style="color:#e5e7eb;">${ctx.log}</span></div>
          <div>Écoles : <span style="color:#e5e7eb;">${ctx.ecol}</span></div>
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
    // Clear hover state on all features
    if (mapS.geoJson) {
      mapS.geoJson.features.forEach((f) => {
        mapS.map.setFeatureState(
          { source: 'arrondissements', id: f.id },
          { hover: false }
        );
      });
    }
    // Set hover state on the clicked arr
    mapS.map.setFeatureState(
      { source: 'arrondissements', id: arr },
      { hover: true }
    );
    mapS.hoveredId = arr;
  }

  // Context stats

  function mapGetContextStats(arr) {
    const prixCache = mapS.cache[`prix_m2_${mapS.year}`] || {};
    const logCache = mapS.cache['logements_sociaux_pct_all'] || {};
    const ecolCache = mapS.cache['nb_total_ecoles_all'] || {};
    const evCache = mapS.cache['surface_ev_all'] || {};
    const abriCache = mapS.cache['nb_abribacs_all'] || {};

    const fmtPrix = MAP_INDICATORS.prix_m2.fmt;
    const fmtLog = MAP_INDICATORS.logements_sociaux_pct.fmt;
    const fmtEcol = MAP_INDICATORS.nb_total_ecoles.fmt;
    const fmtEv = MAP_INDICATORS.surface_ev.fmt;
    const fmtAbri = MAP_INDICATORS.nb_abribacs.fmt;

    const safe = (map, key, fmt) => {
      const v = map[key];
      return v !== undefined && v !== null ? fmt(v) : '—';
    };

    return {
      prix: safe(prixCache, arr, fmtPrix),
      log: safe(logCache, arr, fmtLog),
      ecol: safe(ecolCache, arr, fmtEcol),
      ev: safe(evCache, arr, fmtEv),
      abri: safe(abriCache, arr, fmtAbri),
    };
  }

  // Comparaison mode

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

    // Update compare layer filter
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

  async function mapRenderComparison(arr1, arr2) {
    // Show comparison panel if it exists
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
          <div><span style="color:#9ba9c2;">Écoles :</span> ${ctx.ecol}</div>
          <div><span style="color:#9ba9c2;">Espaces verts :</span> ${ctx.ev}</div>
          <div><span style="color:#9ba9c2;">Abribacs :</span> ${ctx.abri}</div>
        </div>
      </div>
    `;

    const colA = document.getElementById('compare-col-a');
    if (colA) colA.innerHTML = renderCard(ctx1, name1);
    const colB = document.getElementById('compare-col-b');
    if (colB) colB.innerHTML = renderCard(ctx2, name2);

    // Also update Chart.js comparison selects and trigger
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

  // Controls

  function mapSetupControls() {
    // Indicator select
    const indSel = document.getElementById('map-indicator');
    if (indSel) {
      // Populate options if empty
      if (indSel.options.length === 0) {
        Object.entries(MAP_INDICATORS).forEach(([key, cfg]) => {
          const opt = document.createElement('option');
          opt.value = key;
          opt.textContent = cfg.label;
          if (key === mapS.indicator) opt.selected = true;
          indSel.appendChild(opt);
        });
      }
      indSel.addEventListener('change', () => {
        mapS.indicator = indSel.value;
        const cfg = MAP_INDICATORS[mapS.indicator];
        const yearControl = document.getElementById('year-control');
        if (yearControl) {
          yearControl.style.opacity = cfg && cfg.hasYear ? '1' : '0.35';
          yearControl.style.pointerEvents = cfg && cfg.hasYear ? '' : 'none';
        }
        mapUpdateChoropleth();
      });
    }

    // Year slider
    const slider = document.getElementById('map-year-slider');
    if (slider) {
      slider.value = mapS.year;
      slider.addEventListener('input', () => {
        mapS.year = Number(slider.value);
        const disp = document.getElementById('year-display');
        if (disp) disp.textContent = mapS.year;
        mapUpdateChoropleth();
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

    // Layer toggle checkboxes
    const toggleMap = {
      'toggle-maternelles': 'pts-layer-maternelles',
      'toggle-elementaires': 'pts-layer-elementaires',
      'toggle-colleges': 'pts-layer-colleges',
      'toggle-abribacs': 'pts-layer-abribacs',
    };
    Object.entries(toggleMap).forEach(([cbId, layerId]) => {
      const cb = document.getElementById(cbId);
      if (cb) {
        cb.addEventListener('change', () => {
          mapSetPointVisible(layerId, cb.checked);
        });
      }
    });

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

    // Exit compare buttons
    const btnExit = document.getElementById('btn-exit-compare');
    if (btnExit) btnExit.addEventListener('click', mapExitCompareMode);

    const btnClose = document.getElementById('btn-close-compare');
    if (btnClose) btnClose.addEventListener('click', mapExitCompareMode);
  }

  // Play / animation

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
