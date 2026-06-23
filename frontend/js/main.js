// Point d'entrée principal de l'interface Urban Data Explorer.
// Initialise les graphiques Chart.js, branche les listeners UI et orchestre
// les appels API pour alimenter les KPIs, cartes et visualisations.

console.log("main.js chargé");

if (window.Chart) {
  Chart.defaults.devicePixelRatio = 2;
}

const API_BASE_URL = "http://localhost:8000/api";
const API_KEY = "urban-data-explorer-dev-key";

const THEME_KEY = 'urban-data-theme';

// Retourne les couleurs Chart.js adaptées au thème clair ou sombre
function getChartThemeColors(theme) {
  const isLight = theme === 'light';
  return {
    text: isLight ? '#0f172a' : '#e5e7eb',
    grid: isLight ? 'rgba(15, 23, 42, 0.08)' : 'rgba(255, 255, 255, 0.06)',
    tick: isLight ? '#475569' : '#cbd5e1',
  };
}

// Applique le thème sauvegardé immédiatement pour éviter le flash au chargement
(function() {
  const saved = localStorage.getItem(THEME_KEY) || 'dark';
  document.documentElement.setAttribute('data-theme', saved);
})();

// Construit l'URL avec les paramètres filtres, envoie la requête authentifiée et retourne le JSON
async function apiGet(path, params = {}) {
  const url = new URL(API_BASE_URL + path);

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.append(key, value);
    }
  });

  const resp = await fetch(url, { headers: { "X-API-Key": API_KEY } });
  if (!resp.ok) {
    let errMsg = `Erreur API (${resp.status})`;
    try {
      const data = await resp.json();
      if (data.detail) errMsg = data.detail;
    } catch (_) {
      /* ignore parse failure */
    }
    throw new Error(errMsg);
  }
  return resp.json();
}

// Fonctions d'accès aux endpoints de l'API
function fetchArrondissements() {
  return apiGet("/arrondissements");
}
function fetchPrix(annee, arrondissement) {
  return apiGet("/prix", { annee, arrondissement });
}
function fetchTimeline(arr) {
  return apiGet("/timeline", { arr });
}
function fetchLogementsSociaux(arrondissement, annee) {
  return apiGet("/logements_sociaux", { arrondissement, annee });
}
function fetchTypologie(arrondissement, annee) {
  return apiGet("/typologie", { arrondissement, annee });
}
function fetchEspacesVerts(arrondissement) {
  return apiGet("/espaces_verts", { arrondissement });
}
function fetchIti(annee, arrondissement) {
  return apiGet("/iti", { annee, arrondissement });
}
function fetchIqv(annee) {
  return apiGet("/iqv", { annee });
}
function fetchIam(annee) {
  return apiGet("/iam", { annee });
}
function fetchIpr(annee) {
  return apiGet("/ipr", { annee });
}

// Affiche un message temporaire non bloquant pendant 3 secondes
function showToast(msg) {
  const el = document.createElement("div");
  el.className = "toast";
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 3000);
}


document.addEventListener("DOMContentLoaded", async () => {
  // DOM
  const arrSelect = document.getElementById("arr-select");
  const arrCompareA = document.getElementById("arr-compare-a");
  const arrCompareB = document.getElementById("arr-compare-b");
  const anneeInput = document.getElementById("annee-input");
  // typologieAnneeInput supprimé — le dropdown unique anneeInput pilote tout

  const btnPrix = document.getElementById("btn-prix");
  const btnTimeline = document.getElementById("btn-timeline");
  const btnLogements = document.getElementById("btn-logements-sociaux");
  const btnTypologie = document.getElementById("btn-typologie");
  const btnIti = document.getElementById("btn-iti");
  const btnCompare = document.getElementById("btn-compare");

  const resultPre = document.getElementById("result");
  const timelineCanvas = document.getElementById("timelineChart");
  const compareTimelineCanvas = document.getElementById("compareTimelineChart");
  const logementsCanvas = document.getElementById("logementsChart");
  const typologieCanvas = document.getElementById("typologieChart");
  const itiCanvas = document.getElementById("itiChart");
  const iqvCanvas = document.getElementById("iqvChart");
  const iamCanvas = document.getElementById("iamChart");
  const iprCanvas = document.getElementById("iprChart");

  const kpiPrice = document.getElementById("kpi-price");
  const kpiLogements = document.getElementById("kpi-logements");
  const kpiVerts = document.getElementById("kpi-verts");
  const badgeSelection = document.getElementById("badge-selection");
  const lastUpdate = document.getElementById("last-update-badge");

  // Instances Chart.js — conservées pour pouvoir détruire avant recréation
  let timelineChart = null;
  let compareChart = null;
  let logementsChart = null;
  let typologieChart = null;
  let itiChart = null;
  let iqvChart = null;
  let iamChart = null;
  let iprChart = null;

  // Génère les options communes à tous les graphiques (couleurs, grille, légende)
  function buildChartDefaults() {
    const c = getChartThemeColors(localStorage.getItem(THEME_KEY) || 'dark');
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: c.text } },
        tooltip: {
          backgroundColor: "#0f172a",
          borderColor: "#1f2937",
          borderWidth: 1,
        },
      },
      scales: {
        x: { ticks: { color: c.tick }, grid: { color: c.grid } },
        y: { ticks: { color: c.tick }, grid: { color: c.grid } },
      },
    };
  }
  const chartDefaults = buildChartDefaults();

  function stringify(data) {
    if (resultPre) resultPre.textContent = JSON.stringify(data, null, 2);
  }

  // Formate le numéro d'arrondissement en libellé français (1er, 2e…)
  function arrLabel(num) {
    return num === 1 ? "1er" : `${num}e`;
  }

  const LABELS = {
    part_studio_pct:       'Studios',
    part_T2_pct:           'T2',
    part_T3plus_pct:       'T3 et plus',
    logements_sociaux_pct: 'Part des logements sociaux',
    nb_espaces_verts:      "Nombre d'espaces verts",
    surface_totale_m2:     'Surface totale (m²)',
    iti:                   'Indice de Tension Immobilière',
    prix_norm:             'Prix/m² normalisé',
    variation_norm:        'Variation normalisée',
    sociaux_inv_norm:      'Impact logement social (inv.)',
    nb_abribacs:           'Points de collecte (Abribacs)',
    prix_m2_median:        'Prix médian au m²',
    variation_pct:         'Variation du prix (%)',
  };
  function prettyLabel(key) { return LABELS[key] || key; }

  // Formate une valeur numérique en notation française, retourne "–" si absente
  function formatNumber(value, opts = {}) {
    if (value === undefined || value === null || Number.isNaN(value)) return "–";
    return Number(value).toLocaleString("fr-FR", {
      maximumFractionDigits: 1,
      ...opts,
    });
  }

  // Met à jour le badge de sélection et l'horodatage de dernière mise à jour
  function updateStatus() {
    const v = arrSelect.value;
    const arrText = v ? arrLabel(parseInt(v, 10)) : "Tous";
    badgeSelection.textContent = arrText;
    const now = new Date();
    if (lastUpdate) lastUpdate.textContent = "Mise à jour : " + now.toLocaleString("fr-FR", {
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  // Peuple un élément <select> avec la liste des arrondissements disponibles
  function applyOptionsToSelect(select, arrondissements) {
    select.innerHTML = '<option value="">—</option>';
    arrondissements.forEach((arr) => {
      const opt = document.createElement("option");
      opt.value = arr;
      opt.textContent = arrLabel(arr);
      select.appendChild(opt);
    });
  }

  // Chargement initial de la liste des arrondissements dans tous les selects
  try {
    const data = await fetchArrondissements();
    const arrs = data.arrondissements || [];
    applyOptionsToSelect(arrSelect, arrs);
    applyOptionsToSelect(arrCompareA, arrs);
    applyOptionsToSelect(arrCompareB, arrs);
  } catch (e) {
    console.error("Erreur chargement arrondissements:", e);
    stringify({ error: "Erreur chargement arrondissements : " + e.message });
  }

  updateStatus();

  // Synchronise l'icône du bouton de thème avec la préférence sauvegardée
  const _savedTheme = localStorage.getItem(THEME_KEY) || 'dark';
  const _themeIcon = document.querySelector('.theme-icon');
  if (_themeIcon) _themeIcon.textContent = _savedTheme === 'light' ? '☀️' : '🌙';

  document.getElementById('theme-toggle')?.addEventListener('click', () => {
    const current = document.documentElement.getAttribute('data-theme') || 'dark';
    const next = current === 'dark' ? 'light' : 'dark';
    localStorage.setItem(THEME_KEY, next);
    location.reload();
  });

  // Charge les KPIs Paris dès le démarrage (sans arrondissement sélectionné).
  // setTimeout(0) garantit que toutes les fonctions ci-dessous sont déclarées avant l'appel.
  setTimeout(() => {
    loadPrix();
    loadLogements();
    updateKpiVerts();
    loadIti();
    loadIqv();
    loadIam();
    loadIpr();
  }, 0);

  // FONCTIONS DE CHARGEMENT

  // Charge le prix médian au m² et met à jour le KPI — moyenne Paris ou arrondissement sélectionné
  async function loadPrix() {
    const arr = arrSelect.value || undefined;
    const annee = anneeInput.value ? parseInt(anneeInput.value, 10) : undefined;
    const kpiNote = document.getElementById("kpi-price-note");

    stringify({ info: `Chargement des prix pour ${annee || 'toutes années'}...` });
    try {
      const data = await fetchPrix(annee, arr ? parseInt(arr, 10) : undefined);
      stringify(data);

      if (arr) {
        const rows = Array.isArray(data) ? data : [data];
        const source = rows[0] || {};
        const prix = source.prix_m2_median ?? source.prix_m2 ?? null;
        kpiPrice.textContent =
          prix !== null ? `${formatNumber(prix, { maximumFractionDigits: 0 })} €/m²` : "N/A";
        if (kpiNote) kpiNote.textContent = annee ? `En ${annee}` : "Toutes années";
      } else {
        // Calcule la moyenne des prix médians sur tous les arrondissements : Σ(prix_i) / n
        const rows = Array.isArray(data) ? data : [];
        const vals = rows
          .map((r) => r.prix_m2_median ?? r.prix_m2 ?? null)
          .filter((v) => v !== null);
        if (vals.length) {
          const moy = vals.reduce((s, v) => s + v, 0) / vals.length;
          kpiPrice.textContent = `${formatNumber(moy, { maximumFractionDigits: 0 })} €/m²`;
          if (kpiNote) kpiNote.textContent = `Moy. Paris${annee ? ` ${annee}` : ""}`;
        } else {
          kpiPrice.textContent = "N/A";
        }
      }
    } catch (e) {
      console.error(e);
      stringify({ error: e.message });
      kpiPrice.textContent = "N/A";
    }
    updateStatus();
  }

  // Trace l'évolution du prix médian au m² sur la durée pour l'arrondissement sélectionné
  async function loadTimeline() {
    const arr = arrSelect.value;
    if (!arr) {
      stringify({ info: "Sélectionne un arrondissement pour la timeline." });
      return;
    }

    stringify({ info: "Chargement de la timeline..." });
    try {
      const data = await fetchTimeline(parseInt(arr, 10));
      stringify(data);

      const timeline = data.timeline || [];
      const labels = timeline.map((row) => row.annee);
      const values = timeline.map(
        (row) => row.prix_m2_median ?? row.prix_m2 ?? row.valeur ?? 0
      );

      if (!labels.length) {
        showToast("Pas de données pour cette timeline.");
        return;
      }

      if (timelineChart) timelineChart.destroy();

      timelineChart = new Chart(timelineCanvas, {
        type: "line",
        data: {
          labels,
          datasets: [
            {
              label: `Prix/m² médian - ${arrLabel(parseInt(arr, 10))}`,
              data: values,
              tension: 0.25,
              borderColor: "#f59e0b",
              backgroundColor: "rgba(245, 158, 11, 0.18)",
              fill: true,
              pointRadius: 3,
            },
          ],
        },
        options: {
          ...chartDefaults,
          scales: {
            ...chartDefaults.scales,
            x: {
              ...chartDefaults.scales.x,
              title: { display: true, text: "Année", color: "#cbd5e1" },
            },
            y: {
              ...chartDefaults.scales.y,
              title: { display: true, text: "Prix/m²", color: "#cbd5e1" },
            },
          },
        },
      });
    } catch (e) {
      console.error(e);
      stringify({ error: e.message });
    }
    updateStatus();
  }

  // Affiche la répartition des types de logements (studios, T2, T3+) en donut chart
  async function loadTypologie({ showAlert = false } = {}) {
    const arr = arrSelect.value;
    const anneeVal = anneeInput.value;

    if (!arr || !anneeVal) {
      if (showAlert) showToast("Choisis un arrondissement ET une année.");
      return;
    }

    const annee = parseInt(anneeVal, 10);
    stringify({ info: "Chargement de la typologie..." });
    try {
      const data = await fetchTypologie(parseInt(arr, 10), annee);
      stringify(data);

      if (!Array.isArray(data) || !data.length) {
        if (showAlert) showToast("Pas de données de typologie pour ces filtres.");
        return;
      }

      const row = data[0];
      const keys = Object.keys(row).filter(
        (k) => !["arrondissement", "annee"].includes(k)
      );
      if (!keys.length) {
        if (showAlert) showToast("Aucune colonne de typologie exploitable.");
        return;
      }

      const labels = keys.map(prettyLabel);
      const values = keys.map((k) => row[k]);

      if (typologieChart) typologieChart.destroy();

      typologieChart = new Chart(typologieCanvas, {
        type: "doughnut",
        data: {
          labels,
          datasets: [
            {
              label: `Typologie ${arrLabel(parseInt(arr, 10))} en ${annee}`,
              data: values,
              backgroundColor: [
                "#22d3ee",
                "#f59e0b",
                "#84cc16",
                "#c084fc",
                "#fb7185",
                "#38bdf8",
              ],
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            tooltip: chartDefaults.plugins.tooltip,
            legend: {
              labels: {
                color: chartDefaults.plugins.legend.labels.color,
                // Enrichit chaque entrée de légende avec le pourcentage : part_i = valeur_i / Σ(valeurs)
                generateLabels(chart) {
                  const d = chart.data;
                  const total = d.datasets[0].data.reduce((s, v) => s + (v || 0), 0);
                  const labelColor = chartDefaults.plugins.legend.labels.color;
                  return d.labels.map((lbl, i) => ({
                    text: total > 0
                      ? `${lbl} — ${((d.datasets[0].data[i] / total) * 100).toFixed(1)} %`
                      : lbl,
                    fillStyle: d.datasets[0].backgroundColor[i],
                    strokeStyle: d.datasets[0].backgroundColor[i],
                    fontColor: labelColor,
                    lineWidth: 0,
                    hidden: false,
                    index: i,
                  }));
                },
              },
            },
          },
        },
      });
    } catch (e) {
      console.error(e);
      stringify({ error: e.message });
    }
    updateStatus();
  }

  // Charge la part de logements sociaux par arrondissement et met à jour le KPI
  async function loadLogements() {
    const arr = arrSelect.value || undefined;
    const annee = anneeInput.value ? parseInt(anneeInput.value, 10) : undefined;

    stringify({ info: "Chargement des logements sociaux..." });
    try {
      const data = await fetchLogementsSociaux(undefined, annee);
      stringify(data);

      if (!Array.isArray(data) || !data.length) {
        showToast("Pas de données logements sociaux.");
        kpiLogements.textContent = "N/A";
        return;
      }

      const numericKeys = Object.keys(data[0]).filter(
        (k) => !["arrondissement", "annee"].includes(k)
      );
      if (!numericKeys.length) {
        showToast("Colonnes numériques introuvables pour logements sociaux.");
        return;
      }
      const valueKey = numericKeys[0];

      const labels = data.map((row) => arrLabel(row.arrondissement));
      const values = data.map((row) => row[valueKey]);

      if (logementsChart) logementsChart.destroy();

      logementsChart = new Chart(logementsCanvas, {
        type: "bar",
        data: {
          labels,
          datasets: [
            {
              label: `${prettyLabel(valueKey)} (%)`,
              data: values,
              backgroundColor: "rgba(34, 211, 238, 0.35)",
              borderColor: "#22d3ee",
            },
          ],
        },
        options: {
          ...chartDefaults,
          scales: {
            ...chartDefaults.scales,
            x: {
              ...chartDefaults.scales.x,
              title: { display: true, text: "Arrondissement", color: "#cbd5e1" },
            },
            y: {
              ...chartDefaults.scales.y,
              title: { display: true, text: "Part (%)", color: "#cbd5e1" },
            },
          },
        },
      });

      // KPI : valeur de l'arrondissement sélectionné ou moyenne Paris
      const kpiNote = document.getElementById("kpi-logements-note");
      if (arr) {
        const selectedRow = data.find(
          (row) => Number(row.arrondissement) === Number(arr)
        );
        const firstVal = selectedRow ? selectedRow[valueKey] : null;
        kpiLogements.textContent =
          firstVal !== undefined && firstVal !== null
            ? `${formatNumber(firstVal)} %`
            : "N/A";
        if (kpiNote) kpiNote.textContent = `${arrLabel(parseInt(arr, 10))} arrondissement`;
      } else {
        const vals = data.map((r) => r[valueKey]).filter((v) => v !== null && v !== undefined);
        if (vals.length) {
          const moy = vals.reduce((s, v) => s + Number(v), 0) / vals.length;
          kpiLogements.textContent = `${formatNumber(moy)} %`;
          if (kpiNote) kpiNote.textContent = annee ? `Moy. Paris ${annee}` : "Moy. Paris";
        } else {
          kpiLogements.textContent = "N/A";
        }
      }
    } catch (e) {
      console.error(e);
      stringify({ error: e.message });
      kpiLogements.textContent = "N/A";
    }
    updateStatus();
  }

  // Met à jour le KPI surface d'espaces verts — total Paris ou arrondissement sélectionné
  async function updateKpiVerts() {
    const arr = arrSelect.value || undefined;
    try {
      const data = await fetchEspacesVerts(undefined);
      if (!Array.isArray(data) || !data.length) return;
      const kpiNote = document.getElementById("kpi-verts-note");
      if (arr) {
        const row = data.find((r) => Number(r.arrondissement) === Number(arr));
        const val = row ? (row.surface_totale_m2 ?? null) : null;
        kpiVerts.textContent = val !== null
          ? `${formatNumber(val, { maximumFractionDigits: 0 })} m²` : "N/A";
        if (kpiNote) kpiNote.textContent = `${arrLabel(parseInt(arr, 10))} arrondissement`;
      } else {
        // Somme de toutes les surfaces : Σ(surface_totale_m2) sur les 20 arrondissements
        const total = data.reduce((s, r) => s + Number(r.surface_totale_m2 ?? 0), 0);
        kpiVerts.textContent = `${formatNumber(total, { maximumFractionDigits: 0 })} m²`;
        if (kpiNote) kpiNote.textContent = "Total Paris";
      }
    } catch (e) {
      console.error(e);
      kpiVerts.textContent = "N/A";
    }
  }

  // Trace l'IQV (Indice de Qualité de Vie) par arrondissement, coloré par seuil
  async function loadIqv(anneeOverride) {
    const annee = anneeOverride
      ?? (anneeInput.value ? parseInt(anneeInput.value, 10) : 2024);
    try {
      const data = await fetchIqv(annee);
      if (!Array.isArray(data) || !data.length) return;

      const sorted = [...data].sort((a, b) => b.iqv_score - a.iqv_score);
      const labels = sorted.map((r) => arrLabel(r.arrondissement));
      const values = sorted.map((r) => r.iqv_score);
      // Seuils : vert ≥ 70, orange ≥ 40, rouge < 40
      const colors = values.map((v) =>
        v >= 70 ? "rgba(34, 197, 94, 0.8)"
               : v >= 40 ? "rgba(251, 191, 36, 0.8)"
               : "rgba(239, 68, 68, 0.8)"
      );
      const borders = values.map((v) =>
        v >= 70 ? "#22c55e" : v >= 40 ? "#fbbf24" : "#ef4444"
      );

      if (iqvChart) iqvChart.destroy();

      const iqvYearTag = document.getElementById("iqv-chart-year-tag");
      if (iqvYearTag) iqvYearTag.textContent = String(annee);

      iqvChart = new Chart(iqvCanvas, {
        type: "bar",
        data: {
          labels,
          datasets: [{
            label: `IQV ${annee}`,
            data: values,
            backgroundColor: colors,
            borderColor: borders,
            borderWidth: 1,
          }],
        },
        options: {
          ...chartDefaults,
          indexAxis: "y",
          scales: {
            x: {
              ...chartDefaults.scales.x,
              min: 0,
              max: 100,
              title: { display: true, text: "IQV (0 = faible qualité · 100 = excellente)", color: chartDefaults.scales.x.ticks.color },
            },
            y: { ...chartDefaults.scales.y, title: { display: false } },
          },
          plugins: {
            ...chartDefaults.plugins,
            tooltip: {
              ...chartDefaults.plugins.tooltip,
              callbacks: {
                label(ctx) {
                  return `IQV : ${sorted[ctx.dataIndex].iqv_score.toFixed(1)}`;
                },
              },
            },
          },
        },
      });
    } catch (e) {
      console.error(e);
    }
    updateStatus();
  }

  // Trace l'IAM (Indice d'Attractivité du Marché) par arrondissement, coloré par seuil
  async function loadIam(anneeOverride) {
    const annee = anneeOverride
      ?? (anneeInput.value ? parseInt(anneeInput.value, 10) : 2024);
    try {
      const data = await fetchIam(annee);
      if (!Array.isArray(data) || !data.length) return;

      const sorted = [...data].sort((a, b) => b.iam_score - a.iam_score);
      const labels = sorted.map((r) => arrLabel(r.arrondissement));
      const values = sorted.map((r) => r.iam_score);
      // Seuils : bleu ≥ 60, indigo ≥ 30, gris < 30
      const colors = values.map((v) =>
        v >= 60 ? "rgba(59, 130, 246, 0.8)"
               : v >= 30 ? "rgba(99, 102, 241, 0.75)"
               : "rgba(148, 163, 184, 0.7)"
      );
      const borders = values.map((v) =>
        v >= 60 ? "#3b82f6" : v >= 30 ? "#6366f1" : "#94a3b8"
      );

      if (iamChart) iamChart.destroy();

      const iamYearTag = document.getElementById("iam-chart-year-tag");
      if (iamYearTag) iamYearTag.textContent = String(annee);

      iamChart = new Chart(iamCanvas, {
        type: "bar",
        data: {
          labels,
          datasets: [{
            label: `IAM ${annee}`,
            data: values,
            backgroundColor: colors,
            borderColor: borders,
            borderWidth: 1,
          }],
        },
        options: {
          ...chartDefaults,
          indexAxis: "y",
          scales: {
            x: {
              ...chartDefaults.scales.x,
              min: 0,
              max: 100,
              title: { display: true, text: "IAM (0 = marché inactif · 100 = très dynamique)", color: chartDefaults.scales.x.ticks.color },
            },
            y: { ...chartDefaults.scales.y, title: { display: false } },
          },
          plugins: {
            ...chartDefaults.plugins,
            tooltip: {
              ...chartDefaults.plugins.tooltip,
              callbacks: {
                label(ctx) {
                  return `IAM : ${sorted[ctx.dataIndex].iam_score.toFixed(1)}`;
                },
              },
            },
          },
        },
      });
    } catch (e) {
      console.error(e);
    }
    updateStatus();
  }

  // Trace l'IPR (Indice de Pression Résidentielle) par arrondissement, coloré par seuil
  async function loadIpr(anneeOverride) {
    const annee = anneeOverride
      ?? (anneeInput.value ? parseInt(anneeInput.value, 10) : 2024);
    try {
      const data = await fetchIpr(annee);
      if (!Array.isArray(data) || !data.length) return;

      const sorted = [...data].sort((a, b) => b.ipr_score - a.ipr_score);
      const labels = sorted.map((r) => arrLabel(r.arrondissement));
      const values = sorted.map((r) => r.ipr_score);
      // Seuils : rouge ≥ 70 (forte pression), orange ≥ 40, jaune < 40
      const colors = values.map((v) =>
        v >= 70 ? "rgba(220, 38, 38, 0.8)"
               : v >= 40 ? "rgba(249, 115, 22, 0.75)"
               : "rgba(250, 204, 21, 0.7)"
      );
      const borders = values.map((v) =>
        v >= 70 ? "#dc2626" : v >= 40 ? "#f97316" : "#facc15"
      );

      if (iprChart) iprChart.destroy();

      const iprYearTag = document.getElementById("ipr-chart-year-tag");
      if (iprYearTag) iprYearTag.textContent = String(annee);

      iprChart = new Chart(iprCanvas, {
        type: "bar",
        data: {
          labels,
          datasets: [{
            label: `IPR ${annee}`,
            data: values,
            backgroundColor: colors,
            borderColor: borders,
            borderWidth: 1,
          }],
        },
        options: {
          ...chartDefaults,
          indexAxis: "y",
          scales: {
            x: {
              ...chartDefaults.scales.x,
              min: 0,
              max: 100,
              title: { display: true, text: "IPR (0 = peu de pression · 100 = très forte)", color: chartDefaults.scales.x.ticks.color },
            },
            y: { ...chartDefaults.scales.y, title: { display: false } },
          },
          plugins: {
            ...chartDefaults.plugins,
            tooltip: {
              ...chartDefaults.plugins.tooltip,
              callbacks: {
                label(ctx) {
                  return `IPR : ${sorted[ctx.dataIndex].ipr_score.toFixed(1)}`;
                },
              },
            },
          },
        },
      });
    } catch (e) {
      console.error(e);
    }
    updateStatus();
  }

  // Trace l'ITI (Indice de Tension Immobilière) — limité aux années 2022-2025 disponibles dans la couche Gold
  async function loadIti(anneeOverride) {
    const annee = anneeOverride
      ?? (anneeInput.value ? parseInt(anneeInput.value, 10) : 2024);

    const itiAnnee = Math.max(2022, Math.min(2025, annee));

    stringify({ info: `Chargement ITI ${itiAnnee}...` });
    try {
      const data = await fetchIti(itiAnnee, undefined);
      stringify(data);

      if (!Array.isArray(data) || !data.length) {
        return;
      }

      const sorted = [...data].sort((a, b) => b.iti - a.iti);
      const labels = sorted.map((row) => arrLabel(row.arrondissement));
      const values = sorted.map((row) => row.iti);

      // Seuils ITI : rouge > 70 (très tendu), orange 40-70, vert < 40 (détendu)
      const colors = values.map((v) =>
        v > 70
          ? "rgba(239, 68, 68, 0.75)"
          : v >= 40
          ? "rgba(251, 146, 60, 0.75)"
          : "rgba(74, 222, 128, 0.75)"
      );
      const borders = values.map((v) =>
        v > 70 ? "#ef4444" : v >= 40 ? "#fb923c" : "#4ade80"
      );

      if (itiChart) itiChart.destroy();

      const itiYearTag = document.getElementById("iti-chart-year-tag");
      if (itiYearTag) itiYearTag.textContent = String(itiAnnee);

      itiChart = new Chart(itiCanvas, {
        type: "bar",
        data: {
          labels,
          datasets: [
            {
              label: `ITI ${itiAnnee}`,
              data: values,
              backgroundColor: colors,
              borderColor: borders,
              borderWidth: 1,
            },
          ],
        },
        options: {
          ...chartDefaults,
          indexAxis: "y",
          scales: {
            x: {
              ...chartDefaults.scales.x,
              min: 0,
              max: 100,
              title: { display: true, text: "ITI (0 = détendu · 100 = très tendu)", color: "#cbd5e1" },
            },
            y: {
              ...chartDefaults.scales.y,
              title: { display: false },
            },
          },
          plugins: {
            ...chartDefaults.plugins,
            tooltip: {
              ...chartDefaults.plugins.tooltip,
              // Détail des trois composantes de l'ITI dans le tooltip
              callbacks: {
                label(ctx) {
                  const row = sorted[ctx.dataIndex];
                  return [
                    `ITI : ${row.iti.toFixed(1)}`,
                    `  Prix normalisé : ${row.prix_norm.toFixed(1)}`,
                    `  Variation normalisée : ${row.variation_norm.toFixed(1)}`,
                    `  Impact social (inv.) : ${row.sociaux_inv_norm.toFixed(1)}`,
                  ];
                },
              },
            },
          },
        },
      });
    } catch (e) {
      console.error(e);
      stringify({ error: e.message });
    }
    updateStatus();
  }


  // Désactive dans chaque select l'option déjà choisie dans l'autre pour éviter A = B
  function syncCompareSelects() {
    const a = arrCompareA.value;
    const b = arrCompareB.value;
    Array.from(arrCompareB.options).forEach((opt) => {
      opt.disabled = opt.value !== "" && opt.value === a;
    });
    Array.from(arrCompareA.options).forEach((opt) => {
      opt.disabled = opt.value !== "" && opt.value === b;
    });
    if (a && b && a === b) {
      arrCompareB.value = "";
    }
  }

  // Charge les timelines des deux arrondissements en parallèle et les superpose sur un même graphique
  async function loadComparison() {
    const arrA = arrCompareA.value;
    const arrB = arrCompareB.value;
    if (!arrA || !arrB) {
      showToast("Choisis deux arrondissements différents pour comparer.");
      return;
    }

    stringify({ info: "Chargement de la comparaison..." });
    try {
      const [dataA, dataB] = await Promise.all([
        fetchTimeline(parseInt(arrA, 10)),
        fetchTimeline(parseInt(arrB, 10)),
      ]);

      const timelineA = dataA.timeline || [];
      const timelineB = dataB.timeline || [];

      // Union des années des deux séries, triée chronologiquement
      const labels = Array.from(
        new Set([
          ...timelineA.map((d) => d.annee),
          ...timelineB.map((d) => d.annee),
        ]).values()
      ).sort((a, b) => a - b);

      const datasetA = labels.map((annee) => {
        const item = timelineA.find((d) => d.annee === annee);
        return item ? item.prix_m2_median ?? item.prix_m2 ?? 0 : null;
      });
      const datasetB = labels.map((annee) => {
        const item = timelineB.find((d) => d.annee === annee);
        return item ? item.prix_m2_median ?? item.prix_m2 ?? 0 : null;
      });

      if (compareChart) compareChart.destroy();

      compareChart = new Chart(compareTimelineCanvas, {
        type: "line",
        data: {
          labels,
          datasets: [
            {
              label: `${arrLabel(parseInt(arrA, 10))} arrondissement`,
              data: datasetA,
              tension: 0.25,
              borderColor: "#22d3ee",
              backgroundColor: "rgba(34, 211, 238, 0.18)",
              borderWidth: 2,
              fill: false,
            },
            {
              label: `${arrLabel(parseInt(arrB, 10))} arrondissement`,
              data: datasetB,
              tension: 0.25,
              borderColor: "#f59e0b",
              backgroundColor: "rgba(245, 158, 11, 0.18)",
              borderWidth: 2,
              fill: false,
            },
          ],
        },
        options: {
          ...chartDefaults,
          scales: {
            ...chartDefaults.scales,
            x: {
              ...chartDefaults.scales.x,
              title: { display: true, text: "Année", color: "#cbd5e1" },
            },
            y: {
              ...chartDefaults.scales.y,
              title: { display: true, text: "Prix/m²", color: "#cbd5e1" },
            },
          },
        },
      });
    } catch (e) {
      console.error(e);
      stringify({ error: e.message });
    }
  }

  // LISTENERS

  btnPrix.addEventListener("click", loadPrix);
  btnTimeline.addEventListener("click", () => {
    if (!arrSelect.value) {
      showToast("Sélectionne un arrondissement pour la timeline.");
      return;
    }
    loadTimeline();
  });
  // btnTypologie click est géré par le listener de cycle ci-dessous (Studios → T2 → T3+)
  btnLogements.addEventListener("click", loadLogements);
  btnIti.addEventListener("click", () => loadIti());
  btnCompare.addEventListener("click", loadComparison);

  // Surligne les arrondissements comparés sur la carte lors d'un changement de sélection
  arrCompareA.addEventListener("change", () => {
    syncCompareSelects();
    const arrA = parseInt(arrCompareA.value, 10);
    const arrB = parseInt(arrCompareB.value, 10);
    if (typeof window.setMapHighlight === 'function') {
      window.setMapHighlight([arrA, arrB].filter((n) => !isNaN(n) && n > 0));
    }
  });
  arrCompareB.addEventListener("change", () => {
    syncCompareSelects();
    const arrA = parseInt(arrCompareA.value, 10);
    const arrB = parseInt(arrCompareB.value, 10);
    if (typeof window.setMapHighlight === 'function') {
      window.setMapHighlight([arrA, arrB].filter((n) => !isNaN(n) && n > 0));
    }
  });

  // Le dropdown Année unique déclenche la mise à jour de TOUS les graphiques et de la carte
  anneeInput.addEventListener("change", () => {
    const yr = anneeInput.value ? parseInt(anneeInput.value, 10) : null;
    if (yr && (yr < 2022 || yr > 2025)) {
      if (typeof window.getMapIndicator === 'function' && window.getMapIndicator() === 'iti') {
        showToast("ITI disponible uniquement de 2022 à 2025.");
      }
    }
    loadPrix();
    loadLogements();
    loadTypologie({ showAlert: false });
    loadIti(yr || undefined);
    loadIqv(yr || undefined);
    loadIam(yr || undefined);
    loadIpr(yr || undefined);
    if (yr && typeof window.setMapYear === 'function') {
      window.setMapYear(yr);
    }
  });

  // Le changement d'arrondissement déclenche la mise à jour de tous les graphiques
  arrSelect.addEventListener("change", () => {
    updateStatus();
    loadTimeline();
    if (anneeInput.value) loadPrix();
    loadTypologie({ showAlert: false });
    loadLogements();
    updateKpiVerts();
    loadIti();
    loadIqv();
    loadIam();
    loadIpr();
  });

  // Bridge carte → graphiques : appelé par map.js lors du clic sur un arrondissement
  window.onMapArrondissementClick = (arrNum) => {
    arrSelect.value = String(arrNum);
    arrSelect.dispatchEvent(new Event("change"));
  };

  let typologieIndex = -1;
  // Cycle des sous-indicateurs de taille des logements pour la choroplèthe
  const TYPOLOGIE_CYCLE = [
    { key: 'part_studio_pct', label: 'Studios' },
    { key: 'part_T2_pct',     label: 'T2' },
    { key: 'part_T3plus_pct', label: 'T3+' },
  ];

  const SIDEBAR_MAP_INDICATORS = {
    'btn-prix':              'prix_m2',
    'btn-timeline':          'variation_pct',
    'btn-logements-sociaux': 'logements_sociaux_pct',
    'btn-iti':               'iti',
    'btn-iqv':               'iqv',
    'btn-iam':               'iam',
    'btn-ipr':               'ipr',
  };

  const allSidebarBtns = [...Object.keys(SIDEBAR_MAP_INDICATORS), 'btn-typologie']
    .map((id) => document.getElementById(id))
    .filter(Boolean);

  // Met en évidence le bouton sidebar actif en retirant la classe des autres
  function setSidebarActive(activeId) {
    allSidebarBtns.forEach((b) => {
      b.classList.toggle('btn-active', b.id === activeId);
    });
  }

  // Bouton "Taille des logements" — cycle Studios → T2 → T3+ sur la carte choroplèthe
  if (btnTypologie) {
    btnTypologie.addEventListener('click', () => {
      typologieIndex = (typologieIndex + 1) % 3;
      const item = TYPOLOGIE_CYCLE[typologieIndex];
      btnTypologie.textContent = 'Taille : ' + item.label;
      const nextItem = TYPOLOGIE_CYCLE[(typologieIndex + 1) % 3];
      btnTypologie.setAttribute('data-tooltip', 'Cliquez pour passer à : ' + nextItem.label);
      if (typeof window.setMapIndicator === 'function') {
        window.setMapIndicator(item.key);
      }
      setSidebarActive('btn-typologie');
      console.log('[sidebar] indicateur carte →', item.key);
    });
  }

  // Branche chaque bouton sidebar sur l'indicateur de carte correspondant
  Object.entries(SIDEBAR_MAP_INDICATORS).forEach(([btnId, mapKey]) => {
    const btn = document.getElementById(btnId);
    if (!btn) return;
    btn.addEventListener('click', () => {
      if (typeof window.setMapIndicator === 'function') {
        window.setMapIndicator(mapKey);
      }
      typologieIndex = -1;
      if (btnTypologie) btnTypologie.textContent = 'Taille des logements';
      setSidebarActive(btnId);
      console.log('[sidebar] indicateur carte →', mapKey);
    });
  });

  if (typeof initMap === "function") initMap();
});
