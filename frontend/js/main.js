console.log("main.js chargé");

// rendu HD des graphes
if (window.Chart) {
  Chart.defaults.devicePixelRatio = 2;
}

const API_BASE_URL = "http://localhost:8000/api";

// ---------- client API générique (TRANSFÉRÉ DE api_client.js) ----------
async function apiGet(path, params = {}) {
  const url = new URL(API_BASE_URL + path);

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.append(key, value);
    }
  });

  const resp = await fetch(url);
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

// fonctions spécialisées (pour chaque endpoint)
function fetchArrondissements() {
  return apiGet("/arrondissements");
}
function fetchPrix(annee, arrondissement) {
  return apiGet("/prix", { annee, arrondissement });
}
function fetchTimeline(arr) {
  return apiGet("/timeline", { arr });
}
function fetchLogementsSociaux(arrondissement) {
  return apiGet("/logements_sociaux", { arrondissement });
}
function fetchTypologie(arrondissement, annee) {
  return apiGet("/typologie", { arrondissement, annee });
}
function fetchEspacesVerts(arrondissement) {
  return apiGet("/espaces_verts", { arrondissement });
}
function fetchEcoles(arrondissement) {
  return apiGet("/etablissements_scolaires", { arrondissement });
}
function fetchAbribacs(arrondissement) {
  return apiGet("/abribacs", { arrondissement });
}

// =====================================================
// POINT D'ENTRÉE : LA LOGIQUE D'APPLICATION COMMENCE ICI
// =====================================================

document.addEventListener("DOMContentLoaded", async () => {
  // --- DOM ---
  const arrSelect = document.getElementById("arr-select");
  const arrCompareA = document.getElementById("arr-compare-a");
  const arrCompareB = document.getElementById("arr-compare-b");
  const anneeInput = document.getElementById("annee-input");
  const typologieAnneeInput = document.getElementById("typologie-annee");

  const btnPrix = document.getElementById("btn-prix");
  const btnTimeline = document.getElementById("btn-timeline");
  const btnLogements = document.getElementById("btn-logements-sociaux");
  const btnTypologie = document.getElementById("btn-typologie");
  const btnVerts = document.getElementById("btn-espaces-verts");
  const btnEcoles = document.getElementById("btn-ecoles");
  const btnAbribacs = document.getElementById("btn-abribacs"); 
  const btnCompare = document.getElementById("btn-compare");

  const resultPre = document.getElementById("result");
  const timelineCanvas = document.getElementById("timelineChart");
  const compareTimelineCanvas = document.getElementById("compareTimelineChart");
  const logementsCanvas = document.getElementById("logementsChart");
  const typologieCanvas = document.getElementById("typologieChart");
  const vertsCanvas = document.getElementById("vertsChart");
  const ecolesCanvas = document.getElementById("ecolesChart");
  const abribacsCanvas = document.getElementById("abribacsChart"); 

  const kpiPrice = document.getElementById("kpi-price");
  const kpiLogements = document.getElementById("kpi-logements");
  const kpiVerts = document.getElementById("kpi-verts");
  const badgeSelection = document.getElementById("badge-selection");
  const lastUpdate = document.getElementById("last-update");

  // --- charts instances ---
  let timelineChart = null;
  let compareChart = null;
  let logementsChart = null;
  let typologieChart = null;
  let vertsChart = null;
  let ecolesChart = null;
  let abribacsChart = null; 

  const chartDefaults = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        labels: { color: "#e5e7eb" },
      },
      tooltip: {
        backgroundColor: "#0f172a",
        borderColor: "#1f2937",
        borderWidth: 1,
      },
    },
    scales: {
      x: {
        ticks: { color: "#cbd5e1" },
        grid: { color: "rgba(255,255,255,0.06)" },
      },
      y: {
        ticks: { color: "#cbd5e1" },
        grid: { color: "rgba(255,255,255,0.06)" },
      },
    },
  };

  // utils
  function stringify(data) {
    resultPre.textContent = JSON.stringify(data, null, 2);
  }

  function formatNumber(value, opts = {}) {
    if (value === undefined || value === null || Number.isNaN(value)) return "–";
    return Number(value).toLocaleString("fr-FR", {
      maximumFractionDigits: 1,
      ...opts,
    });
  }

  function updateStatus() {
    const arrText = arrSelect.value ? `${arrSelect.value}e` : "Tous";
    badgeSelection.textContent = arrText;
    const now = new Date();
    lastUpdate.textContent = now.toLocaleString("fr-FR", {
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  function applyOptionsToSelect(select, arrondissements) {
    select.innerHTML = '<option value="">—</option>';
    arrondissements.forEach((arr) => {
      const opt = document.createElement("option");
      opt.value = arr;
      opt.textContent = `${arr}e`;
      select.appendChild(opt);
    });
  }

  // ---- chargement arrondissements ----
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

  // =====================================================
  //   FONCTIONS DE CHARGEMENT
  // =====================================================

  async function loadPrix() {
    const arr = arrSelect.value || undefined;
    const annee = anneeInput.value ? parseInt(anneeInput.value, 10) : undefined;

    stringify({ info: `Chargement des prix pour ${annee || 'toutes années'}...` });
    try {
      const data = await fetchPrix(annee, arr ? parseInt(arr, 10) : undefined);
      stringify(data);
      const source = Array.isArray(data) ? data[0] || {} : data || {};
      const prix =
        source.prix_m2_median ??
        source.prix_m2 ??
        source.prix ??
        source.prix_median ??
        null;
      kpiPrice.textContent =
        prix !== null ? `${formatNumber(prix, { maximumFractionDigits: 0 })} €` : "N/A";
    } catch (e) {
      console.error(e);
      stringify({ error: e.message });
      kpiPrice.textContent = "N/A";
    }
    updateStatus();
  }

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
        alert("Pas de données pour cette timeline.");
        return;
      }

      if (timelineChart) timelineChart.destroy();

      timelineChart = new Chart(timelineCanvas, {
        type: "line",
        data: {
          labels,
          datasets: [
            {
              label: `Prix/m² médian - ${arr}e`,
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

  async function loadTypologie({ showAlert = false } = {}) {
    const arr = arrSelect.value;
    const anneeVal = typologieAnneeInput.value;

    if (!arr || !anneeVal) {
      if (showAlert) alert("Choisis un arrondissement ET une année.");
      return;
    }

    const annee = parseInt(anneeVal, 10);
    stringify({ info: "Chargement de la typologie..." });
    try {
      const data = await fetchTypologie(parseInt(arr, 10), annee);
      stringify(data);

      if (!Array.isArray(data) || !data.length) {
        if (showAlert) alert("Pas de données de typologie pour ces filtres.");
        return;
      }

      const row = data[0];
      const keys = Object.keys(row).filter(
        (k) => !["arrondissement", "annee"].includes(k)
      );
      if (!keys.length) {
        if (showAlert) alert("Aucune colonne de typologie exploitable.");
        return;
      }

      const labels = keys;
      const values = keys.map((k) => row[k]);

      if (typologieChart) typologieChart.destroy();

      typologieChart = new Chart(typologieCanvas, {
        type: "doughnut",
        data: {
          labels,
          datasets: [
            {
              label: `Typologie ${arr}e en ${annee}`,
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
        options: chartDefaults,
      });
    } catch (e) {
      console.error(e);
      stringify({ error: e.message });
    }
    updateStatus();
  }

  async function loadLogements() {
    const arr = arrSelect.value || undefined;

    stringify({ info: "Chargement des logements sociaux..." });
    try {
      const data = await fetchLogementsSociaux(undefined);
      stringify(data);

      if (!Array.isArray(data) || !data.length) {
        alert("Pas de données logements sociaux.");
        kpiLogements.textContent = "N/A";
        return;
      }

      const numericKeys = Object.keys(data[0]).filter(
        (k) => !["arrondissement", "annee"].includes(k)
      );
      if (!numericKeys.length) {
        alert("Colonnes numériques introuvables pour logements sociaux.");
        return;
      }
      const valueKey = numericKeys[0];

      const labels = data.map((row) => row.arrondissement);
      const values = data.map((row) => row[valueKey]);

      if (logementsChart) logementsChart.destroy();

      logementsChart = new Chart(logementsCanvas, {
        type: "bar",
        data: {
          labels,
          datasets: [
            {
              label: `${valueKey} (%)`,
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
              title: { display: true, text: valueKey, color: "#cbd5e1" },
            },
          },
        },
      });

      if (arr) {
        const selectedRow = data.find(
          (row) => Number(row.arrondissement) === Number(arr)
        );
        const firstVal = selectedRow ? selectedRow[valueKey] : null;
        kpiLogements.textContent =
          firstVal !== undefined && firstVal !== null
            ? `${formatNumber(firstVal)} %`
            : "N/A";
      } else {
        kpiLogements.textContent = "–";
      }
    } catch (e) {
      console.error(e);
      stringify({ error: e.message });
      kpiLogements.textContent = "N/A";
    }
    updateStatus();
  }

  async function loadVerts() {
    const arr = arrSelect.value || undefined;

    stringify({ info: "Chargement des espaces verts..." });
    try {
      const data = await fetchEspacesVerts(undefined);
      stringify(data);

      if (!Array.isArray(data) || !data.length) {
        alert("Pas de données d'espaces verts.");
        kpiVerts.textContent = "N/A";
        return;
      }

      const numericKeys = Object.keys(data[0]).filter(
        (k) => !["arrondissement"].includes(k)
      );
      if (!numericKeys.length) {
        alert("Colonnes numériques introuvables pour espaces verts.");
        return;
      }
      const valueKey = numericKeys[0];

      const labels = data.map((row) => row.arrondissement);
      const values = data.map((row) => row[valueKey]);

      if (vertsChart) vertsChart.destroy();

      vertsChart = new Chart(vertsCanvas, {
        type: "bar",
        data: {
          labels,
          datasets: [
            {
              label: valueKey,
              data: values,
              backgroundColor: "rgba(132, 204, 22, 0.35)",
              borderColor: "#84cc16",
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
              title: { display: true, text: valueKey, color: "#cbd5e1" },
            },
          },
        },
      });

      if (arr) {
        const selectedRow = data.find(
          (row) => Number(row.arrondissement) === Number(arr)
        );
        const firstVal = selectedRow ? selectedRow[valueKey] : null;
        kpiVerts.textContent =
          firstVal !== undefined && firstVal !== null
            ? formatNumber(firstVal)
            : "N/A";
      } else {
        kpiVerts.textContent = "–";
      }
    } catch (e) {
      console.error(e);
      stringify({ error: e.message });
      kpiVerts.textContent = "N/A";
    }
    updateStatus();
  }

  async function loadEcoles() {
    stringify({ info: "Chargement des établissements scolaires..." });
    try {
      const data = await fetchEcoles(undefined);
      stringify(data);

      if (!Array.isArray(data) || !data.length) {
        alert("Pas de données d'écoles.");
        return;
      }

      const labels = data.map((row) => row.arrondissement);
      const values = data.map((row) => row.nb_total_ecoles ?? 0);

      if (ecolesChart) ecolesChart.destroy();

      ecolesChart = new Chart(ecolesCanvas, {
        type: "bar",
        data: {
          labels,
          datasets: [
            {
              label: "Nombre total d'écoles",
              data: values,
              backgroundColor: "rgba(251, 113, 133, 0.35)",
              borderColor: "#fb7185",
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
              title: { display: true, text: "Établissements", color: "#cbd5e1" },
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
  
  async function loadAbribacs() {
    stringify({ info: "Chargement des Abribacs / PAVDA..." });
    try {
      const data = await fetchAbribacs(undefined);
      stringify(data);

      if (!Array.isArray(data) || !data.length) {
        alert("Pas de données Abribacs / PAVDA.");
        return;
      }

      const numericKeys = Object.keys(data[0]).filter(
        (k) => !["arrondissement"].includes(k)
      );
      if (!numericKeys.length) {
        alert("Colonnes numériques introuvables pour Abribacs / PAVDA.");
        return;
      }
      const valueKey = numericKeys[0];

      const labels = data.map((row) => row.arrondissement);
      const values = data.map((row) => row[valueKey]);

      if (abribacsChart) abribacsChart.destroy();

      abribacsChart = new Chart(abribacsCanvas, {
        type: "bar",
        data: {
          labels,
          datasets: [
            {
              label: "Nombre d'Abribacs (PAVDA)",
              data: values,
              backgroundColor: "rgba(192, 132, 252, 0.35)",
              borderColor: "#c084fc",
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
              title: { display: true, text: "Points de collecte", color: "#cbd5e1" },
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

  async function loadComparison() {
    const arrA = arrCompareA.value;
    const arrB = arrCompareB.value;
    if (!arrA || !arrB) {
      alert("Choisis deux arrondissements pour comparer.");
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

      const labels = Array.from(
        new Set([
          ...timelineA.map((d) => d.annee),
          ...timelineB.map((d) => d.annee),
        ]).values()
      ).sort();

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
              label: `Arr. ${arrA}`,
              data: datasetA,
              tension: 0.25,
              borderColor: "#22d3ee",
              backgroundColor: "rgba(34, 211, 238, 0.18)",
              fill: false,
            },
            {
              label: `Arr. ${arrB}`,
              data: datasetB,
              tension: 0.25,
              borderColor: "#f59e0b",
              backgroundColor: "rgba(245, 158, 11, 0.18)",
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


  // =====================================================
  //   LISTENERS (Les branchements finaux)
  // =====================================================

  // Événements des boutons (clic)
  btnPrix.addEventListener("click", loadPrix);
  btnTimeline.addEventListener("click", () => {
    if (!arrSelect.value) {
      alert("Choisis un arrondissement d'abord.");
      return;
    }
    loadTimeline();
  });
  btnTypologie.addEventListener("click", () =>
    loadTypologie({ showAlert: true })
  );
  btnLogements.addEventListener("click", loadLogements);
  btnVerts.addEventListener("click", loadVerts);
  btnEcoles.addEventListener("click", loadEcoles);
  btnAbribacs.addEventListener("click", loadAbribacs); 
  btnCompare.addEventListener("click", loadComparison);

  // Événements des filtres (change)
  
  // FIX: L'année pour le prix déclenche la mise à jour
  anneeInput.addEventListener("change", loadPrix); 
  
  // L'année pour la typologie déclenche la mise à jour
  typologieAnneeInput.addEventListener("change", () =>
    loadTypologie({ showAlert: false })
  );

  // Le changement d'arrondissement déclenche TOUTES les mises à jour
  arrSelect.addEventListener("change", () => {
    updateStatus();
    loadTimeline();
    // Si une année est remplie pour le prix, on le charge.
    if (anneeInput.value) loadPrix(); 
    loadTypologie({ showAlert: false });
    loadLogements();
    loadVerts();
    loadEcoles();
    loadAbribacs();
  });
});