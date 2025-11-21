console.log("api_client.js chargé");

const API_BASE_URL = "http://localhost:8000/api";

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
    } catch (_) {}
    throw new Error(errMsg);
  }
  return resp.json();
}

// Fonctions globales
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

function fetchEtablissementsScolaires(arrondissement) {
  return apiGet("/etablissements_scolaires", { arrondissement });
}
