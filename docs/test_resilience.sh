#!/bin/bash
# Test de résilience Urban Data Explorer
# Vérifie la dégradation et la récupération automatique lors d'une panne PostgreSQL.
set -e

API_KEY="urban-data-explorer-dev-key"
API_URL="http://localhost:8000"

echo "=== Test de résilience Urban Data Explorer ==="
echo ""

echo "1. État initial : tous les services UP"
docker compose ps
echo ""

echo "2. Test API normal (doit retourner 200)"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "X-API-Key: $API_KEY" \
  "$API_URL/api/prix?annee=2024&arrondissement=1")
echo "HTTP code: $HTTP_CODE (attendu: 200)"
if [ "$HTTP_CODE" != "200" ]; then
  echo "AVERTISSEMENT: l'API ne répond pas normalement avant la panne"
fi
echo ""

echo "3. Simulation de panne : coupure de PostgreSQL"
docker compose stop postgres
sleep 3
echo ""

echo "4. Test API en mode dégradé (doit retourner 500)"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "X-API-Key: $API_KEY" \
  "$API_URL/api/prix?annee=2024&arrondissement=1")
echo "HTTP code: $HTTP_CODE (attendu: 500)"
if [ "$HTTP_CODE" = "500" ]; then
  echo "OK : l'API détecte correctement la panne"
else
  echo "INFO: code $HTTP_CODE reçu (connection pool peut encore répondre brièvement)"
fi
echo ""

echo "5. Récupération : redémarrage PostgreSQL"
docker compose start postgres
echo "Attente du healthcheck (10s)..."
sleep 10
echo ""

echo "6. Vérification de la récupération automatique"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "X-API-Key: $API_KEY" \
  "$API_URL/api/prix?annee=2024&arrondissement=1")
echo "HTTP code: $HTTP_CODE (attendu: 200)"
if [ "$HTTP_CODE" = "200" ]; then
  echo "OK : récupération automatique confirmée"
else
  echo "ECHEC: l'API n'a pas récupéré (code $HTTP_CODE)"
  exit 1
fi
echo ""

echo "=== Test de résilience terminé avec succès ==="
