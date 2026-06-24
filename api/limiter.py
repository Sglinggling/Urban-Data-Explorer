"""
Initialisation du limiteur de requêtes HTTP de l'API.
Restreint le nombre d'appels par adresse IP pour protéger les ressources serveur.
"""
from slowapi import Limiter
from slowapi.util import get_remote_address

# Identifie chaque client par son adresse IP pour appliquer les quotas par utilisateur
limiter = Limiter(key_func=get_remote_address)
