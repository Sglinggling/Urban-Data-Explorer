FROM nginx:alpine

# Config nginx custom (remplace le default)
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Fichiers statiques (index.html, style.css, js/, data/)
COPY . /usr/share/nginx/html/

EXPOSE 80
