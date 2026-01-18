# Utiliser une image Python légère
FROM python:3.11-slim

# Répertoire de travail
WORKDIR /app

# Copier les fichiers de dépendances et installer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste du code
COPY . .

# Créer les dossiers nécessaires
RUN mkdir -p uploads outputs

# Recevoir les variables de Coolify comme ARG (build-time)
ARG SUPABASE_URL
ARG SUPABASE_KEY

# Les convertir en ENV (runtime) pour que l'application puisse les lire
ENV SUPABASE_URL=${SUPABASE_URL}
ENV SUPABASE_KEY=${SUPABASE_KEY}

# Exposer le port 5000
EXPOSE 5000

# Commande de démarrage (Gunicorn pour la production)
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "app:app"]
