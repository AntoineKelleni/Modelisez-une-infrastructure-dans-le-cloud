FROM eclipse-temurin:17-jdk-jammy

# 1. Installer Python 3 + pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# 2. Dossier de travail
WORKDIR /app

# 3. Installer PySpark (version alignée avec le connecteur 3.5.0)
RUN pip3 install --no-cache-dir pyspark==3.5.0

# 4. Copier le script PySpark
COPY pyspark_app.py .

# 5. Indiquer à PySpark d'utiliser python3
ENV PYSPARK_PYTHON=python3

# 6. Commande de démarrage
CMD ["python3", "pyspark_app.py"]
