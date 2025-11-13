FROM python:3.11-slim

WORKDIR /app

# Installation de la librairie Kafka
RUN pip install --no-cache-dir kafka-python

# Copie du script producteur
COPY producer.py .

# Le script sera lancé automatiquement au démarrage du conteneur
CMD ["python", "producer.py"]
