import os
import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "client_tickets")

# Nombre de tickets à envoyer (modifiable avec la variable d'env MAX_MESSAGES)
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "200"))

REQUEST_TYPES = ["incident", "question", "demande_evolution", "bug"]
PRIORITIES = ["LOW", "MEDIUM", "HIGH"]

REQUESTS_EXAMPLES = [
    "Problème de connexion à l'application",
    "Erreur sur la facture",
    "Demande d'ajout de fonctionnalité",
    "Lenteur générale du système",
    "Lenteur générale du système",
    "Question sur l'utilisation d'un module",
]


def generate_ticket() -> dict:
    return {
        "ticket_id": str(uuid.uuid4()),
        "client_id": random.randint(1, 1000),
        "created_at": datetime.utcnow().isoformat(),
        "request": random.choice(REQUESTS_EXAMPLES),
        "request_type": random.choice(REQUEST_TYPES),
        "priority": random.choice(PRIORITIES),
    }


def main():
    print(f"[PRODUCER] Connexion à {BOOTSTRAP_SERVERS}")
    print(f"[PRODUCER] Topic : {TOPIC_NAME}")
    print(f"[PRODUCER] Nombre de tickets à envoyer : {MAX_MESSAGES}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for i in range(MAX_MESSAGES):
        ticket = generate_ticket()
        producer.send(TOPIC_NAME, ticket)
        producer.flush()
        print(f"[PRODUCER] Ticket {i+1}/{MAX_MESSAGES} envoyé : {ticket}")
        time.sleep(1)

    print("[PRODUCER] Fin de l’envoi des tickets.")


if __name__ == "__main__":
    main()

