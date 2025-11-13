import pandas as pd
from pathlib import Path

# Dossier où Spark écrit les fichiers parquet
OUTPUT_DIR = Path("data/outputs")

# Vérification de base
if not OUTPUT_DIR.exists():
    print(f" Le dossier {OUTPUT_DIR} n'existe pas.")
    exit(1)

# Lecture de tous les fichiers Parquet du dossier
print(f"Lecture des fichiers Parquet dans : {OUTPUT_DIR.resolve()}")

df = pd.read_parquet(OUTPUT_DIR)

print("\n Fichier(s) chargé(s) avec succès !\n")

print(" Aperçu des 20 premières lignes :")
print(df.head(20))

print("\n Colonnes disponibles :")
print(df.columns.tolist())

print("\n Nombre total de lignes :", len(df))

print("\n Exemple de répartition par type de demande :")
print(df["request_type"].value_counts())
print("\n Exemple de répartition par priorité :")
print(df["priority"].value_counts())

