# Phase 1 — Kafka + Producteur d'événements de courses

## C'est quoi Phase 1 ?

On simule une application de covoiturage qui génère des événements en temps réel
(une course est demandée, acceptée, démarrée, terminée ou annulée) et on les envoie
dans Apache Kafka. C'est la **porte d'entrée** de tout le pipeline.

Tout tourne dans Docker — aucune installation Python locale nécessaire.

---

## Architecture

```
[Conteneur Producer — Python 3.11]
    │
    │  génère des faux événements de courses
    ▼
[Conteneur Kafka]  ← topic: ride_events (3 partitions)
                      topic: ride_events_dlq (messages en erreur)
    │
    ▼
[Conteneur Kafka UI — http://localhost:8080]
    (interface web pour visualiser les messages)
    │
    ▼
[Phase 2 → PySpark lit depuis Kafka]
```

---

## Les fichiers

### `Dockerfile`
Construit l'image du producteur à partir de **Python 3.11 slim**.
Installe les dépendances système (`librdkafka`), puis les packages Python via
`requirements.txt`, et copie le code source.

Utiliser Python 3.11 (et non 3.14) garantit que tous les packages data engineering
ont des wheels pré-compilés disponibles — pas besoin de C++ Build Tools.

### `infra/docker-compose.yml`
Lance tous les conteneurs avec une seule commande. Contient :

| Service | Rôle | Port |
|---------|------|------|
| `zookeeper` | Coordination interne de Kafka | 2181 |
| `kafka` | Broker de messages | 9092 |
| `kafka-ui` | Interface web de visualisation | 8080 |
| `kafka-init` | Crée les topics au démarrage | — |
| `producer` | Producteur d'événements (notre code) | — |

Le producteur attend que `kafka` soit **healthy** et que `kafka-init` soit
**terminé** avant de démarrer — garantit que les topics existent avant le
premier message.

> **Important** — à l'intérieur de Docker, le broker s'appelle `kafka:29092`
> (réseau interne), pas `localhost:9092`. C'est pourquoi la variable
> `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` est définie dans le service producer.

### `.dockerignore`
Exclut du build Docker les fichiers inutiles (`.git`, `.venv`, fichiers MD, cache
Python…). Réduit la taille du contexte de build et évite de copier des secrets
dans l'image.

### `config/settings.py`
Toute la configuration en un seul endroit. Chaque valeur lit d'abord une variable
d'environnement, puis utilise une valeur par défaut. Le `docker-compose.yml` injecte
les variables nécessaires via la section `environment`.

### `producer/ride_event_generator.py`
Génère de faux événements réalistes. Utilise :
- **Faker** pour les adresses
- **Haversine** pour la distance GPS entre départ et arrivée
- Des coordonnées réelles (New York, San Francisco, Chicago…)

### `producer/kafka_producer.py`
Publie les événements vers Kafka. Points clés :
- Messages **identifiés par `trip_id`** → toutes les étapes d'une course vont sur
  la même partition (ordre garanti)
- Échecs → renvoyés dans le **DLQ** au lieu d'être perdus
- Arrêt propre sur Ctrl+C / `docker-compose down`

---

## Le schéma d'un événement

```json
{
  "event_id":         "uuid unique par événement",
  "trip_id":          "uuid partagé par toute la course",
  "driver_id":        "DRV-4A2B9F1C",
  "rider_id":         "RDR-7E3D0A8B",
  "status":           "completed",
  "city":             "New York",
  "pickup":           { "lat": 40.71, "lon": -74.00, "address": "123 Broadway" },
  "dropoff":          { "lat": 40.75, "lon": -73.98, "address": "456 Park Ave" },
  "fare_usd":         18.50,
  "surge_multiplier": 1.40,
  "total_fare_usd":   25.90,
  "distance_miles":   3.72,
  "timestamp":        "2026-09-25T14:30:00Z",
  "event_version":    "1.0"
}
```

---

## Comment lancer (100% Docker)

### Prérequis
- Docker Desktop installé et démarré — c'est tout.

### Démarrer tout le pipeline

```bash
cd infra
docker compose up --build
```

`--build` reconstruit l'image du producteur à la première exécution.
Les fois suivantes, `docker compose up` suffit.

### Vérifier que ça fonctionne

1. Ouvrir **http://localhost:8080** (Kafka UI)
2. Aller dans **Topics → ride_events → Messages**
3. Les événements JSON doivent apparaître en temps réel

### Voir les logs du producteur

```bash
docker compose logs -f producer
```

### Modes de lancement alternatifs

```bash
# Produire exactement 100 événements puis s'arrêter
docker compose run producer python producer/kafka_producer.py --max-events 100

# Simuler 5 courses complètes (requested → accepted → started → completed)
docker compose run producer python producer/kafka_producer.py --lifecycle --trips 5

# Changer la vitesse (50 événements/sec)
docker compose run producer python producer/kafka_producer.py --events-per-second 50
```

### Arrêter tout

```bash
docker compose down
```

---

## Prochaine étape — Phase 2

PySpark va lire les événements depuis Kafka et les écrire dans Delta Lake :
- **Bronze** — données brutes telles quelles
- **Silver** — nettoyées et dédupliquées
- **Gold** — métriques agrégées (courses par ville, tarif moyen, taux d'annulation…)
