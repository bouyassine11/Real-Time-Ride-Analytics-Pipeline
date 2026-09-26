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

---

---

# Phase 2 — PySpark Structured Streaming + Delta Lake

## C'est quoi Phase 2 ?

On consomme les événements de Kafka avec **PySpark Structured Streaming** et on
les transforme en trois couches Delta Lake : Bronze (brut), Silver (propre),
Gold (agrégé). C'est l'architecture **Medallion**, standard dans les pipelines
de données modernes.

---

## Architecture complète

```
[Kafka: ride_events]
        │
        ▼
[spark-bronze]  →  Delta Bronze  (JSON brut + métadonnées Kafka)
        │
        ▼
[spark-silver]  →  Delta Silver  (typé, dédupliqué, validé)
        │
        ▼
[spark-gold]    →  Delta Gold/rides_per_city    (nb courses/ville/minute)
                →  Delta Gold/fare_metrics      (tarif moyen, revenus)
                →  Delta Gold/cancellation      (taux d'annulation)
```

---

## Les fichiers

### `delta/schemas.py`
Définit les schémas PySpark (`StructType`) pour chaque couche.
Tous les jobs Spark importent depuis ici — un seul endroit à modifier si le
schéma évolue.

| Schéma | Utilisé par |
|--------|------------|
| `BRONZE_SCHEMA` | bronze_ingestion.py |
| `SILVER_SCHEMA` | silver_transform.py |
| `GOLD_RIDES_PER_CITY_SCHEMA` | gold_aggregations.py |
| `GOLD_FARE_METRICS_SCHEMA` | gold_aggregations.py |
| `GOLD_CANCELLATION_SCHEMA` | gold_aggregations.py |

### `spark/bronze_ingestion.py`
**Kafka → Delta Bronze**

- Lit les messages bruts depuis Kafka (`value` = JSON en bytes)
- Convertit en string UTF-8 sans toucher au contenu
- Ajoute les métadonnées : `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingestion_timestamp`
- Écrit en mode **append** dans Delta Bronze toutes les 10 secondes
- Sauvegarde sa progression via un **checkpoint** → peut reprendre sans relire Kafka depuis le début

### `spark/silver_transform.py`
**Delta Bronze → Delta Silver**

- Lit la table Bronze en streaming
- Parse le JSON avec `from_json()` en colonnes typées
- **Filtre** les enregistrements invalides (event_id null, status invalide, timestamp manquant)
- **Déduplique** par `event_id` via un **Delta MERGE** (upsert) : si l'event existe déjà → ignoré, sinon → inséré
- Garantit qu'aucun événement n'apparaît deux fois même si le producteur a retenté

### `spark/gold_aggregations.py`
**Delta Silver → 3 tables Gold**

Utilise des **fenêtres glissantes de 1 minute** sur `event_timestamp` (heure réelle de l'événement, pas de traitement).

| Table Gold | Ce qu'elle contient |
|-----------|---------------------|
| `rides_per_city` | Nombre de courses par ville par minute |
| `fare_metrics` | Tarif moyen, surge moyen, revenus totaux (courses terminées uniquement) |
| `cancellation` | Nombre d'annulations et taux d'annulation par ville par minute |

**Watermark de 2 minutes** : Spark attend jusqu'à 2 minutes les événements en retard
avant de fermer une fenêtre. Au-delà → ignorés.

---

## Nouveaux services Docker

| Service | Image | Rôle |
|---------|-------|------|
| `spark-bronze` | `bitnami/spark:3.5.1` | Job Bronze en continu |
| `spark-silver` | `bitnami/spark:3.5.1` | Job Silver en continu |
| `spark-gold` | `bitnami/spark:3.5.1` | Job Gold en continu |

Les données Delta sont persistées dans un **volume Docker** (`delta-data`) —
elles survivent aux redémarrages des conteneurs.

---

## Comment lancer Phase 1 + Phase 2

```bash
cd infra
docker compose up --build
```

Tous les services démarrent dans le bon ordre :
`zookeeper → kafka → kafka-init → producer → spark-bronze → spark-silver → spark-gold`

### Voir les logs de chaque job Spark

```bash
docker compose logs -f spark-bronze
docker compose logs -f spark-silver
docker compose logs -f spark-gold
```

### Démarrer uniquement l'infrastructure + le producteur (sans Spark)

```bash
docker compose up zookeeper kafka kafka-ui kafka-init producer
```

### Démarrer Spark séparément

```bash
docker compose up spark-bronze spark-silver spark-gold
```

---

## Comment vérifier que Phase 2 fonctionne

**1. Logs des jobs Spark** — pas d'erreur `WARN` ou `ERROR` répétée

**2. Fichiers Delta créés** — inspecter le volume depuis un conteneur :
```bash
docker compose run spark-bronze ls /app/delta_data/
# Doit afficher : bronze/  silver/  gold/  checkpoints/
```

**3. Lire les données Gold directement** :
```bash
docker compose run spark-bronze bash -c "
pip install delta-spark==3.1.0 --quiet &&
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  - << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('check').getOrCreate()
spark.read.format('delta').load('/app/delta_data/gold/rides_per_city').show(10)
EOF
"
```

---

## Prochaine étape — Phase 3

**Apache Airflow** va orchestrer l'ensemble du pipeline :
- DAG qui démarre le producteur, les jobs Spark, et vérifie les résultats
- Alertes SLA si un job prend trop longtemps
- Checks de qualité des données sur les tables Gold

---

---

# Phase 3 — Visualisation Grafana

## C'est quoi Phase 3 ?

On ajoute une couche de visualisation pour afficher les KPIs en temps réel.
Grafana ne peut pas lire les fichiers Delta directement — une **API FastAPI** fait
le lien entre les tables Gold et Grafana.

```
[Delta Gold Tables]
        │
        ▼
[API FastAPI — port 8000]   ← lit les Parquet Delta, retourne du JSON
        │
        ▼
[Grafana — port 3000]       ← interroge l'API toutes les 10 secondes
        │
        ▼
[Dashboard avec 10 panneaux KPI]
```

---

## Les fichiers

### `api/main.py`
Service FastAPI avec 6 endpoints :

| Endpoint | Ce qu'il retourne |
|----------|------------------|
| `GET /health` | Statut de l'API (pour Docker healthcheck) |
| `GET /metrics/rides-per-city` | Courses par ville par fenêtre de 1 min |
| `GET /metrics/fare-metrics` | Tarif moyen, surge, revenus par ville |
| `GET /metrics/cancellation-rate` | Taux d'annulation par ville |
| `GET /metrics/summary` | KPIs globaux (total courses, revenus, tarif moyen) |
| `GET /metrics/rides-timeseries` | Format time series pour Grafana |

Utilise **`deltalake`** (Python pur, sans JVM) pour lire les fichiers Parquet Delta.
Tous les endpoints acceptent `?city=NAME` et `?limit=N` en paramètres.

### `api/Dockerfile`
Image Python 3.11 slim. Installe uniquement les packages nécessaires à l'API :
`fastapi`, `uvicorn`, `pandas`, `pyarrow`, `deltalake`. Pas de Spark ici.

### `grafana/provisioning/datasources/api.yml`
Configure automatiquement la datasource Grafana au démarrage — pointe vers
`http://api:8000`. Grafana n'a pas besoin d'être configuré manuellement.

### `grafana/provisioning/dashboards/dashboard.yml`
Indique à Grafana où trouver les fichiers JSON de dashboards.
Recharge automatiquement toutes les 30 secondes.

### `grafana/dashboards/ride_analytics.json`
Dashboard complet avec **10 panneaux** :

**Ligne 1 — KPIs instantanés (stat/gauge)**
| Panneau | Métrique |
|---------|---------|
| Total Rides | Nombre total de courses |
| Total Revenue | Revenus totaux en USD |
| Avg Fare | Tarif moyen en USD |
| Avg Surge | Multiplicateur de surge moyen (vert < 1.5, orange < 2.5, rouge > 2.5) |
| Cancellation Rate | Taux d'annulation en % (vert < 15%, orange < 30%, rouge > 30%) |

**Ligne 2 — Time series**
| Panneau | Métrique |
|---------|---------|
| Rides per City | Nombre de courses par ville au fil du temps |
| Avg Fare by City | Tarif moyen par ville au fil du temps |

**Ligne 3 — Comparaison par ville**
| Panneau | Métrique |
|---------|---------|
| Cancellation Rate by City | Barre horizontale par ville |
| Avg Surge by City | Barre horizontale par ville |
| Revenue by City | Donut chart — part de revenus par ville |

Le dashboard se **rafraîchit toutes les 10 secondes** et affiche les 30 dernières minutes par défaut.

---

## Nouveaux services Docker

| Service | Image | Port | Rôle |
|---------|-------|------|------|
| `api` | Python 3.11 (custom) | 8000 | Lit Delta Gold → JSON |
| `grafana` | `grafana/grafana:10.4.0` | 3000 | Dashboard KPI |

---

## Comment accéder au dashboard

```bash
cd infra
docker compose up --build
```

Puis ouvrir **http://localhost:3000** dans le navigateur.

- Login : `admin` / `admin`
- Le dashboard **"🚗 Real-Time Ride Analytics"** s'ouvre automatiquement
- Attendre ~2 minutes que Spark commence à écrire les tables Gold

### Vérifier l'API directement

```bash
# Health check
curl http://localhost:8000/health

# KPIs globaux
curl http://localhost:8000/metrics/summary

# Courses par ville (10 dernières fenêtres)
curl "http://localhost:8000/metrics/rides-per-city?limit=10"

# Filtrer sur une ville
curl "http://localhost:8000/metrics/fare-metrics?city=New+York"
```

---

## Architecture complète du pipeline

```
[Producteur Python]
        │  événements JSON (10/sec)
        ▼
[Kafka: ride_events — 3 partitions]
        │
        ▼
[spark-bronze]  →  Delta Bronze  (brut + métadonnées Kafka)
        │
        ▼
[spark-silver]  →  Delta Silver  (typé, dédupliqué, validé)
        │
        ▼
[spark-gold]    →  Delta Gold    (courses/ville, tarifs, annulations)
        │
        ▼
[API FastAPI]   →  JSON endpoints
        │
        ▼
[Grafana]       →  Dashboard temps réel — http://localhost:3000
```
