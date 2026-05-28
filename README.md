# iLog Kafka Delta Sink

Elaborato finale — Corso di Laurea in Informatica, Università di Trento  
Laureanda: **Alessia Angeli** | Supervisore: **Prof. Fausto Giunchiglia** | Co-supervisore: **Leonardo Javier Malcotti**  
Anno accademico 2024/2025

---

## Descrizione

Questo repository contiene il codice e la documentazione dell'elaborato finale dal titolo  
**"Un connettore per lo storage dei dati per la piattaforma iLog"**.

Il progetto propone una nuova architettura per la gestione dei dati in streaming della piattaforma **iLog**, evolvendo la soluzione esistente (Cassandra + PostgreSQL) con una pipeline basata su tecnologie moderne per Big Data. I dati vengono letti da Apache Kafka, processati da un job Apache Flink e scritti in formato Delta Lake (Parquet) su MinIO, il tutto orchestrato su Kubernetes.

---

## Architettura

```
┌──────────────────────────────────────────────────────────┐
│                        Kubernetes                        │
│                                                          │
│  ┌──────────┐    ┌──────────────┐    ┌─────────────────┐ │
│  │  Kafka   │───▶│  Flink Job   │───▶│  MinIO (S3)     │ │
│  │(+Zoo-    │    │  (Job +      │    │  Delta Lake /   │ │
│  │ keeper)  │    │  Task Mgr)   │    │  Parquet files  │ │
│  └──────────┘    └──────────────┘    └─────────────────┘ │
│       ▲                                                   │
│  ┌──────────────────┐                                     │
│  │ message-producer │  (Spring Boot — genera messaggi     │
│  │  (Spring Boot)   │   di test su Kafka)                 │
│  └──────────────────┘                                     │
└──────────────────────────────────────────────────────────┘
```

---

## Struttura del repository

```
.
├── k8s/                         # Manifest Kubernetes
│   ├── configMapFlink.yaml      # ConfigMap con config Flink, S3 e logging
│   ├── jobmanager.yaml          # Deployment + Service del JobManager Flink
│   ├── taskmanager.yaml         # Deployment dei TaskManager Flink (2 repliche)
│   ├── kafka.yaml               # Deployment + Service di Kafka
│   ├── kafka-ui.yaml            # Kafka UI (Provectus)
│   ├── zookeeper.yaml           # Deployment + Service di Zookeeper
│   ├── minio.yaml               # Deployment + Service di MinIO
│   ├── minio-console.yaml       # Ingress per MinIO Console
│   └── message-producer.yaml    # Deployment + Service + Ingress del producer
│
├── src/                         # Codice sorgente (Spring Boot — message producer)
├── Dockerfile                   # Build multi-stage (Maven + Amazon Corretto 17)
├── pom.xml                      # Dipendenze Maven (Spring Boot 3.3.2, Kafka)
├── mvnw / mvnw.cmd              # Maven wrapper
├── tesi/
│   └── Tesi_Angeli_Alessia_226817.pdf
└── README.md
```

---

## Tecnologie utilizzate

| Componente | Tecnologia | Versione |
|---|---|---|
| Streaming source | Apache Kafka (Bitnami) | 3.x |
| Coordinamento Kafka | Apache Zookeeper (Bitnami) | 3.8 |
| Stream processing | Apache Flink | 1.19.1 |
| Storage layer | Delta Lake | — |
| Object storage | MinIO (Bitnami) | latest |
| File format | Apache Parquet | — |
| Message producer | Spring Boot | 3.3.2 |
| Linguaggio | Java | 17 |
| Orchestrazione | Kubernetes | — |
| Build | Maven | 3.9.1 |

---

## Prerequisiti

- Kubernetes cluster attivo (es. Minikube, k3s o cluster remoto)
- `kubectl` configurato
- Docker (per la build dell'immagine del message producer)

---

## Deploy su Kubernetes

Applicare i manifest nell'ordine seguente:

```bash
# 1. Zookeeper (richiesto da Kafka)
kubectl apply -f k8s/zookeeper.yaml

# 2. Kafka
kubectl apply -f k8s/kafka.yaml

# 3. Kafka UI (opzionale — per monitorare i topic)
kubectl apply -f k8s/kafka-ui.yaml

# 4. MinIO
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/minio-console.yaml

# 5. Flink (ConfigMap prima dei pod)
kubectl apply -f k8s/configMapFlink.yaml
kubectl apply -f k8s/jobmanager.yaml
kubectl apply -f k8s/taskmanager.yaml

# 6. Message producer
kubectl apply -f k8s/message-producer.yaml
```

Per verificare che i pod siano in esecuzione:

```bash
kubectl get pods
```

---

## Build del message producer

```bash
# Build locale
./mvnw clean package

# Build immagine Docker
docker build -t message-producer:0.0 .
```

> L'immagine usa un build multi-stage: Maven 3.9.1 + Amazon Corretto 17 per la build, Amazon Corretto 17 Alpine come runtime finale.

---

## Configurazione

Le credenziali e gli endpoint sono definiti in `k8s/configMapFlink.yaml`:

```yaml
s3.access-key: miniouser
s3.secret-key: miniopassword
s3.endpoint: http://minio-service:9000
s3.path.style.access: true
```

Le credenziali MinIO sono impostate in `k8s/minio.yaml`:

```yaml
MINIO_ROOT_USER: miniouser
MINIO_ROOT_PASSWORD: miniopassword
```

> ⚠️ In un ambiente di produzione sostituire le credenziali con Kubernetes Secrets.

---

## Formato dei messaggi Kafka

I messaggi prodotti e consumati sono in formato JSON:

```json
{
  "id": "sensor-001",
  "timestamp": 1700000000.0,
  "content": "valore rilevato dal sensore"
}
```

---

## Porte esposte

| Servizio | Porta interna | NodePort |
|---|---|---|
| Kafka | 9092 | 31092 |
| Kafka (external) | 9097 | 31097 |
| Kafka UI | 8080 | — |
| Flink UI | 8081 | — |
| MinIO API | 9000 | — |
| MinIO Console | 9001 | — |
| Message Producer | 8080 | — |

---

## Limitazioni note

Durante lo sviluppo è emerso un problema con il **checkpointing di Flink**: i checkpoint si avviavano ma non si completavano mai, impedendo il commit dei dati sul Delta Sink. L'analisi ha portato a ipotizzare che l'API `DeltaSink.forRowData()` fosse deprecata nelle versioni più recenti del connettore.

Possibili soluzioni future:
- Aggiornare il connettore Delta a una versione compatibile con Flink 1.17+
- Valutare `FileSystemLogSink` come alternativa
- Aggiungere strumenti di monitoraggio avanzati per isolare il punto critico della pipeline

---

## Riferimenti

- [iLog — A methodology and platform for high-quality rich personal data](https://arxiv.org/abs/2501.16864)
- [Delta Lake documentation](https://docs.delta.io/latest/index.html)
- [Apache Flink documentation](https://nightlies.apache.org/flink/flink-docs-master/)
- [Apache Kafka documentation](https://kafka.apache.org/documentation/)
- [MinIO documentation](https://min.io/product/overview)
