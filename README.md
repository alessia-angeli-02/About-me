# iLog Kafka Delta Sink

Elaborato finale — Corso di Laurea in Informatica, Università di Trento  
Laureanda: **Alessia Angeli** | Supervisore: **Prof. Fausto Giunchiglia** | Co-supervisore: **Leonardo Javier Malcotti**  
Anno accademico 2024/2025

---

## Descrizione

Questo repository contiene il codice e la documentazione dell'elaborato finale dal titolo  
**"Un connettore per lo storage dei dati per la piattaforma iLog"**.

Il progetto propone una nuova architettura per la gestione dei dati in streaming della piattaforma **iLog**, sostituendo i database tradizionali (Cassandra e PostgreSQL) con una pipeline basata su tecnologie moderne per Big Data.

---

## Architettura proposta

```
Apache Kafka  →  Apache Flink  →  Delta Lake  →  MinIO (S3-compatible)
   (source)        (processing)     (format)       (object storage)
```

I dati generati dai sensori iLog vengono pubblicati su topic Kafka. Il job Flink li legge in streaming, li converte in formato **Parquet** tramite **Delta Lake** e li scrive in bucket **MinIO**, organizzati per esperimento iLog.

---

## Tecnologie utilizzate

| Componente | Tecnologia | Versione consigliata |
|---|---|---|
| Streaming source | Apache Kafka | 3.x |
| Stream processing | Apache Flink | 1.17+ |
| Storage layer | Delta Lake | compatibile con Flink 1.17+ |
| Object storage | MinIO | latest |
| File format | Apache Parquet | — |
| Orchestrazione | Kubernetes | — |
| Linguaggio | Java | 11+ |

---

## Struttura del repository

```
.
├── src/
│   └── main/java/demo/
│       ├── DemoApplication.java    # entry point del job Flink
│       ├── model/
│       │   └── Message.java        # POJO per i messaggi Kafka
│       └── ...
├── tesi/
│   └── Tesi_Angeli_Alessia_226817.pdf
└── README.md
```

---

## Prerequisiti

- Java 11 o superiore
- Apache Flink 1.17+ installato o cluster Kubernetes disponibile
- Istanza Kafka raggiungibile (es. `kafka-service:9092`)
- Istanza MinIO raggiungibile (es. `http://minio-service:9000`)
- Dipendenze Maven: `flink-connector-kafka`, `delta-flink`, `hadoop-aws`

---

## Configurazione

Prima di avviare il job, verificare i seguenti parametri in `DemoApplication.java`:

```java
// Kafka
.setBootstrapServers("kafka-service:9092")
.setTopics("my-topic")
.setGroupId("my-group")

// MinIO
hadoopConfig.set("fs.s3a.endpoint", "http://minio-service:9000");
hadoopConfig.set("fs.s3a.access.key", "miniouser");
hadoopConfig.set("fs.s3a.secret.key", "miniopassword");
hadoopConfig.set("fs.s3a.path.style", "true");

// Destinazione Delta Lake
new Path("s3a://your-bucket-name/table1")
```

In un ambiente di produzione questi valori andrebbero esternalizzati tramite file di configurazione o variabili d'ambiente.

---

## Avvio

Compilare il progetto con Maven:

```bash
mvn clean package
```

Sottomettere il job a Flink:

```bash
flink run -c demo.DemoApplication target/your-app.jar
```

---

## Formato dei messaggi Kafka

I messaggi attesi sul topic Kafka sono in formato JSON con la seguente struttura:

```json
{
  "id": "sensor-001",
  "timestamp": 1700000000.0,
  "content": "valore rilevato"
}
```

---

## Limitazioni note

Durante lo sviluppo è emerso un problema con il **checkpointing di Flink**: i checkpoint si avviavano ma non si completavano mai, impedendo il commit dei dati sul Delta Sink. L'analisi ha portato a ipotizzare che l'API `DeltaSink.forRowData()` fosse deprecata nelle versioni più recenti del connettore.

Possibili soluzioni future:
- Aggiornare il connettore Delta a una versione compatibile con Flink 1.17+
- Valutare `FileSystemLogSink` come alternativa
- Migliorare il monitoraggio della pipeline per isolare il punto critico

---

## Riferimenti

- [iLog — A methodology and platform for high-quality rich personal data](https://arxiv.org/abs/2501.16864)
- [Delta Lake documentation](https://docs.delta.io/latest/index.html)
- [Apache Flink documentation](https://nightlies.apache.org/flink/flink-docs-master/)
- [Apache Kafka documentation](https://kafka.apache.org/documentation/)
- [MinIO documentation](https://min.io/product/overview)

