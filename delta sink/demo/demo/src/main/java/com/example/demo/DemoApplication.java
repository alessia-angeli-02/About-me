package com.example.demo;

import com.example.demo.model.Message;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.types.RowKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;

public class DemoApplication {


    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);

        // type of the message
        RowType messageType = new RowType(List.of(
                new RowType.RowField("id", VarCharType.STRING_TYPE),
                new RowType.RowField("timestamp", new DoubleType()),
                new RowType.RowField("content", VarCharType.STRING_TYPE)
        ));

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-service:9092")
                .setTopics("my-topic")
                .setGroupId("my-group")
                .setProperty("enable.auto.commit", String.valueOf(true)) // default value, there some warning in the logs: debug!!
                .setProperty("auto.commit.interval.ms", String.valueOf(5)) // default value
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();

        // Set up the configuration for MinIO
        org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
        hadoopConfig.set("fs.s3a.endpoint", "http://minio-service:9000");
        hadoopConfig.set("fs.s3a.access.key", "miniouser");
        hadoopConfig.set("fs.s3a.secret.key", "miniopassword");
        hadoopConfig.set("fs.s3a.path.style", "true");

        DeltaSink<RowData> deltaSink = null;

        try {
            deltaSink = DeltaSink
                    .forRowData(
                            new Path("s3a://your-bucket-name/table1"),
                            hadoopConfig,
                            messageType
                    )
                    .build();

            // Se arriva qui senza lanciare eccezioni, la creazione è riuscita
            System.out.println("DeltaSink creato con successo");

            // Puoi aggiungere qui ulteriori test o operazioni con deltaSink
        } catch (Exception e) {
            System.err.println("Errore nella creazione di DeltaSink: " + e.getMessage());
            e.printStackTrace();
        }

        // Generate a manual message
        /*env.fromElements("message1", "12345.67", "content")
                .map(element -> {
                    var row = new GenericRowData(3);

                    // Create message: string, double, string
                    row.setField(0, StringData.fromString("message1"));
                    row.setField(1, 12345.67);
                    row.setField(2, StringData.fromString("content"));

                    row.setRowKind(RowKind.INSERT);

                    // Print the message to the console
                    System.out.println("Generated message: " + row.toString());

                    return (RowData) row;
                })
                .sinkTo(deltaSink);*/

        /*DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new Path("s3a://demo/table1"), // /tmp/delta-table
                        hadoopConfig,
                        messageType
                )
                .build();*/

        env.fromSource(kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "Kafka")
                .map(element -> {
                    var ctn = new ObjectMapper().readValue(element, Message.class);
                    var row = new GenericRowData(3);
                    row.setField(0, StringData.fromString(ctn.getId()));
                    row.setField(1, ctn.getTimestamp());
                    row.setField(2, StringData.fromString(ctn.getContent()));
                    row.setRowKind(RowKind.INSERT);
                    System.out.println("Received message: " + element);
                    return (RowData) row;
                })
                .sinkTo(deltaSink);

        // Execute the job
        env.execute("Kafka to Delta Lake Job on MinIO");

    }
}