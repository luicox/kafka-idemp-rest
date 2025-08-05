package com.luico.producer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.util.Properties;

import java.io.FileInputStream;
import java.io.IOException;

public class ExactlyOnceProducer {

    public static void main(String[] args) throws IOException {
        Properties appProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/application-poc.properties")) {
            appProps.load(fis);
        } catch (IOException e) {
            System.err.println("No se pudo cargar application-poc.properties");
            e.printStackTrace();
            return;
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appProps.getProperty("kafka.bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, appProps.getProperty("kafka.key.serializer", StringSerializer.class.getName()));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id-1");

        // Configuración de seguridad
        props.put("security.protocol", appProps.getProperty("kafka.security.protocol"));
        props.put("sasl.mechanism", appProps.getProperty("kafka.sasl.mechanism"));
        props.put("sasl.jaas.config", appProps.getProperty("kafka.sasl.jaas.config"));
        props.put("ssl.endpoint.identification.algorithm", appProps.getProperty("kafka.ssl.endpoint.identification.algorithm"));

        props.put("schema.registry.url", appProps.getProperty("schema.registry.url"));
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", appProps.getProperty("basic.auth.user.info"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topic = appProps.getProperty("input.topic");

        Schema schema = new Schema.Parser().parse(new File("src/main/resources/model/movement.avsc"));


        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

        producer.initTransactions();

        try {
            producer.beginTransaction();
            for (int i = 1; i <= 1000; i++) {
                for (int j = 100; j <= 100; j++) {

                    GenericRecord record = new GenericData.Record(schema);
                    record.put("accountNumber", ""+i);
                    record.put("finalAccountStatus", "ACTIVO");
                    record.put("movementAmount", j+".00");
                    record.put("finalBalance", "1250.75");
                    record.put("movementTime", "14:35:22");
                    record.put("movementDate", "20250804");
                    record.put("termId", "ATM123");
                    record.put("tbr", "XYZ123");
                    record.put("transactionCode", "DEP");
                    record.put("operationUser", "usuario001");
                    record.put("flagNextDay", "0");
                    record.put("valutaDate", "04082025");
                    record.put("operationReference", "Pago de servicios");
                    record.put("agencyBranchCode", "AG001");
                    record.put("operationNumber", i+"-"+j);
                    record.put("extorno", "false");
                    record.put("operationType", "CR");
                    record.put("trxOrigin", "ORIGEN-XYZ");
                    record.put("productCode", "PROD001");
                    record.put("correlationId", "abc-123-def-456");
                    record.put("channel", "WEB");
                    record.put("requestHeader", "HEADER_ENCRYPTED_BASE64");
                    record.put("requestBody", "BODY_ENCRYPTED_BASE64");


                    producer.send(new ProducerRecord<>(topic, ""+i, record));
                }
            }
            producer.commitTransaction();
            System.out.println("10,000 mensajes enviados exactamente una vez.");
        } catch (ProducerFencedException e) {
            producer.close();
        } catch (Exception e) {
            producer.abortTransaction();
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}