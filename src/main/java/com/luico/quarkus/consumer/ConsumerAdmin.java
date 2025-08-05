package com.luico.quarkus.consumer;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Properties;

@ApplicationScoped
public class ConsumerAdmin {

    private static final Logger logger = LogManager.getLogger(ConsumerAdmin.class);

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;


    @ConfigProperty(name = "kafka.sasl.jaas.config")
    String jaasConfig;

    @ConfigProperty(name = "schema.registry.url")
    String registryUrl;

    @ConfigProperty(name = "basic.auth.user.info")
    String userInfo;


    public KafkaConsumer<String, String> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1048576");// 1MB máximo por poll
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000"); // 5 minutos
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", jaasConfig);
        props.put("ssl.endpoint.identification.algorithm", "https");

        // propiedades adicionales para parallel consumer
        //props.put("auto.commit.interval.ms", "1000");
        props.put("enable.auto.commit", "false");
        // Agrega aquí más propiedades si es necesario (seguridad, etc)
        return new KafkaConsumer<>(props);
    }

    public KafkaConsumer<String, GenericRecord> createConsumerTechnicalStatus(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", jaasConfig);
        props.put("ssl.endpoint.identification.algorithm", "https");


        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", registryUrl);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", userInfo);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        // Agrega aquí más propiedades si es necesario (seguridad, etc)
        return new KafkaConsumer<>(props);
    }


}