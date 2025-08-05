package com.luico.quarkus.consumer;

import com.luico.model.TechnicalStatus;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import jakarta.inject.Inject;
import jakarta.enterprise.context.ApplicationScoped;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.*;

@ApplicationScoped
//@Startup
public class CoordinatorConsumer {

    //@Inject
    //Logger logger;
    private static final Logger logger = LogManager.getLogger(CoordinatorConsumer.class);


    @ConfigProperty(name = "technicalstatus.topic")
    String technicalstatusTopic;

    @ConfigProperty(name = "input.topic")
    String inputTopic;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @Inject
    EventProcessor eventProcessor;

    @Inject
    ConsumerAdmin consumerAdmin;

    //private  AtomicReference<RateLimiter> rateLimiterRef = new AtomicReference<>(RateLimiter.create(10.0)); // 10 ops/seg
    DynamicRateLimiter dynamicRateLimiter = new DynamicRateLimiter(0.2); // valor inicial


    private final ExecutorService executor = Executors.newFixedThreadPool(700);

    @PostConstruct
    void init() {
        try {
            consumeAndProcessTechnicalStatus();
            logger.info("🔁 Iniciando consumo de mensajes en topic: {}", inputTopic);
            consumeAndProcess();
        } catch (Exception e) {
            logger.error("Error inicializando DeduplicationConsumer", e);
        }
    }

    @PreDestroy
    void cleanup() {

        executor.shutdown();
    }

    private void consumeAndProcess() {
        new Thread(() -> {
            KafkaConsumer<String, String> consumer = consumerAdmin.createConsumer("coordinator-group");
            consumer.subscribe(Collections.singletonList(inputTopic), new RebalanceListener(consumer, dynamicRateLimiter, null, null));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    logger.info("🔁 No hay mensajes nuevos, esperando...");
                    continue;
                } else {
                    logger.info("🔁 Procesando {} mensajes nuevos...", records.count());
                }
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    executor.submit(() -> {
                        try {
                            eventProcessor.process(record.key(), null);
                        } catch (Exception e) {
                            logger.error("❌ Excepción para {}: {}", key, e.getMessage());
                        }
                    });

                }
                dynamicRateLimiter.acquire();
            }
        }).start();
    }

    private void consumeAndProcessTechnicalStatus() {
        new Thread(() -> {
            KafkaConsumer<String, GenericRecord> consumer = consumerAdmin.createConsumerTechnicalStatus("coordinator-technicalstatus-group");
            consumer.subscribe(Collections.singletonList(technicalstatusTopic));

            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(10000));
                if (records.isEmpty()) {
                    logger.info("🔁 Technical status :: No hay mensajes nuevos ...");
                    continue;
                } else {
                    logger.info("🔁 Technical status :: Procesando {} mensajes nuevos...", records.count());
                }
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    String key = record.key();
                    GenericRecord avroRecord = record.value();

                    TechnicalStatus technicalStatus = new TechnicalStatus(
                            avroRecord.get("system").toString(),
                            avroRecord.get("type").toString(),
                            avroRecord.get("status").toString(),
                            avroRecord.get("throughput").toString()
                    );
                    //rateLimiterRef.get().setRate(technicalStatus.throughput() != null ? Double.parseDouble(technicalStatus.throughput()) : 10.0);
                    dynamicRateLimiter.updateRate(Double.parseDouble(technicalStatus.throughput()));
                    logger.info("🔁 Technical status: {} - {} - {} - {}", technicalStatus.system(), technicalStatus.type(), technicalStatus.status(), technicalStatus.throughput());
                }
            }
        }).start();
    }


}