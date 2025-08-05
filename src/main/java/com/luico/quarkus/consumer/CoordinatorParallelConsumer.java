package com.luico.quarkus.consumer;

import com.luico.model.TechnicalStatus;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
@Startup
public class CoordinatorParallelConsumer {

    //@Inject
    //Logger logger;
    private static final Logger logger = LogManager.getLogger(CoordinatorParallelConsumer.class);


    @ConfigProperty(name = "technicalstatus.topic")
    String technicalstatusTopic;

    @ConfigProperty(name = "input.topic")
    String inputTopic;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "parallel.consumer.max.concurrency")
    String parallelMaxConcurrency;

    @Inject
    EventProcessor eventProcessor;

    @Inject
    ConsumerAdmin consumerAdmin;

    //final AtomicReference<RateLimiter> rateLimiterRef = new AtomicReference<>(RateLimiter.create(0.2)); // 10 ops/seg
    DynamicRateLimiter dynamicRateLimiter = new DynamicRateLimiter(0.2); // valor inicial

    private AtomicReference<Double> throughput = new AtomicReference<>(1.0); // 10 ops/seg
    private AtomicReference<Integer> partitions = new AtomicReference<>(1);

    AtomicLong receivedMessages = new AtomicLong();
    AtomicLong processedMessages = new AtomicLong();


    //private final ExecutorService executor = Executors.newFixedThreadPool(500);

    @PostConstruct
    void init() {
        try {
            consumeAndProcessTechnicalStatus();
            logger.info("🔁 Iniciando consumo de mensajes en topic: {}", inputTopic);
            consumeAndProcess();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error inicializando CoordinarParallelConsumer", e);
        }
    }

    @PreDestroy
    void cleanup() {

        //executor.shutdown();
    }

    private void consumeAndProcess() {
        KafkaConsumer<String, String> consumer = consumerAdmin.createConsumer("coordinator-group-3");

        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .maxConcurrency(400)
                .build();

        ParallelStreamProcessor<String, String> pc = ParallelStreamProcessor.createEosStreamProcessor(options);

        // La suscripción debe hacerse aquí:
        pc.subscribe(Collections.singletonList(inputTopic), new RebalanceListener(consumer, dynamicRateLimiter, throughput, partitions));
        logger.info("🧵 Pool de hilos Maximo configurado: {}", options.getMaxConcurrency());

        pc.poll(record -> {
            receivedMessages.incrementAndGet(); // Se recibió desde Kafka
            String key = record.key();
            try {
                while (!dynamicRateLimiter.tryAcquire()) {
                    Thread.sleep(100);
                }
                eventProcessor.process(key, null);
                logger.info("✅  Procesado mensaje con key: {}", key);
                processedMessages.incrementAndGet();

            } catch (Exception e) {
                logger.error("❌ Excepción para {}: {}", key, e.getMessage());
            }
        });

        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        monitor.scheduleAtFixedRate(() -> {
            long inQueue = receivedMessages.get() - processedMessages.get();
            logger.info("📊 Estimación de mensajes en cola (no procesados):{}, recibidos:{} , procesados:{}", inQueue, receivedMessages.get(), processedMessages.get());
        }, 0, 10, TimeUnit.SECONDS);


    }

    private void consumeAndProcessTechnicalStatus() {
        new Thread(() -> {
            KafkaConsumer<String, GenericRecord> consumer = consumerAdmin.createConsumerTechnicalStatus("coordinator-technicalstatus-group" + System.currentTimeMillis());
            consumer.subscribe(Collections.singletonList(technicalstatusTopic));

            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(10000));
                if (records.isEmpty()) {
                    //logger.info("Technical status :: No hay mensajes nuevos ...");
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
                    this.throughput.set(Double.parseDouble(technicalStatus.throughput()));
                    Double currrentRate = (double) ((this.throughput.get() / 6) * partitions.get());


                    //this.rateLimiterRef.set(RateLimiter.create(currrentRate));
                    dynamicRateLimiter.updateRate(currrentRate);
                    logger.info("🔁 Technical status :: Ajustando RateLimiter a: {} ops/seg por Instance", currrentRate);
                    logger.info("🔁 Technical status: {} - {} - {} - {}", technicalStatus.system(), technicalStatus.type(), technicalStatus.status(), technicalStatus.throughput());
                }
            }
        }).start();
    }


}