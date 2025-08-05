package com.luico.kconsumer;

// DeduplicationConsumer.java - Con configuración externa y log4j para Confluent Cloud

import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.rocksdb.*;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class DeduplicationConsumer {

    private static final Logger logger = LogManager.getLogger(DeduplicationConsumer.class);

    private static final String CONFIG_FILE = "application-poc.properties";
    private static String dedupTopic;
    private static String inputTopic;
    private static String bootstrapServers;

    private static RocksDB rocksDb;
    private static final String ROCKS_DB_PATH = "/tmp/rocksdb-dedup" + UUID.randomUUID();
    private static final long TTL_HOURS = 48;

    private static final RateLimiter rateLimiter = RateLimiter.create(600.0); // 600 TPS
    private static final ExecutorService executor = Executors.newFixedThreadPool(20);

    private static final MeterRegistry registry = new SimpleMeterRegistry();
    private static final Counter processedCounter = Counter.builder("messages.processed").register(registry);
    private static final Counter duplicatedCounter = Counter.builder("messages.duplicated").register(registry);
    private static final Counter errorCounter = Counter.builder("messages.errors").register(registry);
    private static final Timer restTimer = Timer.builder("rest.latency").register(registry);

    public static void main(String[] args) throws Exception {

        // Cargar configuración desde archivo externo
        Properties config = new Properties();
        config.load(DeduplicationConsumer.class.getClassLoader().getResourceAsStream(CONFIG_FILE));
        dedupTopic = config.getProperty("dedup.topic");
        inputTopic = config.getProperty("input.topic");
        bootstrapServers = config.getProperty("bootstrap.servers");
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);
        rocksDb = RocksDB.open(options, ROCKS_DB_PATH);


        String dedup_consumerGroup = "dedup-loadsync-" + UUID.randomUUID();
        // Logica
        try (KafkaConsumer<String, String> loaderConsumer = createConsumer(dedup_consumerGroup, config)) {
            loadDedupTopicSafely(loaderConsumer);
        }
        print();
        startDedupSyncListener(dedup_consumerGroup, config);
        consumeAndProcess(config);
    }


    private static void loadDedupTopicSafely(KafkaConsumer<String, String> consumer) throws Exception {
        List<TopicPartition> partitions = consumer.partitionsFor(dedupTopic)
                .stream()
                .map(info -> new TopicPartition(dedupTopic, info.partition()))
                .toList();

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        int idleCount = 0;
        int idleThreshold = 10;

        logger.info("🔁 Cargando dedup-topic a RocksDB...");

        while (idleCount < idleThreshold) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            if (records.isEmpty()) {
                logger.info("🔁 Vacio - Esperando datos...");
                idleCount++;
            } else {
                idleCount = 0;
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() == null) {
                        rocksDb.delete(record.key().getBytes());
                    } else {
                        logger.info("🔁 Agregando a RocksDB: key={}, value={}", record.key(), record.value());
                        rocksDb.put(record.key().getBytes(), record.value().getBytes());
                    }
                }
            }
        }

        logger.info("✅ Carga de RocksDB completa.");
    }

    private static void consumeAndProcess(Properties config) {
        KafkaConsumer<String, String> consumer = createConsumer("main-processor", config);
        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        consumer.subscribe(Collections.singletonList(inputTopic));

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

                if (!alreadyProcessed(key)) {
                    executor.submit(() -> {
                        try {
                            rateLimiter.acquire();
                            boolean ok = restTimer.record(() -> callRestService(record.value()));
                            if (ok) {
                                String now = Long.toString(System.currentTimeMillis());
                                rocksDb.put(key.getBytes(), now.getBytes());
                                producer.send(new ProducerRecord<>(dedupTopic, key, now), (metadata, ex) -> {
                                    if (ex != null) {
                                        logger.error("❌ Error al enviar a dedup_topic: {}", ex.getMessage());
                                    } else {
                                        logger.info("📤 Mensaje enviado a dedup_topic: partition={}, offset={}", metadata.partition(), metadata.offset());
                                    }
                                });
                                processedCounter.increment();
                                logger.info("✅ Procesado: {}", key);
                            } else {
                                errorCounter.increment();
                                logger.info("❌ Fallo REST para: {}", key);
                            }
                        } catch (Exception e) {
                            logger.error("❌ Excepción para {}: {}", key, e.getMessage());
                            errorCounter.increment();
                        }
                    });
                } else {
                    logger.info("🔁 Duplicado detectado: {}", key);
                    duplicatedCounter.increment();
                }
            }
        }
    }

    private static boolean alreadyProcessed(String key) {
        try {
            byte[] valueBytes = rocksDb.get(key.getBytes());
            if (valueBytes == null) return false;
            long timestamp = Long.parseLong(new String(valueBytes));
            long ageMs = System.currentTimeMillis() - timestamp;
            return ageMs < Duration.ofHours(TTL_HOURS).toMillis();
        } catch (Exception e) {
            logger.error("❌ Error al verificar RocksDB para {}: {}", key, e.getMessage());
            return false;
        }
    }

    private static boolean callRestService(String payload) {
        try {
            Thread.sleep(1000);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    private static void print() {
        // Imprimir métricas en consola
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            logger.info("================ MÉTRICAS =================");
            logger.info("🔢 Procesados: {}", processedCounter.count());
            logger.info("🔁 Duplicados: {}", duplicatedCounter.count());
            logger.info("❌ Errores: {}", errorCounter.count());
            //logger.info("⏱️ Latencia REST (promedio ms): {}",
            //String.format("%.2f", restTimer.mean(Duration.ofMillis(1).toNanos()))
            //      );
            try (RocksIterator it = rocksDb.newIterator()) {
                long count = 0;
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    count++;
                    logger.info("       📦 RocksDB Entry: key={}, value={}", new String(it.key()), new String(it.value()));
                }
                logger.info("📦 Entradas actuales en RocksDB: {}", count);
            }
            logger.info("==========================================\n");
        }, 10, 30, TimeUnit.SECONDS);
    }

    private static KafkaConsumer<String, String> createConsumer(String groupId, Properties baseConfig) {
        Properties props = new Properties();
        props.putAll(baseConfig);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    private static void startDedupSyncListener(String groupId, Properties config) {
        new Thread(() -> {
            KafkaConsumer<String, String> syncConsumer = createConsumer(groupId, config);
            syncConsumer.subscribe(Collections.singletonList(dedupTopic));
            logger.info("🔄 Sincronizando cambios futuros desde dedup-store...");

            while (true) {
                ConsumerRecords<String, String> records = syncConsumer.poll(Duration.ofMillis(500));
                // logger.info("🔄 cantidad de registros a sincronizar: {}", records.count());
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        if (record.value() == null) {
                            rocksDb.delete(record.key().getBytes());
                        } else {
                            rocksDb.put(record.key().getBytes(), record.value().getBytes());
                        }
                        logger.info("🪪 Sync RocksDB: {} -> {}", record.key(), record.value());
                    } catch (Exception e) {
                        logger.error("❌ Error sincronizando RocksDB: {}", e.getMessage());
                    }
                }
            }
        }).start();
    }


}

