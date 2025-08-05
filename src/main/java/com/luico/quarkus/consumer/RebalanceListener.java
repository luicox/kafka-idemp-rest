package com.luico.quarkus.consumer;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RebalanceListener implements ConsumerRebalanceListener {

    private final Consumer<String, String> consumer;
    AtomicReference<Double> throughput;
    AtomicReference<Integer> partitions;
    //AtomicReference<RateLimiter> rateLimiterRef;

    //private final AtomicLong throughput;
    //private final AtomicInteger partitions;
    private final DynamicRateLimiter rateLimiter;


    public RebalanceListener(Consumer<String, String> consumer, DynamicRateLimiter rateLimiter, AtomicReference<Double> throughput, AtomicReference<Integer> partitions) {
        this.rateLimiter = rateLimiter;
        this.consumer = consumer;
        this.throughput = throughput;
        this.partitions = partitions;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("[🛑 REBALANCE] Partitions revoked: " + partitions);

        try {
//            // 1. Obtener los offsets procesados desde la tabla de tracking
//            Map<TopicPartition, Long> lastProcessedOffsets = trackingService.getLastProcessedOffsets(partitions);
//
//            // 2. Commit manual de los offsets ANTES de perder las particiones
//            lastProcessedOffsets.forEach((tp, offset) -> {
//                consumer.commitSync(Map.of(tp, new org.apache.kafka.clients.consumer.OffsetAndMetadata(offset + 1)));
//                System.out.println("[✅ COMMIT] Offset " + (offset + 1) + " for partition " + tp);
//            });

        } catch (Exception e) {
            System.err.println("[❌ ERROR] While committing offsets on rebalance: " + e.getMessage());
            // Decide si quieres lanzar o seguir
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("[📦 REBALANCE] Partitions assigned: " + partitions);
        System.out.println("🧵 Hilos activos: " + Thread.activeCount());
        this.partitions.set(partitions.size());
        Double currrentRate = (double) ((this.throughput.get() / 6) * partitions.size());
        //this.rateLimiterRef.set(RateLimiter.create(currrentRate)); // Ajusta el rate limiter según el número de hilos activos
        rateLimiter.updateRate(currrentRate);
        System.out.println("🔄 Ajustando RateLimiter a: " + currrentRate + " ops/seg por Instance");

        // Opcional: puedes buscar desde dónde empezar a consumir según tu tracking
        // Por default, Kafka lo hace desde el último offset committeado
    }
}
