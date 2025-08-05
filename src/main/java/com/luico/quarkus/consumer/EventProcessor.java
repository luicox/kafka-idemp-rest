package com.luico.quarkus.consumer;

import com.google.common.util.concurrent.RateLimiter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class EventProcessor {

    @Inject
    TrackingRepository repository;

    //private final RateLimiter rateLimiter = RateLimiter.create(10.0); // 1 evento por segundo
    AtomicReference<RateLimiter> rateLimiterRef;

    public void process(String eventId, AtomicReference<RateLimiter> rateLimiterRef) {

        try {
            this.callRestService(eventId);
            //System.out.println("Procesado a: " + this.rateLimiterRef.get().getRate()+ " ops/seg por Instance");
        } catch (Exception e) {
            System.err.println("Error processing event " + eventId + ": " + e.getMessage());
            // Puedes hacer retry o enviar a DLQ aquí
        }
    }

    public void processDB(String eventId, AtomicReference<RateLimiter> rateLimiterRef) {
        this.rateLimiterRef = rateLimiterRef;
        if (repository.isAlreadyProcessed(eventId)) {
            System.out.println("Duplicate event ignored: " + eventId);
            return;
        }

        repository.saveAsInProgress(eventId);

        try {
            rateLimiterRef.get().acquire();
            this.callRestService(eventId);
            repository.markAsCompleted(eventId);
        } catch (Exception e) {
            System.err.println("Error processing event " + eventId + ": " + e.getMessage());
            // Puedes hacer retry o enviar a DLQ aquí
        }
    }

    private boolean callRestService(String payload) {
        try {
            Thread.sleep(1000);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
}

