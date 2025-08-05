package com.luico.quarkus.consumer;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.atomic.AtomicReference;


public class DynamicRateLimiter {
    private final AtomicReference<RateLimiter> limiter;

    public DynamicRateLimiter(double initialRate) {
        this.limiter = new AtomicReference<>(RateLimiter.create(initialRate));
    }

    public void updateRate(double newRate) {
        limiter.set(RateLimiter.create(newRate)); // Reemplaza completamente el objeto
        System.out.println("🎚️ Nuevo rate aplicado: " + newRate + " ops/seg");
    }

    public void acquire() {
        limiter.get().acquire(); // Usa siempre la versión más reciente
    }

    public Boolean tryAcquire() {
        return limiter.get().tryAcquire(); // Usa siempre la versión más reciente
    }
}

