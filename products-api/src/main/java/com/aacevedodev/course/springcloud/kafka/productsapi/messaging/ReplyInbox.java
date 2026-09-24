package com.aacevedodev.course.springcloud.kafka.productsapi.messaging;

import com.aacevedodev.course.springcloud.kafka.productsapi.models.Reply;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ReplyInbox {
    private final ConcurrentHashMap<String, CompletableFuture<Reply<?>>> pending = new ConcurrentHashMap<>();

    public CompletableFuture<Reply<?>> register(String correlationId) {
        CompletableFuture<Reply<?>> future = new CompletableFuture<>();
        pending.put(correlationId, future);
        return future;
    }

    public void complete(String correlationId, Reply<?> reply) {
        CompletableFuture<Reply<?>> future = pending.remove(correlationId);

        if (correlationId == null) {
            throw new NullPointerException("correlationId no puede ser null.");
        }

        if (future != null) {
            future.complete(reply);
        }
    }
}
