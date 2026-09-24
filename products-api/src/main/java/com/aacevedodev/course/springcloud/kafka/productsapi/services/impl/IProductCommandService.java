package com.aacevedodev.course.springcloud.kafka.productsapi.services.impl;

import com.aacevedodev.course.springcloud.kafka.productsapi.messaging.ReplyInbox;
import com.aacevedodev.course.springcloud.kafka.productsapi.models.Command;
import com.aacevedodev.course.springcloud.kafka.productsapi.models.Reply;
import com.aacevedodev.course.springcloud.kafka.productsapi.models.dto.ProductDto;
import com.aacevedodev.course.springcloud.kafka.productsapi.services.ProductCommandService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class IProductCommandService implements ProductCommandService {

    private final StreamBridge bridge;

    private final ReplyInbox replyInbox;

    public IProductCommandService(StreamBridge bridge, ReplyInbox replyInbox) {
        this.bridge = bridge;
        this.replyInbox = replyInbox;
    }

    @Override
    public Reply<?> sendCreateAndAwait(ProductDto dto, Duration timeout) {
        Command<ProductDto> cmd = new Command<>("CREATE", null , dto);
        String correlationId = UUID.randomUUID().toString();
        CompletableFuture<Reply<?>> future = replyInbox.register(correlationId);
        Message<Command<ProductDto>> msg = MessageBuilder.withPayload(cmd)
                .setHeader("correlationId", correlationId)
                .build();

        boolean sent = this.bridge.send("commands-out-0", msg);

        if (!sent){
            throw new IllegalArgumentException("No se pudo enviar command a Kafka.");
        }

        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Timeout esperando respuesta de products-commands",e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
