package com.aacevedodev.course.springcloud.kafka.productsapi.services.impl;

import com.aacevedodev.course.springcloud.kafka.productsapi.models.Command;
import com.aacevedodev.course.springcloud.kafka.productsapi.models.dto.ProductDto;
import com.aacevedodev.course.springcloud.kafka.productsapi.services.ProductCommandService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class IProductCommandService implements ProductCommandService {

    private final StreamBridge bridge;

    public IProductCommandService(StreamBridge bridge) {
        this.bridge = bridge;
    }

    @Override
    public void sendCreate(ProductDto dto) {
        Command<ProductDto> cmd = new Command<>("CREATE", null , dto);
        String correlationId = UUID.randomUUID().toString();
        Message<Command<ProductDto>> msg = MessageBuilder.withPayload(cmd)
                .setHeader("correlationId", correlationId)
                .build();

        boolean sent = this.bridge.send("commands-out-0", msg);

        if (!sent){
            throw new IllegalArgumentException("No se pudo enviar command a Kafka.");
        }
    }
}
