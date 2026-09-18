package com.aacevedodev.course.springcloud.kafka.productscommand.handlers;

import com.aacevedodev.course.springcloud.kafka.productscommand.models.Command;
import com.aacevedodev.course.springcloud.kafka.productscommand.models.dto.ProductDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class ProductCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProductCommandConsumer.class);

    @Bean
    public Consumer<Command<ProductDto>> handleCommands(){
        return cmd -> {
            log.info("Comando recibido y consumido con exito: type={}, body={}", cmd.type(), cmd.body());
        };
    }
}
