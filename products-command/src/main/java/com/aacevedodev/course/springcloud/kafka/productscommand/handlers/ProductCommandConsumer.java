package com.aacevedodev.course.springcloud.kafka.productscommand.handlers;

import com.aacevedodev.course.springcloud.kafka.productscommand.models.Command;
import com.aacevedodev.course.springcloud.kafka.productscommand.models.Reply;
import com.aacevedodev.course.springcloud.kafka.productscommand.models.dto.ProductDto;
import com.aacevedodev.course.springcloud.kafka.productscommand.services.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
public class ProductCommandConsumer {

    private final ProductService service;

    public ProductCommandConsumer(ProductService service) {
        this.service = service;
    }

    private static final Logger log = LoggerFactory.getLogger(ProductCommandConsumer.class);

    @Bean
    public Function<Message<Command<ProductDto>>, Message<Reply<?>>> handleCommands(){
        return msg -> {
            Command<ProductDto> cmd = msg.getPayload();
            String type = cmd.type() == null ? "" : cmd.type().toUpperCase();
            Reply<ProductDto> reply = null;
            switch (type) {
                case "CREATE" -> {
                    if (cmd.body() == null) {
                        log.warn("Crear cuerpo vacio.");
                        reply = new Reply<>("ERROR", "Cuerpo del producto vacio.", null);
                    }

                    ProductDto productSaved = service.create(cmd.body());
                    log.info("Creando producto name={}, price={}.", productSaved.name(), productSaved.price());
                    reply = new Reply<>("CREATE", "Producto creado satisfactoriamente.", productSaved);
                }
                case "UPDATE" -> {
                    log.info("Modificado producto name=, price= , null, null.");
                }
                case "DELETE" -> {
                    log.info("Eliminado producto id= ,name= .");
                }
                case "READ_ALL" -> {
                    log.info("Productos: ");
                }
                case "READ_ONE" -> {
                    log.info("Producto: ");
                }
                default -> {
                    log.warn("Unknow type={}", type);
                    reply = new Reply<>("UNKNOW", "Tipo no valido", null);
                }
            }
            String correlationId = msg.getHeaders().get("correlationId", String.class);
            log.info("correlationId={}", correlationId);

            MessageBuilder<Reply<?>> out = MessageBuilder.withPayload(reply);

            if (correlationId != null) {
                out.setHeader("correlationId", correlationId);
            }
            return out.build();
        };
    }
}
