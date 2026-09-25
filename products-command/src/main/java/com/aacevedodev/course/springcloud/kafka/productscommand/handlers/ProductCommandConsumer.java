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

import java.util.List;
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
            Reply<?> reply = null;
            switch (type) {
                case "CREATE" -> {
                    if (cmd.body() == null) {
                        log.warn("Crear cuerpo vacio.");
                        reply = new Reply<>("ERROR", "Cuerpo del producto vacio.", null);
                    }

                    ProductDto productSaved = service.create(cmd.body());
                    log.info("Creando producto name={}, price={}.", productSaved.name(), productSaved.price());
                    reply = new Reply<>("SUCCESS", "Producto creado satisfactoriamente.", productSaved);
                }

                case "READ_ONE" -> {
                    if(cmd.id() == null) {
                        log.warn("Id null/vacio.");
                        reply = new Reply<>("Error", "Id nulo o vacio.", null);
                    }
                    ProductDto dto = service.findById(cmd.id());

                    reply = (dto == null)?
                            new Reply<>("Error", "Producto no encontrado", null):
                            new Reply<>("SUCCESS", "products: ", dto);
                    log.info("Buscando producto con id= {}", cmd.id());

                }

                case "READ_ALL" -> {
                    List<ProductDto> dtoList = service.findAll();
                    reply = (dtoList == null)?
                            new Reply<>("Error", "Lista de productos vacia", null):
                            new Reply<>("READ_ALL", "Lista de productos", dtoList);
                }
                case "UPDATE" -> {
                    if (cmd.body() == null || cmd.id() == null) {
                        log.warn("Id y body son requeridos.");
                        reply = new Reply<>("ERROR", "Id y body son requeridos.", null);
                    }

                    ProductDto dto = service.findById(cmd.id());
                    if (dto == null) {
                        new Reply<>("Error", "Producto no encontrado", null);
                    } else {
                        service.update(cmd.id(), dto);
                        new Reply<>("SUCCESS", "Producto modificado", dto);
                        log.info("Porducto modificado, new name= {}, new price= {}", dto.name(), dto.price());
                    }
                }
                case "DELETE" -> {
                    if (cmd.id() == null) {
                        log.warn("Id es requerido.");
                        reply = new Reply<>("ERROR", "Id requerido.", null);
                    }

                    boolean result = service.delete(cmd.id());
                    reply = (result)?
                            new Reply<>("SUCCESS", "Producto eliminado", "deleted"):
                            new Reply<>("Error", "Producto no encontrado", null);
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
