package com.aacevedodev.course.springcloud.kafka.productscommand.handlers;

import com.aacevedodev.course.springcloud.kafka.productscommand.models.Command;
import com.aacevedodev.course.springcloud.kafka.productscommand.models.Reply;
import com.aacevedodev.course.springcloud.kafka.productscommand.models.dto.ProductDto;
import com.aacevedodev.course.springcloud.kafka.productscommand.services.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
    public Function<Command<ProductDto>, Reply<?>> handleCommands(){
        return cmd -> {
            String type = cmd.type() == null ? "" : cmd.type().toUpperCase();

            switch (type) {
                case "CREATE" -> {
                    if (cmd.body() == null) {
                        log.warn("Crear cuerpo vacio.");
                        return new Reply<>("ERROR", "Cuerpo del producto vacio.", null);
                    }
                    ProductDto productSaved = service.create(cmd.body());
                    log.info("Creando producto name={}, price={}.", productSaved.name(), productSaved.price());
                    return new Reply<>("CREATE", "Producto creado satisfactoriamente.", productSaved);
                }
//                case "UPDATE" -> {log.info("Modificado producto name=, price= , null, null.");}
//                case "DELETE" -> {log.info("Eliminado producto id= ,name= .");}
//                case "READ_ALL" -> {log.info("Productos: ");}
//                case "READ_ONE" -> {log.info("Producto: ");}
                default -> {
                    log.warn("Unknow type={}", type);
                    return new Reply<>("UNKNOW", "Tipo no valido", null);
                }
            }
        };
    }
}
