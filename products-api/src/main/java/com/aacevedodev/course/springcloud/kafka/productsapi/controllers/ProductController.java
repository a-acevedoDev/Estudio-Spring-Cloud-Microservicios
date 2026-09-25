package com.aacevedodev.course.springcloud.kafka.productsapi.controllers;

import com.aacevedodev.course.springcloud.kafka.productsapi.models.Reply;
import com.aacevedodev.course.springcloud.kafka.productsapi.models.dto.ProductDto;
import com.aacevedodev.course.springcloud.kafka.productsapi.services.ProductCommandService;
import jakarta.validation.Valid;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Map;

@RestController
@RequestMapping("/products")
public class ProductController {

    @Autowired
    private ProductCommandService service;

    @PostMapping
    public ResponseEntity<?> create(@Valid @RequestBody ProductDto dto){
        Reply<?> reply = service.sendCreateAndAwait(dto, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> read(@PathVariable Long id){
        Reply<?> reply = service.sendReadAndAwait(id, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    @GetMapping()
    public ResponseEntity<?> readAll(){
        Reply<?> reply = service.sendReadAllAndAwait(Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> update(@PathVariable Long id, @Valid @RequestBody ProductDto dto) {
        Reply<?> reply = service.sendUpdateAndAwait(id, dto, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    private static @NonNull ResponseEntity<?> getResponseEntity(Reply<?> reply) {
        if ("SUCCESS".equalsIgnoreCase(reply.status())) {
            return ResponseEntity.ok(reply.body());
        }
        return ResponseEntity.ok().body(Map.of("Error", reply.message()));
    }

}
