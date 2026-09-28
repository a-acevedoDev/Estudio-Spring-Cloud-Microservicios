package com.aacevedodev.course.springcloud.kafka.productsapi.controllers;

import com.aacevedodev.course.springcloud.kafka.productsapi.models.Reply;
import com.aacevedodev.course.springcloud.kafka.productsapi.models.ReplyStatus;
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
        return getResponseEntity(service.sendCreateAndAwait(dto, Duration.ofSeconds(5)));
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> read(@PathVariable Long id){
        return getResponseEntity(service.sendReadAndAwait(id, Duration.ofSeconds(5)));
    }

    @GetMapping()
    public ResponseEntity<?> readAll(){
        return getResponseEntity(service.sendReadAllAndAwait(Duration.ofSeconds(5)));
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> update(@PathVariable Long id, @Valid @RequestBody ProductDto dto) {
        return getResponseEntity(service.sendUpdateAndAwait(id, dto, Duration.ofSeconds(5)));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable Long id) {
        return getResponseEntity(service.sendDeleteAndAwait(id, Duration.ofSeconds(5)));
    }

    private static @NonNull ResponseEntity<?> getResponseEntity(Reply<?> reply) {
        if (reply.status().isSuccess()) {
            return ResponseEntity.ok(reply.body());
        }
        return ResponseEntity.ok().body(Map.of("Error", reply.message()));
    }

}
