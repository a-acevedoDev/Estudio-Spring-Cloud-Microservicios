package com.aacevedodev.course.springcloud.kafka.productsapi.controllers;

import com.aacevedodev.course.springcloud.kafka.productsapi.models.dto.ProductDto;
import com.aacevedodev.course.springcloud.kafka.productsapi.services.ProductCommandService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/products")
public class ProductController {

    @Autowired
    private ProductCommandService service;

    @PostMapping
    public ResponseEntity<?> create(@Valid @RequestBody ProductDto dto){
        service.sendCreate(dto);
        return  ResponseEntity.ok().body(Map.of("message", "Success sent"));
    }
}
