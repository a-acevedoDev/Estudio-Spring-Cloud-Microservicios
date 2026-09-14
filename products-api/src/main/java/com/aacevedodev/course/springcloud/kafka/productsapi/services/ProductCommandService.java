package com.aacevedodev.course.springcloud.kafka.productsapi.services;

import com.aacevedodev.course.springcloud.kafka.productsapi.models.dto.ProductDto;

public interface ProductCommandService {
    void sendCreate(ProductDto dto);
}
