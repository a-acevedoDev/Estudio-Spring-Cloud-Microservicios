package com.aacevedodev.course.springcloud.kafka.productsapi.services;

import com.aacevedodev.course.springcloud.kafka.productsapi.models.Reply;
import com.aacevedodev.course.springcloud.kafka.productsapi.models.dto.ProductDto;

import java.time.Duration;

public interface ProductCommandService {
    Reply<?> sendCreateAndAwait(ProductDto dto, Duration timeout);
}
