package com.aacevedodev.course.springcloud.kafka.productscommand.services;

import com.aacevedodev.course.springcloud.kafka.productscommand.models.dto.ProductDto;

public interface ProductService {
    ProductDto create(ProductDto dto);

}
