package com.aacevedodev.course.springcloud.kafka.productscommand.services;

import com.aacevedodev.course.springcloud.kafka.productscommand.models.dto.ProductDto;

import java.util.List;

public interface ProductService {
    ProductDto create(ProductDto dto);
    ProductDto update(Long id, ProductDto dto);
    ProductDto findById(Long id);
    List<ProductDto> findAll();
    boolean delete(Long id);
}
