package com.aacevedodev.course.springcloud.kafka.productscommand.models.mappers;

import com.aacevedodev.course.springcloud.kafka.productscommand.entities.Product;
import com.aacevedodev.course.springcloud.kafka.productscommand.models.dto.ProductDto;

public final class Mappers {

    private Mappers() {
    }

    static public ProductDto toDto(Product product){
        return new ProductDto(product.getId(), product.getName(), product.getPrice());
    }

    static public Product toEntity(ProductDto dto){
        Product entity = new Product(dto.name(), dto.price());
        entity.setId(dto.id());
        return entity;
    }
}
