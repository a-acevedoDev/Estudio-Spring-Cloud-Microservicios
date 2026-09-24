package com.aacevedodev.course.springcloud.kafka.productscommand.services.impl;

import com.aacevedodev.course.springcloud.kafka.productscommand.entities.Product;
import com.aacevedodev.course.springcloud.kafka.productscommand.models.dto.ProductDto;
import com.aacevedodev.course.springcloud.kafka.productscommand.models.mappers.Mappers;
import com.aacevedodev.course.springcloud.kafka.productscommand.repositories.ProductRepository;
import com.aacevedodev.course.springcloud.kafka.productscommand.services.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IProductService implements ProductService {

    @Autowired
    private ProductRepository repository;

    @Override
    @Transactional
    public ProductDto create(ProductDto dto) {
        return Mappers.toDto(repository.save(Mappers.toEntity(dto)));
    }
}
