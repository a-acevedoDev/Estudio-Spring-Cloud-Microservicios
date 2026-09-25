package com.aacevedodev.course.springcloud.kafka.productscommand.services.impl;

import com.aacevedodev.course.springcloud.kafka.productscommand.entities.Product;
import com.aacevedodev.course.springcloud.kafka.productscommand.models.dto.ProductDto;
import com.aacevedodev.course.springcloud.kafka.productscommand.models.mappers.Mappers;
import com.aacevedodev.course.springcloud.kafka.productscommand.repositories.ProductRepository;
import com.aacevedodev.course.springcloud.kafka.productscommand.services.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class IProductService implements ProductService {

    @Autowired
    private ProductRepository repository;

    @Override
    @Transactional
    public ProductDto create(ProductDto dto) {
        return Mappers.toDto(repository.save(Mappers.toEntity(dto)));
    }

    @Override
    @Transactional
    public ProductDto update(Long id, ProductDto dto) {
        Product entity = repository.findById(id).orElseThrow(() -> new RuntimeException("Producto no encontrado."));

        if (entity == null){
            return null;
        }

        entity.setName(dto.name());
        entity.setPrice(dto.price());

        return Mappers.toDto(repository.save(entity));
    }

    @Override
    @Transactional(readOnly = true)
    public ProductDto findById(Long id) {
        return repository.findById(id).map(Mappers::toDto).orElseThrow(() -> new RuntimeException("Error, producto no encontrado con id: " + id));
    }

    @Override
    @Transactional(readOnly = true)
    public List<ProductDto> findAll() {
        return repository.findAll().stream().map(Mappers::toDto).collect(Collectors.toList());
    }

    @Override
    @Transactional
    public boolean delete(Long id) {
        boolean result = repository.existsById(id);
        if (result){
            repository.deleteById(id);
            return true;
        }
        return false;
    }
}
