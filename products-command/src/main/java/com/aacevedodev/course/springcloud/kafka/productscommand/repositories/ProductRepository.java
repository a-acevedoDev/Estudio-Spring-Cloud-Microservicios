package com.aacevedodev.course.springcloud.kafka.productscommand.repositories;

import com.aacevedodev.course.springcloud.kafka.productscommand.entities.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {
}
