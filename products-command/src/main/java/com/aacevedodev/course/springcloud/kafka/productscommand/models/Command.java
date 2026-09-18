package com.aacevedodev.course.springcloud.kafka.productscommand.models;

public record Command<T>(String type, Long id, T body) {
}
