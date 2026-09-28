package com.aacevedodev.course.springcloud.kafka.productsapi.models;

public record Command<T>(CommandType type, Long id, T body) {
}
