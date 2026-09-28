package com.aacevedodev.course.springcloud.kafka.productsapi.models;

public record Reply<T>(ReplyStatus status, String message, T body ) {
}
