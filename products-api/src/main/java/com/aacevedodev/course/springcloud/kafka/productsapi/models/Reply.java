package com.aacevedodev.course.springcloud.kafka.productsapi.models;

public record Reply<T>(String status, String message, T body ) {
}
