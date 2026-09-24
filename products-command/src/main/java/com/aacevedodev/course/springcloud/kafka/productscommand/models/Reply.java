package com.aacevedodev.course.springcloud.kafka.productscommand.models;

public record Reply<T>(String status, String message, T body) {
}
