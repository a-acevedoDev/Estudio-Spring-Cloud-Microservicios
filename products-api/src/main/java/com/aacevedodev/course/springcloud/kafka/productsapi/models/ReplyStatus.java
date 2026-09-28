package com.aacevedodev.course.springcloud.kafka.productsapi.models;

public enum ReplyStatus {
    SUCCESS,
    ERROR;


    public boolean isSuccess(){
        return this == SUCCESS;
    }
}
