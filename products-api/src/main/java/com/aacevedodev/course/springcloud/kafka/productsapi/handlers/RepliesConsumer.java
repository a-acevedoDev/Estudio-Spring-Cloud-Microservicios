package com.aacevedodev.course.springcloud.kafka.productsapi.handlers;

import com.aacevedodev.course.springcloud.kafka.productsapi.messaging.ReplyInbox;
import com.aacevedodev.course.springcloud.kafka.productsapi.models.Reply;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

@Configuration
public class RepliesConsumer {
    private final ReplyInbox replyInbox;

    public RepliesConsumer(ReplyInbox replyInbox) {
        this.replyInbox = replyInbox;
    }

    @Bean
    public Consumer<Message<Reply<?>>> handleReplies() {
        return message -> {
            String correlationId = message.getHeaders().get("correlationId", String.class);
            replyInbox.complete(correlationId, message.getPayload());
        };
    }
}
