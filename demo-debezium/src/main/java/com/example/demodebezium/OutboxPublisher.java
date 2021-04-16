package com.example.demodebezium;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public class OutboxPublisher implements ApplicationEventPublisherAware {

    private ApplicationEventPublisher eventPublisher;

    @Override
    public void setApplicationEventPublisher(@NonNull ApplicationEventPublisher applicationEventPublisher) {
        this.eventPublisher = applicationEventPublisher;
    }

    /**
     * This method publishes the event on to listeners configured by "@EventListener".
     *
     * @param outboxEvent the event to be published
     */
    public void fire(OutboxEvent outboxEvent) {
        this.eventPublisher.publishEvent(outboxEvent);
    }
}
