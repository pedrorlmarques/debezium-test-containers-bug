package com.example.demodebezium;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Event service responsible for persisting the event in the database.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxService {

    private final OutboxRepository outboxRepository;

    /**
     * This method handles all the events fired by the 'EventPublisher'. The method listens to events
     * and persists them in the database.
     *
     * @param event the event to persist
     */
    @EventListener
    public void handleOutboxEvent(OutboxEvent event) {

        Outbox entity = Outbox.builder()
                .id(UUID.randomUUID())
                .aggregateType(event.getAggregateType())
                .aggregateId(event.getAggregateId())
                .type(event.getType())
                .payload(event.getPayload().toString())
                .build();

        log.info("Handling event : {}", entity);

        entity = this.outboxRepository.save(entity);
        /*
         * Delete the event once written, so that the OUTBOX table doesn't grow immeasurably.
         * The CDC eventing polls the database log entry and not the table in the database.
         */
        this.outboxRepository.delete(entity);
    }
}
