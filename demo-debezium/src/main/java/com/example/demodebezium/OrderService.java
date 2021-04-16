package com.example.demodebezium;


import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxPublisher outboxPublisher;

    @Transactional
    public void createOrder() {
        this.outboxPublisher.fire(OrderCreatedEvent
                .of(this.orderRepository.save(new Order(null, UUID.randomUUID().toString()))));
    }

}
