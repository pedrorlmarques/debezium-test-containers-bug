package com.example.demodebezium;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class OrderServiceTest implements DebeziumContainerTestingSupport {

    @Autowired
    private OrderService orderService;

    @Test
    void test() {

        this.initialiseOutboxConnector();
        this.orderService.createOrder();

        var orders = this.drain(this.createConsumer(UUID.randomUUID().toString(),
                "outbox.event.Order"), 1, 10);

        assertThat(orders.size()).isEqualTo(1);

    }
}
