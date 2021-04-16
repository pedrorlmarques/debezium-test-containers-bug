package com.example.demodebezium;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class OrderServiceTest2 implements DebeziumContainerTestingSupport {

    public static final String OUTBOX_EVENT_ORDER = "outbox.event.Order";

    @Autowired
    private OrderService orderService;

    @BeforeEach
    public void createKafkaTopic() {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(OUTBOX_EVENT_ORDER, 1, Short.parseShort("1")))).all().get();
        } catch (ExecutionException | InterruptedException e) {
            System.err.println(e.getMessage());
        }
    }

    @AfterEach
    void deleteTopics() {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.deleteTopics(List.of(OUTBOX_EVENT_ORDER)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    void test() {

        this.initialiseOutboxConnector();
        this.orderService.createOrder();

        KafkaConsumer<String, String> consumer = this.createConsumer(UUID.randomUUID().toString(), OUTBOX_EVENT_ORDER);

        var orders = this.drain(consumer, 1, 10);

        assertThat(orders.size()).isEqualTo(1);

        consumer.unsubscribe();

    }
}
