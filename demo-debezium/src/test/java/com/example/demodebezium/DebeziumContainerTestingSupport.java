package com.example.demodebezium;

import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Support interface that creates a group of containers to back Debezium CDC process using PostgresSQL.
 * <br><br>
 * <p>It should be used whenever change data capture testing is involved.</p>
 */
public interface DebeziumContainerTestingSupport {

    Logger log = LoggerFactory.getLogger(DebeziumContainerTestingSupport.class);

    Network network = Network.newNetwork();

    DockerImageName imageName = DockerImageName.parse("debezium/postgres:11")
            .asCompatibleSubstituteFor("postgres");

    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.1"))
            .withNetwork(network);

    PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(imageName)
            .withDatabaseName("mannow")
            .withNetwork(network)
            .withNetworkAliases("postgres");

    DebeziumContainer debezium = new DebeziumContainer("debezium/connect:latest")
            .withNetwork(network)
            .withKafka(kafka)
            .dependsOn(kafka, postgres);

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        Startables.deepStart(Stream.of(kafka, postgres, debezium)).join();
        registry.add("spring.cloud.stream.kafka.binder.brokers", kafka::getBootstrapServers);
        //Use the actual production binders instead, and that requires disabling the test binder autoconfiguration
        registry.add("spring.autoconfigure.exclude", () -> "org.springframework.cloud.stream.test.binder" +
                ".TestSupportBinderAutoConfiguration");
        registry.add("spring.datasource.driver-class-name", postgres::getDriverClassName);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    default void initialiseOutboxConnector() {
        String name = "order-outbox-connector";
        debezium.updateOrCreateConnector(name, getConfiguration());
        debezium.ensureConnectorState(name, Connector.State.RUNNING);
    }

    default KafkaConsumer<String, String> createConsumer(String group, String... topicNames) {
        Map<String, Object> consumerProps = new HashMap<>(getConsumerProps(group));
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of(topicNames));
        return consumer;
    }

    private Map<String, String> getConsumerProps(String group) {
        var consumerProps = new HashMap<String, String>();
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization" +
                ".StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization" +
                ".StringDeserializer");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        return consumerProps;
    }

    /**
     * Try to retrieve all the expected records by exhausting the given consumer for the provided amount of time.
     *
     * @param consumer            the consumer to exhaust
     * @param expectedRecordCount the expected record count
     * @param timeout             how long to wait in seconds
     * @return a list of consumer records.
     */
    default List<ConsumerRecord<String, String>> drain(KafkaConsumer<String, String> consumer,
                                                       int expectedRecordCount, int timeout) {
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(timeout, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(500))
                    .iterator()
                    .forEachRemaining(allRecords::add);
            log.info("all records = {}", allRecords);
            return allRecords.size() == expectedRecordCount;
        });

        return allRecords;
    }

    private ConnectorConfiguration getConfiguration() {
        // host, database, user etc. are obtained from the container
        return ConnectorConfiguration.forJdbcContainer(postgres)
                .with("database.server.name", "orders")
                .with("schema.include.list", "public")
                .with("table.include.list", "public.outbox")
                .with("tombstones.on.delete", "false")
                .with("snapshot.mode", "exported") // recommended setting for PostgresSQL
                .with("publication.autocreate.mode", "filtered") // recommended setting when using the pgoutput plug-in
                .with("plugin.name", "pgoutput")
                .with("transforms", "outbox")
                .with("transforms.outbox.type", "io.debezium.transforms.outbox.EventRouter")
                .with("transforms.outbox.table.fields.additional.placement", "type:header:eventType") // e.g. if we
                // wanted we could conditionally consume the message based on the header
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    }
}
