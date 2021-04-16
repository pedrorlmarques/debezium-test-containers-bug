package com.example.demodebezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.ToString;

/**
 * POJO for holding the Order created event to be published.
 */
@ToString
public final class OrderCreatedEvent implements OutboxEvent {

    private static final ObjectMapper mapper = new ObjectMapper();

    private final long id;
    private final JsonNode order;

    private OrderCreatedEvent(long id, JsonNode order) {
        this.id = id;
        this.order = order;
    }

    public static OrderCreatedEvent of(Order order) {

        var asJson = mapper.createObjectNode()
                .put("id", order.getId())
                .put("name", order.getName());

        return new OrderCreatedEvent(order.getId(), asJson);
    }

    @Override
    public String getAggregateId() {
        return String.valueOf(id);
    }

    @Override
    public String getAggregateType() {
        return "Order";
    }

    @Override
    public String getType() {
        return "OrderCreated";
    }

    @Override
    public JsonNode getPayload() {
        return order;
    }
}
