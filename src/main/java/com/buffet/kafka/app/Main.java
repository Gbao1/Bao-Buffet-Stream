package com.buffet.kafka.app;

import com.buffet.kafka.core.Broker;
import com.buffet.kafka.core.ConsumerGroup;
import com.buffet.kafka.core.Producer;
import com.buffet.kafka.domain.BuffetEvent;
import com.buffet.kafka.domain.EventType;
import com.buffet.kafka.protocol.ApiKey;
import com.buffet.kafka.protocol.ProtocolGateway;
import com.buffet.kafka.protocol.Request;
import com.buffet.kafka.protocol.RequestHeader;
import com.buffet.kafka.protocol.Response;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        Producer producer = new Producer(broker);

        String topic = "buffet-events";
        broker.createTopic(topic, 3);

        ProtocolGateway protocolGateway = new ProtocolGateway(broker);

        Request versionsRequest = new Request(new RequestHeader(
            (short) ApiKey.API_VERSIONS.id(),
            (short) 4,
            1001,
            42
        ));
        Response versionsResponse = protocolGateway.handle(versionsRequest.serialize());
        System.out.println("[PROTOCOL] " + versionsResponse);

        Request partitionsRequest = new Request(new RequestHeader(
            (short) ApiKey.DESCRIBE_TOPIC_PARTITIONS.id(),
            (short) 0,
            1002,
            42
        ));
        Response partitionsResponse = protocolGateway.handle(partitionsRequest.serialize());
        System.out.println("[PROTOCOL] " + partitionsResponse);

        ConsumerGroup<BuffetEvent> kitchenGroup = new ConsumerGroup<>(
                "kitchen-group",
                broker,
                topic,
                message -> {
                    BuffetEvent event = message.getPayload();
                    if (event.getType() == EventType.PLATE_PICKED && event.getQuantity() >= 3) {
                        System.out.printf("[KITCHEN] High demand in %s, quantity=%d%n",
                                event.getSection(), event.getQuantity());
                    }
                    if (event.getType() == EventType.KITCHEN_RESTOCK) {
                        System.out.printf("[KITCHEN] Restocked %s by %d%n",
                                event.getSection(), event.getQuantity());
                    }
                },
                10,
                200
        );

        ConsumerGroup<BuffetEvent> billingGroup = new ConsumerGroup<>(
                "billing-group",
                broker,
                topic,
                message -> {
                    BuffetEvent event = message.getPayload();
                    if (event.getType() == EventType.PAYMENT_COMPLETED) {
                        System.out.printf("[BILLING] Customer %s paid %.2f%n",
                                event.getCustomerId(), event.getAmount());
                    }
                },
                10,
                200
        );

        kitchenGroup.start();
        billingGroup.start();

        producer.send(topic, "CUST-1001", BuffetEvent.customerEntered("CUST-1001"));
        producer.send(topic, "CUST-1001", BuffetEvent.platePicked("CUST-1001", "Sushi", 2));
        producer.send(topic, "CUST-1002", BuffetEvent.customerEntered("CUST-1002"));
        producer.send(topic, "CUST-1002", BuffetEvent.platePicked("CUST-1002", "Sushi", 4));
        producer.send(topic, "KITCHEN", BuffetEvent.kitchenRestock("Sushi", 20));
        producer.send(topic, "CUST-1001", BuffetEvent.paymentCompleted("CUST-1001", 39.90));
        producer.send(topic, "CUST-1002", BuffetEvent.paymentCompleted("CUST-1002", 45.50));

        Thread.sleep(2000);

        kitchenGroup.stop();
        billingGroup.stop();

        System.out.println("Simulation completed.");
    }
}
