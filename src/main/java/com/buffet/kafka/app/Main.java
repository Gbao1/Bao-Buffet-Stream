package com.buffet.kafka.app;

import com.buffet.kafka.core.Broker;
import com.buffet.kafka.core.ConsumerGroup;
import com.buffet.kafka.core.Producer;
import com.buffet.kafka.domain.BuffetEvent;
import com.buffet.kafka.domain.EventType;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        Producer producer = new Producer(broker);

        String topic = "buffet-events";
        broker.createTopic(topic, 3);

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
