package com.buffet.kafka.domain;

import java.time.Instant;

public final class BuffetEvent {
    private final EventType type;
    private final String customerId;
    private final String section;
    private final int quantity;
    private final double amount;
    private final Instant timestamp;

    private BuffetEvent(EventType type, String customerId, String section, int quantity, double amount) {
        this.type = type;
        this.customerId = customerId;
        this.section = section;
        this.quantity = quantity;
        this.amount = amount;
        this.timestamp = Instant.now();
    }

    public static BuffetEvent customerEntered(String customerId) {
        return new BuffetEvent(EventType.CUSTOMER_ENTERED, customerId, null, 0, 0.0);
    }

    public static BuffetEvent platePicked(String customerId, String section, int quantity) {
        return new BuffetEvent(EventType.PLATE_PICKED, customerId, section, quantity, 0.0);
    }

    public static BuffetEvent kitchenRestock(String section, int quantity) {
        return new BuffetEvent(EventType.KITCHEN_RESTOCK, null, section, quantity, 0.0);
    }

    public static BuffetEvent paymentCompleted(String customerId, double amount) {
        return new BuffetEvent(EventType.PAYMENT_COMPLETED, customerId, null, 0, amount);
    }

    public EventType getType() {
        return type;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getSection() {
        return section;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getAmount() {
        return amount;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "BuffetEvent{" +
                "type=" + type +
                ", customerId='" + customerId + '\'' +
                ", section='" + section + '\'' +
                ", quantity=" + quantity +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
