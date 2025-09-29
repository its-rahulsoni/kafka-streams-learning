package com.spring.kafka.streams.learning.models;

public class OrderPayment {

    private int orderId;
    private int customerId;
    private double amount;

    public OrderPayment(int orderId, int customerId, double amount) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "OrderPayment{" +
                "orderId='" + orderId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", amount=" + amount +
                '}';
    }

}
