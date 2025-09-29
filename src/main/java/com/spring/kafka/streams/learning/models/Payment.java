package com.spring.kafka.streams.learning.models;

public class Payment {

    private int orderId;
    private double amount;

    public Payment() {
    }

    public Payment(int orderId, double amount) {
        this.orderId = orderId;
        this.amount = amount;
    }
    public int getOrderId() { return orderId; }
    public double getAmount() { return amount; }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Payment{" +
                "orderId='" + orderId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
