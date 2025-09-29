package com.spring.kafka.streams.learning.models;

public class EnrichedOrder {

    private Order order;
    private Customer customer;

    // 🛠️ No-args constructor (needed for deserialization)
    public EnrichedOrder() {
    }

    public EnrichedOrder(Order order, Customer customer) {
        this.order = order;
        this.customer = customer;
    }

    // ✅ Getters and setters
    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    @Override
    public String toString() {
        return "EnrichedOrder{" +
                "order=" + order +
                ", customer=" + customer +
                '}';
    }

}
