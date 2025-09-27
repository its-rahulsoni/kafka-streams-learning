package com.spring.kafka.streams.learning.models;

public class Order {
    private int order_id;
    private int customer_id;
    private String order_date;
    private String total_amount;

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public int getCustomer_id() {
        return customer_id;
    }

    public void setCustomer_id(int customer_id) {
        this.customer_id = customer_id;
    }

    public String getOrder_date() {
        return order_date;
    }

    public void setOrder_date(String order_date) {
        this.order_date = order_date;
    }

    public String getTotal_amount() {
        return total_amount;
    }

    public void setTotal_amount(String total_amount) {
        this.total_amount = total_amount;
    }

    @Override
    public String toString() {
        return "Order{" +
                "order_id=" + order_id +
                ", customer_id=" + customer_id +
                ", order_date='" + order_date + '\'' +
                ", total_amount=" + total_amount +
                '}';
    }
}
