package org.yongjin.model;

import java.math.BigDecimal;
import java.util.UUID;

public class Transaction {
    private UUID transaction_id;
    private long timestamp;
    private BigDecimal amount;
    private String currency;
    private String sender_id;
    private String country;
    private PaymentMethod payment_method;
    private String receiver_id;
    private boolean is_cross_border;
    private String destination_country;

    @Override
    public String toString(){
        return transaction_id +"|"+timestamp+"|"+amount+"|"+currency+"|"+sender_id+"|"+country+"|"+payment_method+"|"+receiver_id+"|"+is_cross_border+"|"+destination_country;
    }

    public UUID getTransaction_id() {
        return transaction_id;
    }

    public void setTransaction_id(UUID transaction_id) {
        this.transaction_id = transaction_id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        if(timestamp > 1_000_000_000_000L){
            this.timestamp = timestamp / 1000;
        } else {
            this.timestamp = timestamp;
        }
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getSender_id() {
        return sender_id;
    }

    public void setSender_id(String sender_id) {
        this.sender_id = sender_id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public PaymentMethod getPayment_method() {
        return payment_method;
    }

    public void setPayment_method(PaymentMethod payment_method) {
        this.payment_method = payment_method;
    }

    public void setPayment_method(String payment_method){
        try{
            this.payment_method = PaymentMethod.valueOf(payment_method);
        } catch(Exception ex){
            this.payment_method = null;
        }
    }

    public String getReceiver_id() {
        return receiver_id;
    }

    public void setReceiver_id(String receiver_id) {
        this.receiver_id = receiver_id;
    }

    public boolean getIs_cross_border() {
        return is_cross_border;
    }

    public void setIs_cross_border(boolean is_cross_border) {
        this.is_cross_border = is_cross_border;
    }

    public String getDestination_country() {
        return destination_country;
    }

    public void setDestination_country(String destination_country) {
        this.destination_country = destination_country;
    }


}


