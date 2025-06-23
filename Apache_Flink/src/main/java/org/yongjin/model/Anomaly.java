package org.yongjin.model;

import java.util.UUID;

public class Anomaly {
    private UUID transaction_id;
    private AnomalyType reason;
    private long timestamp;

    @Override
    public String toString(){
        return transaction_id+"|"+reason+"|"+timestamp;
    }

    public UUID getTransaction_id() {
        return transaction_id;
    }

    public void setTransaction_id(UUID transaction_id) {
        this.transaction_id = transaction_id;
    }

    public AnomalyType getReason() {
        return reason;
    }

    public void setReason(AnomalyType reason) {
        this.reason = reason;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
