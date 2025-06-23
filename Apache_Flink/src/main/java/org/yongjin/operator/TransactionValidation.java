package org.yongjin.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yongjin.model.PaymentMethod;
import org.yongjin.model.Transaction;

import java.math.BigDecimal;
import java.util.UUID;

public class TransactionValidation extends ProcessFunction<String, Transaction> {
    Logger log;
    ObjectMapper mapper;

    @Override
    public void open(Configuration config){
        if(log == null){
            log = LoggerFactory.getLogger(TransactionValidation.class);
        }
        mapper = new ObjectMapper();
    }

    @Override
    public void processElement(String input, Context context, Collector<Transaction> collector) throws Exception {
        try{
            JsonNode node = mapper.readTree(input);

            Transaction txn = new Transaction();

            String txn_id = node.path("transaction_id").asText(null);
            String timestamp = node.path("timestamp").asText(null);
            BigDecimal amount = BigDecimal.valueOf(node.path("amount").asDouble(-1.0));
            String currency = node.path("currency").asText(null);
            String sender_id = node.path("sender_id").asText(null);
            String country = node.path("country").asText(null);
            String payment_method = node.path("payment_method").asText(null);
            String receiver_id = node.path("receiver_id").asText(null);
            Boolean is_cross_border = node.has("is_cross_border") && node.get("is_cross_border").isBoolean() ? node.get("is_cross_border").asBoolean() : null;
            String destination = node.path("destination_country").asText(null);

            if(txn_id == null) {
                throw new Exception(TransactionValidation.class + " transaction_id not found");
            }
            txn.setTransaction_id(UUID.fromString(txn_id));

            if(timestamp == null){
                throw new Exception(TransactionValidation.class + " timestamp not found");
            }
            txn.setTimestamp(Long.valueOf(timestamp));

            if(amount.compareTo(BigDecimal.ZERO) < 0){
                throw new Exception(TransactionValidation.class + " amount not found");
            }
            txn.setAmount(amount);

            if(currency == null){
                throw new Exception(TransactionValidation.class + " currency not found");
            }
            txn.setCurrency(currency);

            if(sender_id == null){
                throw new Exception(TransactionValidation.class + " sender_id not found");
            }
            txn.setSender_id(sender_id);

            if(country == null){
                throw new Exception(TransactionValidation.class + " country not found");
            }
            txn.setCountry(country);

            if(payment_method == null){
                throw new Exception(TransactionValidation.class + " payment_method not found");
            }
            txn.setPayment_method(payment_method);

            if(receiver_id == null){
                throw new Exception(TransactionValidation.class + " receiver_id not found");
            }
            txn.setReceiver_id(receiver_id);

            if(is_cross_border == null){
                throw new Exception(TransactionValidation.class + " is_cross_border not found");
            }
            txn.setIs_cross_border(is_cross_border);

            if(destination == null){
                throw new Exception(TransactionValidation.class + " destination not found");
            }
            txn.setDestination_country(destination);

            collector.collect(txn);
            log.info(TransactionValidation.class + " Collected " + txn);

        } catch (Exception ex){
            log.info(TransactionValidation.class + " Exception: " + ex.toString());
        }
    }

    @Override
    public void close() throws Exception{
        super.close();
    }

}
