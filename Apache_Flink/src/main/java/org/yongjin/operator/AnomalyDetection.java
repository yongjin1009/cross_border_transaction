package org.yongjin.operator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yongjin.model.Anomaly;
import org.yongjin.model.AnomalyType;
import org.yongjin.model.Transaction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.HashMap;

public class AnomalyDetection extends KeyedProcessFunction<String, Transaction, Anomaly> {
    Logger log;
    HashMap<String, String> countryCurrencyMapping;
    ValueState<String> state;

    @Override
    public void open(Configuration config){
        if(log == null){
            log = LoggerFactory.getLogger(AnomalyDetection.class);
        }

        if(state == null){
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("AnomalyState", String.class));
        }

        HttpResponse<String> resp = null;
        try{
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://country.io/currency.json"))
                    .build();
            resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            countryCurrencyMapping = new ObjectMapper().readValue(resp.body(), new TypeReference<>() {});
            System.out.println("Loaded " + countryCurrencyMapping.size() + " entries");
        } catch (Exception ex){
            log.info(AnomalyDetection.class + " Exception when loading currency mapping " + resp.statusCode());

            //Load default set;
            countryCurrencyMapping = new HashMap<>();
            countryCurrencyMapping.put("US", "USD");
            countryCurrencyMapping.put("CN", "CND");
            countryCurrencyMapping.put("MY", "MYR");
            countryCurrencyMapping.put("SG", "SGD");
        }
    }

    @Override
    public void processElement(Transaction txn, Context context, Collector<Anomaly> collector) throws Exception {
        try {
            // Flag if payment method is not valid / null
            if(txn.getPayment_method() == null){
                Anomaly aml = new Anomaly();
                aml.setTransaction_id(txn.getTransaction_id());
                aml.setReason(AnomalyType.INVALID_PAYMENT_METHOD);
                aml.setTimestamp(txn.getTimestamp());

                collector.collect(aml);
                log.info(AnomalyDetection.class + " Collected " + aml);
            }

            // Flag if is_cross_border transaction but not using destination currency
            if(txn.getIs_cross_border()){
                String destination_currency = countryCurrencyMapping.get(txn.getDestination_country());
                if(!destination_currency.equals(txn.getCurrency()) ){
                    Anomaly aml = new Anomaly();
                    aml.setTransaction_id(txn.getTransaction_id());
                    aml.setReason(AnomalyType.THIRD_PARTY_CURRENCY);
                    aml.setTimestamp(txn.getTimestamp());

                    collector.collect(aml);
                    log.info(AnomalyDetection.class + " Collected " + aml);
                }
            }

            // Flag if > 3 transaction from same sender id within 5 seconds
            String cache = state.value();
            if(cache == null){
                state.update("1_"+txn.getTimestamp());
            } else {
                int cache_num = Integer.parseInt(cache.split("_")[0]);
                long cache_ts = Long.parseLong(cache.split("_")[1]);

                if( (txn.getTimestamp() - cache_ts) < 5 ){
                    if(cache_num == 2){
                        Anomaly aml = new Anomaly();
                        aml.setTransaction_id(txn.getTransaction_id());
                        aml.setReason(AnomalyType.HIGH_VELOCITY);
                        aml.setTimestamp(txn.getTimestamp());

                        collector.collect(aml);
                        log.info(AnomalyDetection.class + " Collected " + aml);

                        state.clear();
                    } else {
                        cache_num += 1;
                        state.update(cache_num + "_" + cache_ts);
                    }
                } else {
                    state.update("1_"+txn.getTimestamp());
                }
            }

        } catch (Exception ex){
            log.info(AnomalyDetection.class + " Exception " + ex.toString());
        }
    }

    @Override
    public void close() throws Exception{
        super.close();
    }
}
