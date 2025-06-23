package org.yongjin.job;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.yongjin.config.KafkaConfig;
import org.yongjin.model.Transaction;
import org.yongjin.operator.AnomalyDetection;
import org.yongjin.operator.TransactionValidation;
import org.yongjin.sink.AnomalySink;
import org.yongjin.sink.TransactionSink;

import java.util.Properties;

public class TransactionProcessing {
    public static void main (String[] args) throws Exception{
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = KafkaConfig.getConsumerProperties();

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "transactions",
                new SimpleStringSchema(),
                props
        );

        DataStream<String> stream = see.addSource(consumer);

        DataStream<Transaction> transactions = stream
                .process(new TransactionValidation());


        transactions
                .addSink(new TransactionSink());


        transactions
                .keyBy(transaction -> transaction.getSender_id())
                .process(new AnomalyDetection())
                .addSink(new AnomalySink());

        see.execute(TransactionProcessing.class.toString());
    }
}
