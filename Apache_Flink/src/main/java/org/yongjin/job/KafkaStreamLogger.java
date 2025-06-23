package org.yongjin.job;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yongjin.config.KafkaConfig;

import java.util.Properties;

public class KafkaStreamLogger {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamLogger.class);

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = KafkaConfig.getConsumerProperties();

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "transactions",
                new SimpleStringSchema(),
                props
        );

        // Read from Kafka and print to stdout
        see.addSource(consumer)
                .name("Kafka Source")
                .uid("kafka-source")
                .map(value -> {
                    log.info("Received: " + value);
                    return value;
                });

        see.execute(KafkaStreamLogger.class.toString());
    }
}
