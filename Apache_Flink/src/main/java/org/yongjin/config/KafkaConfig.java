package org.yongjin.config;

import java.util.Properties;

public class KafkaConfig {

    public static Properties getConsumerProperties(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:29092");
        props.setProperty("group.id", "flink-logger-group");

        return props;
    }
}
