package org.yongjin.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yongjin.config.ClickhouseConfig;
import org.yongjin.model.Anomaly;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class AnomalySink extends RichSinkFunction<Anomaly> {
    private transient Connection connection;
    private transient PreparedStatement statement;
    Logger log = LoggerFactory.getLogger(AnomalySink.class);

    @Override
    public void open(Configuration config) throws Exception{
        super.open(config);

        String url = ClickhouseConfig.URL;
        connection = DriverManager.getConnection(url, ClickhouseConfig.USER, ClickhouseConfig.PASSWORD);

        String sql = "INSERT INTO anomaly (transaction_id, reason, timestamp) VALUES (?, ?, ?)";
        statement = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(Anomaly aml, Context context) throws Exception {
        try{
            statement.setObject(1, aml.getTransaction_id());
            statement.setString(2, aml.getReason().name());
            statement.setLong(3, aml.getTimestamp());

            statement.executeUpdate();
        } catch(Exception ex){
            log.info(AnomalySink.class + " Exception " + ex.toString());
        }
    }

    @Override
    public void close() throws Exception {
        if (statement != null) statement.close();
        if (connection != null) connection.close();
    }
}
