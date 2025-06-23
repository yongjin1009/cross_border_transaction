package org.yongjin.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yongjin.config.ClickhouseConfig;
import org.yongjin.model.Transaction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;

public class TransactionSink extends RichSinkFunction<Transaction> {
    private transient Connection connection;
    private transient PreparedStatement statement;
    Logger log = LoggerFactory.getLogger(TransactionSink.class);

    @Override
    public void open(Configuration config) throws Exception{
        super.open(config);

        String url = ClickhouseConfig.URL;
        connection = DriverManager.getConnection(url, ClickhouseConfig.USER, ClickhouseConfig.PASSWORD);

        String sql = "INSERT INTO transaction VALUES (?, ?, ?, ?, ?, ?, ?, ? ,?, ?)";
        statement = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(Transaction txn, Context context) throws Exception {
        try{
            statement.setObject(1, txn.getTransaction_id());
            statement.setLong(2, txn.getTimestamp());
            statement.setBigDecimal(3, txn.getAmount());
            statement.setString(4, txn.getCurrency());
            statement.setString(5, txn.getSender_id());
            statement.setString(6, txn.getCountry());
            if(txn.getPayment_method() == null){
                statement.setNull(7, Types.VARCHAR);
            } else {
                statement.setString(7, txn.getPayment_method().name());
            }
            statement.setString(8, txn.getReceiver_id());
            statement.setBoolean(9, txn.getIs_cross_border());
            statement.setString(10, txn.getDestination_country());

            statement.executeUpdate();
        } catch (Exception ex){
            log.info(TransactionSink.class + " Exception " + ex.toString());
        }
    }

    @Override
    public void close() throws Exception {
        if (statement != null) statement.close();
        if (connection != null) connection.close();
    }
}
