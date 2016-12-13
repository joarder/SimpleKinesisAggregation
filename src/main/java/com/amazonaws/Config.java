package main.java.com.amazonaws;

import java.io.File;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class Config {
    public static void readKinesisConfig() {
        Configurations configs = new Configurations();
        File propertiesFile = new File("kinesis.properties");

        try {
            PropertiesConfiguration config = configs.properties(propertiesFile);
            SimpleAggregationTest.REGION = ((String)config.getProperty("AwsRegion"));
            SimpleAggregationTest.STREAM = ((String)config.getProperty("KinesisStream"));
            SimpleAggregationTest.SHARD_COUNT = (Integer.parseInt((String) config.getProperty("ShardCount")));
            SimpleAggregationTest.PAYLOAD_SIZE = (Integer.parseInt((String) config.getProperty("PayloadSize")));
            SimpleAggregationTest.APP = ((String)config.getProperty("ApplicationName"));
            SimpleAggregationTest.RECORDS = (Integer.parseInt((String) config.getProperty("RecordsCount")));

        } catch(ConfigurationException cex) {
            cex.printStackTrace();
            SimpleAggregationTest.log.error("Can not read Kinesis configuration file!!!");
        }
    }
}
