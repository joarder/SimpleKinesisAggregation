package main.java.com.amazonaws;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Generates 1 M records with random contents and use KPL to send to aggregated them into 100 records chunk 
 * and send to Kinesis Streams.
 * Later, use KCL to de-aggregate and read the records
 */
public class SimpleAggregationTest {
	public static Logger log = LoggerFactory.getLogger(SimpleAggregationTest.class);
	public static Random RANDOM;    
	public static BlockingQueue<SimpleRecord> inputQueue;
	
    public static String REGION = null;
	public static String STREAM = null;	
	public static String APP = null;
	public static int SHARD_COUNT = 0;
	public static int PAYLOAD_SIZE = 0;	
	public static int RECORDS = 0;
	
	public static int generatedRecords = 0;
	public static int producedRecords = 0;
	public static int aggregatedRecords = 0;
	public static int deaggregatedRecords = 0;
	public static int consumedRecords = 0;		
	
	public static void main(String[] args) throws Exception {
		// Initialization
		RANDOM = new Random();
		RANDOM.setSeed(0);
		
		// Reading configurations
        Config.readKinesisConfig();
        
        // Create the Kinesis Stream
        SimpleStreamManagement.createStream();
        
        // Initialize the input queue
        inputQueue = new LinkedBlockingQueue<SimpleRecord>(RECORDS);                
        
        // Start record generation
        Thread generator = new Thread(new SimpleRecordGenerator());
        generator.setName("record-generator");
        generator.start();
        
        // Start producer and start streaming records to Kinesis Streams using KPL Aggregation
        Thread producer = new Thread(new SimpleProducer());
        producer.setName("simple-producer");
        producer.start();  
        
        // Sleep 10s before starting the consumer
//        try {
//        	Thread.sleep(120000);
//        } catch(Exception e) {
//        	e.printStackTrace();      
//        }
        
        // Start consumer
        Thread consumer = new Thread(new SimpleConsumer());
        consumer.setName("simple-consumer");
        consumer.start();        
	}
}