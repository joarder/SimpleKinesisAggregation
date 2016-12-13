package main.java.com.amazonaws;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.Metric;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class SimpleProducer implements Runnable {	
    private KinesisProducer kinesisProducer;
    private KinesisProducerConfiguration cfg;    
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    final AtomicLong completed = new AtomicLong(0);    
    final long outstandingLimit = 5000;
    
    private void init() {
    	cfg = KinesisProducerConfiguration.fromPropertiesFile("./resources/kpl.properties");
        kinesisProducer = new KinesisProducer(cfg);
    }
/*    
    @Override
    public void run() {
    	SimpleAggregationTest.log.info("Starting the KPL producer to aggregate and push "+SimpleAggregationTest.RECORDS+" into <"+SimpleAggregationTest.STREAM+"> ...");
    	
    	init();
    	
        while (!shutdown) {
            try {
                runOnce();
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        stop();
        
        SimpleAggregationTest.log.info("Done pushing "+SimpleAggregationTest.producedRecords+" into <"+SimpleAggregationTest.STREAM+">.");
    }
    
	protected void runOnce() throws Exception {
    	SimpleRecord sampleRecord = SimpleAggregationTest.inputQueue.take();
    	SimpleAggregationTest.aggregatedRecords += 1;
    	
    	String partitionKey = sampleRecord.getSessionId();
        String payload =  sampleRecord.getPayload();
        
        ByteBuffer data = ByteBuffer.wrap(payload.getBytes("UTF-8"));
        
        while (kinesisProducer.getOutstandingRecordsCount() > 1e4) {
            Thread.sleep(1000);
        }
        
        this.sequenceNumber.getAndIncrement();
    	
        // Actually put the record in the stream
        ListenableFuture<UserRecordResult> f = 
        		kinesisProducer.addUserRecord(SimpleAggregationTest.STREAM, partitionKey, data);

        Futures.addCallback(f, new FutureCallback<UserRecordResult>() {
            @Override
            public void onSuccess(UserRecordResult result) {
                long totalTime = result.getAttempts().stream()
                        .mapToLong(a -> a.getDelay() + a.getDuration())
                        .sum();

                // Only log with a small probability, otherwise it'll be very spammy
                //if (SimpleAggregationTest.RANDOM.nextDouble() < 1e-5) {
                    SimpleAggregationTest.log.info(String.format(
                            "Successfully put record, partitionKey=%s, "
                                    + "payload=%s, sequenceNumber=%s, "
                                    + "shardId=%s, took %d attempts, "
                                    + "totalling %s ms",
                            partitionKey, data, result.getSequenceNumber(),
                            result.getShardId(), result.getAttempts().size(),
                            totalTime));
                    
                    SimpleAggregationTest.log.info("Pushed "+SimpleAggregationTest.aggregatedRecords+" records to the Kinesis Streams so far.");
                //}
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof UserRecordFailedException) {
                    UserRecordFailedException e = (UserRecordFailedException) t;
                    UserRecordResult result = e.getResult();

                    String errorList = StringUtils.join(result.getAttempts().stream()
                                        .map(a -> String.format(
                                            "Delay after prev attempt: %d ms, "
                                                    + "Duration: %d ms, Code: %s, "
                                                    + "Message: %s",
                                            a.getDelay(), a.getDuration(),
                                            a.getErrorCode(),
                                            a.getErrorMessage()))
                                        .collect(Collectors.toList()), "\n");

                    SimpleAggregationTest.log.error(String.format(
                            "Record failed to put, partitionKey=%s, "
                                    + "payload=%s, attempts:\n%s", 
                                    partitionKey, 
                                    data, 
                                    errorList));
                }
            }
        });
    }
*/
    @Override
    public void run() {
    	init();
    	SimpleAggregationTest.log.info("Input Queue: "+SimpleAggregationTest.inputQueue.size());
    	
        // Result handler
        final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                if (t instanceof UserRecordFailedException) {
                    Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
                    SimpleAggregationTest.log.error(String.format("Record failed to put - %s : %s", last.getErrorCode(), last.getErrorMessage()));
                }

                SimpleAggregationTest.log.error("Exception during put", t);
                System.exit(1);
            }

            @Override
            public void onSuccess(UserRecordResult result) {
                //SimpleAggregationTest.log.info("Successfully put ["+result.toString()+" ]");
                completed.getAndIncrement();
            }
        };

        // Progress updates
        Thread progress = new Thread(new Runnable() {
            @Override
            public void run() {
            	
                while (true) {
                    long put = sequenceNumber.get();
                    double putPercent = 100.0 * put / SimpleAggregationTest.RECORDS;

                    long done = completed.get();
                    double donePercent = 100.0 * done / SimpleAggregationTest.RECORDS;

                    SimpleAggregationTest.log.info(String.format("Put %d of %d so far (%.2f %%), %d have completed (%.2f %%)",
                            put, SimpleAggregationTest.RECORDS, putPercent, done, donePercent));

                    if (done == SimpleAggregationTest.RECORDS) {
                        break;
                    }

                    // Here we're going to look at the number of user records put over a 5 seconds sliding window.
                    try {
                        for (Metric m : kinesisProducer.getMetrics("UserRecordsPut", 5)) {
                            // Metrics are emitted at different granularity, here we only look at the stream level metric, which has a single dimension of stream name.
                            if (m.getDimensions().size() == 1 && m.getSampleCount() > 0) {
                                SimpleAggregationTest.log.info(String.format(
                                        "(Sliding 5 seconds) Avg put rate: %.2f per sec, success rate: %.2f, failure rate: %.2f, total attempted: %d",
                                        m.getSum() / 5,
                                        m.getSum() / m.getSampleCount() * 100,
                                        (m.getSampleCount() - m.getSum()) / m.getSampleCount() * 100,
                                        (long) m.getSampleCount()));
                            }
                        }
                    } catch (Exception e) {
                        SimpleAggregationTest.log.error("Unexpected error getting metrics", e);
                        System.exit(1);
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        progress.start();

        // Put records
        SimpleAggregationTest.log.info("Initial size of the input queue: "+SimpleAggregationTest.inputQueue.size());
        
        while (true) {
            // We're going to put as fast as we can until we've reached the max number of records outstanding.
            if (sequenceNumber.get() < SimpleAggregationTest.RECORDS) {
                if (kinesisProducer.getOutstandingRecordsCount() < outstandingLimit) {
                	
                	ByteBuffer record = null;
                	String partitionKey = null;
                	String payload = null;
                	
                    try {
                    	if (SimpleAggregationTest.RANDOM.nextDouble() < 1e-5) 
                    		SimpleAggregationTest.log.info("[Randomly checking] Size of the input queue: "+SimpleAggregationTest.inputQueue.size());
                        
                        SimpleRecord simpleRecord = SimpleAggregationTest.inputQueue.take();                    	                    
                    	partitionKey = simpleRecord.getSessionId();
                        payload =  simpleRecord.getPayload();
                        record = ByteBuffer.wrap(payload.getBytes("UTF-8"));
                        
                        sequenceNumber.getAndIncrement();
                        ++SimpleAggregationTest.aggregatedRecords;
                        
                    } catch(Exception e) {
                        e.printStackTrace();
                    }

                    ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(SimpleAggregationTest.STREAM, partitionKey, record);
                    Futures.addCallback(f, callback);

                } else {
                    try {
                        Thread.sleep(1);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } else {
                break;
            }
        }
        
        SimpleAggregationTest.log.info("Final size of the input queue: "+SimpleAggregationTest.inputQueue.size());

        // Wait for remaining records to finish
        while (kinesisProducer.getOutstandingRecordsCount() > 0) {
            kinesisProducer.flush();

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            progress.join();

            for (Metric m : kinesisProducer.getMetrics("UserRecordsPerKinesisRecord")) {
                if (m.getDimensions().containsKey("ShardId")) {
                    SimpleAggregationTest.log.info(String.format(
                            "%.2f user records were aggregated into each Kinesis record on average for shard %s for a total of %d Kinesis records.",
                            m.getMean(),
                            m.getDimensions().get("ShardId"),
                            (long) m.getSampleCount()));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        kinesisProducer.destroy();
        SimpleAggregationTest.log.info("In total pushed "+SimpleAggregationTest.aggregatedRecords+" records.");
        SimpleAggregationTest.log.info("Finished.");
    }
}
