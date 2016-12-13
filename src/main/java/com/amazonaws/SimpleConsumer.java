package main.java.com.amazonaws;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

public class SimpleConsumer implements Runnable, IRecordProcessor {
	
	private String kinesisShardId;

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;    

    @Override
    public void initialize(InitializationInput initializationInput) {    	
        SimpleAggregationTest.log.info("Initializing record processor for shard: " + initializationInput.getShardId());
        this.kinesisShardId = initializationInput.getShardId();
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
    	SimpleAggregationTest.consumedRecords += processRecordsInput.getRecords().size();
    	
        SimpleAggregationTest.log.info("Size of processRecordsInput:" + processRecordsInput.getRecords().size() + " records from " + kinesisShardId);
        SimpleAggregationTest.log.info("Processed " + SimpleAggregationTest.consumedRecords + " records so far.");
        
        // Process records and perform all exception handling.
        processRecordsWithRetries(processRecordsInput.getRecords());

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(processRecordsInput.getCheckpointer());
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     * 
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records) {
        for (Record record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    //
                    // Logic to process record goes here.
                    //
                    processSingleRecord(record);

                    processedSuccessfully = true;
                    break;
                    
                } catch (Throwable t) {
                    SimpleAggregationTest.log.warn("Caught throwable while processing record " + record, t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    SimpleAggregationTest.log.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                SimpleAggregationTest.log.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    /**
     * Process a single record.
     * 
     * @param record The record to be processed.
     */
    private void processSingleRecord(Record record) {
    	++SimpleAggregationTest.deaggregatedRecords;
    	
        String data = null;
        
        try {
            // For this app, we interpret the payload as UTF-8 chars.
            data = new String(record.getData().array(), "UTF-8");
            
        } catch (NumberFormatException e) {
            SimpleAggregationTest.log.error("Record does not match sample record format. Ignoring record with data; " + data);
            e.printStackTrace();
            
        } catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        SimpleAggregationTest.log.info("Shutting down record processor for shard: " + kinesisShardId);
        
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE)
            checkpoint(shutdownInput.getCheckpointer());
    }

    /** Checkpoint with retries.
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        SimpleAggregationTest.log.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
                
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                SimpleAggregationTest.log.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
                
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    SimpleAggregationTest.log.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                    
                } else {
                    SimpleAggregationTest.log.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
                
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                SimpleAggregationTest.log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                
            } catch (InterruptedException e) {
                SimpleAggregationTest.log.debug("Interrupted sleep", e);
            }
        }
    }
    
    private static AWSCredentialsProvider credentialsProvider;

    private static void init() {
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        credentialsProvider = new ProfileCredentialsProvider();
        
        try {
            credentialsProvider.getCredentials();
            
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }
    }

	@Override
	public void run() {
		init();
		
		String workerId = null;
		try {
			workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
			SimpleAggregationTest.log.debug("@workerId:"+workerId);
			
		} catch (UnknownHostException e) {			
			e.printStackTrace();
		}
		
        KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(SimpleAggregationTest.APP,
                		SimpleAggregationTest.STREAM,
                        credentialsProvider,
                        workerId);        
        config.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        
        IRecordProcessorFactory recordProcessorFactory = new SimpleConsumerFactory();        
        final Worker worker = new Worker.Builder()
        	    .recordProcessorFactory(recordProcessorFactory)
        	    .config(config)
        	    .build();

        System.out.printf("Running %s to process stream %s as worker %s...\n",
        		SimpleAggregationTest.APP,
        		SimpleAggregationTest.STREAM,
                workerId);

        int exitCode = 0;
        try {
        	if(SimpleAggregationTest.consumedRecords < SimpleAggregationTest.RECORDS) {
        		worker.run();
        	} else {
        		SimpleAggregationTest.log.info("In total "+SimpleAggregationTest.deaggregatedRecords+" records are consumed.");
        		worker.requestShutdown();
        		worker.shutdown();
        	}
        	
        } catch (Throwable t) {
        	SimpleAggregationTest.log.error("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
        
        System.exit(exitCode);		
	}
}
