package main.java.com.amazonaws;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;

public class SimpleStreamManagement {
	public static AmazonKinesisClient kinesisClient;
	
	private static void init() throws Exception {
        AWSCredentials credentials = null;
        
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
            
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/Users/joarderk/.aws/credentials), and is in valid format.",
                    e);
        }

        kinesisClient = new AmazonKinesisClient(credentials);
    }
	
    private static void waitForStreamToBecomeAvailable(String streamName) throws InterruptedException {
        SimpleAggregationTest.log.info("Waiting for <"+streamName+"> to become ACTIVE ...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(streamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                SimpleAggregationTest.log.info("\t- current state: "+streamStatus);
                
                if ("ACTIVE".equals(streamStatus))
                    return;

            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }

        throw new RuntimeException(String.format("Stream <"+streamName+"> never became active!!!"));
    }
    
    public static void createStream() throws Exception {
    	SimpleAggregationTest.log.info("Checking the status of <"+SimpleAggregationTest.STREAM+"> ...");
    	
        init();

        // Describe the stream and check if it exists
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(SimpleAggregationTest.STREAM);
        
        try {
            StreamDescription streamDescription = kinesisClient.describeStream(describeStreamRequest).getStreamDescription();
            SimpleAggregationTest.log.info("Stream <"+SimpleAggregationTest.STREAM+"> is in "+streamDescription.getStreamStatus()+" status.");

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
            	SimpleAggregationTest.log.info("Stream is being deleted. Exiting ...");
                System.exit(0);
            }

            // Wait for the stream to become active if it is not yet ACTIVE
            if (!"ACTIVE".equals(streamDescription.getStreamStatus()))
                waitForStreamToBecomeAvailable(SimpleAggregationTest.STREAM);
            
        } catch (ResourceNotFoundException ex) {
        	SimpleAggregationTest.log.info("Stream <"+SimpleAggregationTest.STREAM+"> does not exist. Creating it now ...");

            // Create a stream. The number of shards determines the provisioned throughput
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(SimpleAggregationTest.STREAM);
            createStreamRequest.setShardCount(SimpleAggregationTest.SHARD_COUNT);
            kinesisClient.createStream(createStreamRequest);

            // The stream is now being created. Wait for it to become active.
            waitForStreamToBecomeAvailable(SimpleAggregationTest.STREAM);
        }

        // List all of my streams
        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(10);
        ListStreamsResult listStreamsResult = kinesisClient.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();
        
        while (listStreamsResult.isHasMoreStreams()) {
            if (streamNames.size() > 0)
                listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));

            listStreamsResult = kinesisClient.listStreams(listStreamsRequest);
            streamNames.addAll(listStreamsResult.getStreamNames());
        }
        
        // Print all of my streams
        SimpleAggregationTest.log.info("List of my streams: ");
        for (int i = 0; i < streamNames.size(); i++)
            SimpleAggregationTest.log.info("\t- " + streamNames.get(i));
    }
}
