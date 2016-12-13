package main.java.com.amazonaws;

public class SimpleRecordGenerator implements Runnable {
	
	@Override
	public void run() {
		SimpleAggregationTest.log.info("Starting to generate "+SimpleAggregationTest.RECORDS+" records ...");
		
		// Generates the records
		for (int i = 0; i < SimpleAggregationTest.RECORDS; i++) {
            SimpleAggregationTest.inputQueue.offer(Utils.generateRecord());
            ++SimpleAggregationTest.generatedRecords;
        }
        
        try {
			Thread.sleep(1000);			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        SimpleAggregationTest.log.info("Done generating "+SimpleAggregationTest.generatedRecords+" records.");
	}
}
