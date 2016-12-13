package main.java.com.amazonaws;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang3.StringUtils;

public class Utils {
    public static String randomExplicitHashKey() {
        return new BigInteger(128, SimpleAggregationTest.RANDOM).toString(10);
    }
	
	public static ByteBuffer generateRecord(long sequenceNumber, int totalLen) {
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toString(sequenceNumber));
        sb.append(" ");

        while (sb.length() < totalLen) {
            sb.append("a");
        }

        try {
            return ByteBuffer.wrap(sb.toString().getBytes("UTF-8"));

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
	
	public static SimpleRecord generateRecord() {
        byte[] id = new byte[13];        
        SimpleAggregationTest.RANDOM.nextBytes(id);
        
        String payload = StringUtils.repeat("a", 350);
        //String payload = createPayload(SimpleAggregationTest.PAYLOAD_SIZE);
        
        return new SimpleRecord(DatatypeConverter.printBase64Binary(id), payload);
	}
	
	// Creates and returns a payload of size @size in KB	
	public static String createPayload(int size) {		
		StringBuilder sb = new StringBuilder(size);
		
		for (int i = 0; i < size*1024; i++)
	        sb.append((char)(SimpleAggregationTest.RANDOM.nextInt(26) + 'a'));
		
		return sb.toString();
	}
}
