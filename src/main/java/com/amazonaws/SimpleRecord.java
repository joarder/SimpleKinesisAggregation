package main.java.com.amazonaws;

public class SimpleRecord {
    private String sessionId;
    private String payload;

    public SimpleRecord(String sessionId, String payload) {
        this.sessionId = sessionId;
        this.payload = payload;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getPayload() {
        return payload;
    }
}
