package com.github.rizvn.pgmq;

import java.time.Instant;
import java.util.Map;

public class PgmqMessage {
    long msgID;
    int readCount;
    Instant enqueuedAt;
    Instant vt;
    String message;
    Map<String, String> headers;

    // getters and setters
    public long getMsgID() { return msgID; }
    public void setMsgID(long msgID) { this.msgID = msgID; }
    public int getReadCount() { return readCount; }
    public void setReadCount(int readCount) { this.readCount = readCount; }
    public Instant getEnqueuedAt() { return enqueuedAt; }
    public void setEnqueuedAt(Instant enqueuedAt) { this.enqueuedAt = enqueuedAt; }
    public Instant getVT() { return vt; }
    public void setVT(Instant vt) { this.vt = vt; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public Map<String, String> getHeaders() { return headers; }
    public void setHeaders(Map<String, String> headers) { this.headers = headers; }
}

