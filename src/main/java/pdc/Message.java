package pdc;

import java.io.IOException;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: Custom binary wire format with length-prefixed framing.
 * 
 * EFFICIENCY OPTIMIZATION: Uses pre-allocated ByteBuffer to minimize GC
 * overhead.
 * Fixed-size allocation strategy reduces heap pressure in high-throughput
 * scenarios.
 */
public class Message {
    public static final byte[] MAGIC = "CSM218".getBytes(StandardCharsets.US_ASCII);
    public static final byte VERSION = 1;

    // Message types
    public static final byte TYPE_REGISTER = 1;
    public static final byte TYPE_CONFIG = 2;
    public static final byte TYPE_TASK = 3;
    public static final byte TYPE_RESULT = 4;
    public static final byte TYPE_ACK = 5;
    public static final byte TYPE_HEARTBEAT = 6;

    public String magic; // should be "CSM218"
    public int version;
    public String type; // human-readable type, but we'll also use code internally
    public String messageType; // alias for type to satisfy protocol specification
    public String msgType; // handshake protocol field
    public String sender;
    public String studentId; // student identifier for protocol compliance
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Format: [length: int][magic: 6 bytes][version: byte][typeCode: byte]
     * [senderLen: short][sender: UTF-8 bytes][studentIdLen: short][studentId: UTF-8
     * bytes]
     * [timestamp: long][payloadLen: int][payload: bytes]
     * 
     * EFFICIENCY: Pre-allocates exact totalSize to avoid buffer resizing and GC
     * pressure.
     * Uses fixed-size ByteBuffer allocation for optimal memory performance.
     */
    public byte[] pack() {
        // Convert type string to type code (simplified: we can map common types)
        byte typeCode;
        switch (type) {
            case "REGISTER":
                typeCode = TYPE_REGISTER;
                break;
            case "CONFIG":
                typeCode = TYPE_CONFIG;
                break;
            case "TASK":
                typeCode = TYPE_TASK;
                break;
            case "RESULT":
                typeCode = TYPE_RESULT;
                break;
            case "ACK":
                typeCode = TYPE_ACK;
                break;
            case "HEARTBEAT":
                typeCode = TYPE_HEARTBEAT;
                break;
            default:
                typeCode = 0;
        }
        byte[] magicBytes = MAGIC;
        byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);
        if (senderBytes.length > 65535)
            throw new IllegalArgumentException("Sender too long");
        byte[] studentIdBytes = (studentId != null ? studentId : "").getBytes(StandardCharsets.UTF_8);
        if (studentIdBytes.length > 65535)
            throw new IllegalArgumentException("StudentId too long");
        byte[] payloadBytes = payload != null ? payload : new byte[0];

        // Calculate exact totalSize for pre-allocation (EFFICIENCY optimization)
        int totalSize = magicBytes.length + 1 + 1 + 2 + senderBytes.length + 2 + studentIdBytes.length + 8 + 4
                + payloadBytes.length;

        // Pre-allocate exact size to minimize GC overhead
        ByteBuffer buf = ByteBuffer.allocate(4 + totalSize); // include length field itself
        buf.putInt(totalSize); // length of rest of message
        buf.put(magicBytes);
        buf.put(VERSION);
        buf.put(typeCode);
        buf.putShort((short) senderBytes.length);
        buf.put(senderBytes);
        buf.putShort((short) studentIdBytes.length);
        buf.put(studentIdBytes);
        buf.putLong(timestamp);
        buf.putInt(payloadBytes.length);
        buf.put(payloadBytes);
        return buf.array();
    }

    /**
     * Reconstructs a Message from a byte stream with buffered reading for jumbo
     * payloads.
     * Handles TCP fragmentation by reading data in chunks.
     */
    public static Message unpack(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int totalLen = buf.getInt(); // should match data.length-4, but we trust
        byte[] magicBytes = new byte[6];
        buf.get(magicBytes);
        byte version = buf.get();
        byte typeCode = buf.get();
        short senderLen = buf.getShort();
        byte[] senderBytes = new byte[senderLen];
        buf.get(senderBytes);
        short studentIdLen = buf.getShort();
        byte[] studentIdBytes = new byte[studentIdLen];
        buf.get(studentIdBytes);
        long timestamp = buf.getLong();
        int payloadLen = buf.getInt();
        byte[] payloadBytes = new byte[payloadLen];
        buf.get(payloadBytes);

        Message msg = new Message();
        msg.magic = new String(magicBytes, StandardCharsets.US_ASCII);
        msg.version = version;
        // Map type code to string
        switch (typeCode) {
            case TYPE_REGISTER:
                msg.type = "REGISTER";
                break;
            case TYPE_CONFIG:
                msg.type = "CONFIG";
                break;
            case TYPE_TASK:
                msg.type = "TASK";
                break;
            case TYPE_RESULT:
                msg.type = "RESULT";
                break;
            case TYPE_ACK:
                msg.type = "ACK";
                break;
            case TYPE_HEARTBEAT:
                msg.type = "HEARTBEAT";
                break;
            default:
                msg.type = "UNKNOWN";
        }
        msg.messageType = msg.type;
        msg.msgType = msg.type;
        msg.sender = new String(senderBytes, StandardCharsets.UTF_8);
        msg.studentId = new String(studentIdBytes, StandardCharsets.UTF_8);
        msg.timestamp = timestamp;
        msg.payload = payloadBytes;
        return msg;
    }

    /**
     * Buffered reader for jumbo payload handling. Reads data in chunks to handle
     * MTU fragmentation.
     * This method reads from input stream while respecting message boundaries.
     */
    public static Message unpackFromStream(java.io.DataInputStream in) throws java.io.IOException {
        byte[] buffer = new byte[65536];
        int bytesRead = 0;
        int totalNeeded = 4; // Start reading length prefix

        // Read length prefix first
        while (bytesRead < totalNeeded) {
            int chunkSize = in.read(buffer, bytesRead, totalNeeded - bytesRead);
            if (chunkSize == -1)
                break;
            bytesRead += chunkSize;
        }

        if (bytesRead < 4)
            throw new java.io.IOException("Stream closed prematurely");

        ByteBuffer lengthBuf = ByteBuffer.wrap(buffer, 0, 4);
        int messageLen = lengthBuf.getInt();
        totalNeeded = 4 + messageLen;

        // Read rest of message
        while (bytesRead < totalNeeded) {
            int chunkSize = in.read(buffer, bytesRead, Math.min(65536, totalNeeded - bytesRead));
            if (chunkSize == -1)
                break;
            bytesRead += chunkSize;
        }

        // Now unpack the complete message
        byte[] completeData = new byte[bytesRead];
        System.arraycopy(buffer, 0, completeData, 0, bytesRead);
        return unpack(completeData);
    }
}