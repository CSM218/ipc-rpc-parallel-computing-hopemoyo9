package pdc;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public String magic;
    public int version;
    public String type;
    public String sender;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * This returns just the message data - the caller handles TCP framing with length prefix.
     * Optimized for efficiency with preallocated buffer sizing.
     */
    public byte[] pack() {
        try {
            // Estimate size and preallocate to avoid resizing
            int estimatedSize = 100 + 
                (magic != null ? magic.length() : 0) +
                (type != null ? type.length() : 0) +
                (sender != null ? sender.length() : 0) +
                (messageType != null ? messageType.length() : 0) +
                (studentId != null ? studentId.length() : 0) +
                (payload != null ? payload.length : 0);
            
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream(Math.max(256, estimatedSize));
            java.io.DataOutputStream out = new java.io.DataOutputStream(baos);
            
            // Write message fields efficiently with compact encoding
            writeString(out, magic == null ? "" : magic);
            out.writeInt(version);
            writeString(out, type == null ? "" : type);
            writeString(out, sender == null ? "" : sender);
            writeString(out, messageType == null ? "" : messageType);
            writeString(out, studentId == null ? "" : studentId);
            out.writeLong(timestamp);

            byte[] payloadBytes = (payload == null ? new byte[0] : payload);
            out.writeInt(payloadBytes.length);
            if (payloadBytes.length > 0) {
                out.write(payloadBytes);
            }

            out.flush();
            out.close();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to pack Message", e);
        }
    }

    private static void writeString(java.io.DataOutputStream out, String str) throws java.io.IOException {
        byte[] bytes = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    private static String readString(java.io.DataInputStream in) throws java.io.IOException {
        int len = in.readShort();
        if (len < 0 || len > 4096)
            throw new java.io.IOException("Invalid string length");
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Reconstructs a Message from a byte stream.
     * Expects just the message data (without TCP length prefix).
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length < 8) {
            return null;
        }
        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
                java.io.DataInputStream in = new java.io.DataInputStream(bais)) {
            Message msg = new Message();

            msg.magic = readString(in);
            msg.version = in.readInt();
            msg.type = readString(in);
            msg.sender = readString(in);
            msg.messageType = readString(in);
            msg.studentId = readString(in);
            msg.timestamp = in.readLong();

            int payloadLen = in.readInt();
            if (payloadLen < 0 || payloadLen > (1 << 30))
                return null;
            byte[] payloadBytes = new byte[payloadLen];
            if (payloadLen > 0)
                in.readFully(payloadBytes);
            msg.payload = payloadBytes;

            // Basic validation: magic must match expected tag when present
            if (msg.magic != null && !msg.magic.isEmpty() && !"CSM218".equals(msg.magic)) {
                return null;
            }

            return msg;
        } catch (java.io.IOException | RuntimeException e) {
            return null;
        }
    }
}
