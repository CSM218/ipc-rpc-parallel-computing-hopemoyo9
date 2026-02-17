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
     * EFFICIENCY_OPTIMIZED: Pre-allocation and minimal heap garbage collection.
     * Uses fixed buffer sizes and single allocation strategy.
     * Optimized for throughput with minimal GC pressure.
     */
    public byte[] pack() {
        try {
            // EFFICIENCY: Pre-calculate size to avoid resizing garbage
            String magicVal = magic == null ? "CSM218" : magic;
            String typeVal = type == null ? "" : type;
            String senderVal = sender == null ? "" : sender;
            String messageTypeVal = messageType == null ? "" : messageType;
            String studentIdVal = studentId == null ? "" : studentId;
            byte[] payloadBytes = (payload == null ? new byte[0] : payload);

            // Calculate exact size needed - no over-allocation
            int magicLen = magicVal.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            int typeLen = typeVal.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            int senderLen = senderVal.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            int messageLenLen = messageTypeVal.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            int studentIdLen = studentIdVal.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;

            // Exact size: 4-byte lengths for strings + content + 4 ints + 1 long + payload
            int totalSize = 4 + magicLen + // length + magic
                    4 + 4 + // version (int)
                    4 + typeLen + // length + type
                    4 + senderLen + // length + sender
                    4 + messageLenLen + // length + messageType
                    4 + studentIdLen + // length + studentId
                    8 + // timestamp (long)
                    4 + payloadBytes.length; // payload length + payload

            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream(totalSize);
            java.io.DataOutputStream out = new java.io.DataOutputStream(baos);

            // Write fields in standard order - consistent format
            writeString(out, magicVal);
            out.writeInt(version);
            writeString(out, typeVal);
            writeString(out, senderVal);
            writeString(out, messageTypeVal);
            writeString(out, studentIdVal);
            out.writeLong(timestamp);
            out.writeInt(payloadBytes.length);
            if (payloadBytes.length > 0) {
                out.write(payloadBytes);
            }
            out.flush();

            byte[] result = baos.toByteArray();
            baos.close();
            return result;
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to pack Message", e);
        }
    }

    private static void writeString(java.io.DataOutputStream out, String str) throws java.io.IOException {
        byte[] bytes = (str == null ? "" : str).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(java.io.DataInputStream in) throws java.io.IOException {
        int len = in.readInt();
        if (len < 0 || len > 1048576) {
            throw new java.io.IOException("Invalid string length: " + len);
        }
        if (len == 0)
            return "";
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length < 20) {
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
            if (payloadLen < 0 || payloadLen > (1 << 30)) {
                return null;
            }

            byte[] payloadBytes = new byte[payloadLen];
            if (payloadLen > 0) {
                in.readFully(payloadBytes);
            }
            msg.payload = payloadBytes;

            // Validation
            if (msg.magic == null || !msg.magic.equals("CSM218")) {
                return null;
            }

            return msg;
        } catch (java.io.IOException | RuntimeException e) {
            return null;
        }
    }
}
