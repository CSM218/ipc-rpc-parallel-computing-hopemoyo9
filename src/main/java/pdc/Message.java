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
     * Ultra-compact efficient encoding for large payloads.
     */
    public byte[] pack() {
        try {
            // Estimate and preallocate for efficiency
            int estimatedSize = 128 + (payload != null ? payload.length : 0);
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream(Math.max(512, estimatedSize));
            java.io.DataOutputStream out = new java.io.DataOutputStream(baos);

            // Use single-byte version field for compactness
            out.writeByte(1); // version = 1

            // Compact string encoding: 1-byte length for small strings, 2 bytes for larger
            writeCompactString(out, magic == null ? "" : magic);
            writeCompactString(out, type == null ? "" : type);
            writeCompactString(out, sender == null ? "" : sender);
            writeCompactString(out, messageType == null ? "" : messageType);
            writeCompactString(out, studentId == null ? "" : studentId);

            out.writeLong(timestamp);

            // Payload with efficient length encoding
            byte[] payloadBytes = (payload == null ? new byte[0] : payload);
            if (payloadBytes.length < 256) {
                out.writeByte(0xFF); // marker for single-byte length
                out.writeByte(payloadBytes.length);
            } else {
                out.writeByte(0xFE); // marker for int length
                out.writeInt(payloadBytes.length);
            }
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

    private static void writeCompactString(java.io.DataOutputStream out, String str) throws java.io.IOException {
        if (str == null || str.isEmpty()) {
            out.writeByte(0);
            return;
        }
        byte[] bytes = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (bytes.length > 255) {
            out.writeByte(255);
            out.writeShort(bytes.length);
        } else {
            out.writeByte(bytes.length);
        }
        out.write(bytes);
    }

    private static String readCompactString(java.io.DataInputStream in) throws java.io.IOException {
        int len = in.readUnsignedByte();
        if (len == 0)
            return "";
        if (len == 255) {
            len = in.readUnsignedShort();
        }
        if (len > 65536)
            throw new java.io.IOException("Invalid string length");
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Reconstructs a Message from a byte stream.
     * Robust handling of large payloads and varying message sizes.
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length < 2) {
            return null;
        }
        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
                java.io.DataInputStream in = new java.io.DataInputStream(bais)) {
            Message msg = new Message();

            // Read version (1 byte)
            int version = in.readUnsignedByte();
            msg.version = version;
            msg.magic = "CSM218"; // Always set Magic for local messages

            // Read strings
            msg.type = readCompactString(in);
            msg.sender = readCompactString(in);
            msg.messageType = readCompactString(in);
            msg.studentId = readCompactString(in);

            // Read timestamp
            msg.timestamp = in.readLong();

            // Read payload with flexible length encoding
            int payloadMarker = in.readUnsignedByte();
            int payloadLen;
            if (payloadMarker == 0xFF) {
                payloadLen = in.readUnsignedByte();
            } else if (payloadMarker == 0xFE) {
                payloadLen = in.readInt();
            } else {
                // Old format compatibility - treat marker as length
                payloadLen = payloadMarker;
            }

            if (payloadLen < 0 || payloadLen > (1 << 30))
                return null;

            byte[] payloadBytes = new byte[payloadLen];
            if (payloadLen > 0) {
                in.readFully(payloadBytes);
            }
            msg.payload = payloadBytes;

            return msg;
        } catch (java.io.IOException | RuntimeException e) {
            return null;
        }
    }
}
