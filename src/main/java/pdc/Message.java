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
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                java.io.DataOutputStream out = new java.io.DataOutputStream(baos)) {
            byte[] magicBytes = (magic == null ? "" : magic).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(magicBytes.length);
            out.write(magicBytes);

            out.writeInt(version);

            byte[] typeBytes = (type == null ? "" : type).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(typeBytes.length);
            out.write(typeBytes);

            byte[] senderBytes = (sender == null ? "" : sender).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(senderBytes.length);
            out.write(senderBytes);

            byte[] messageTypeBytes = (messageType == null ? "" : messageType)
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(messageTypeBytes.length);
            out.write(messageTypeBytes);

            byte[] studentIdBytes = (studentId == null ? "" : studentId)
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(studentIdBytes.length);
            out.write(studentIdBytes);

            out.writeLong(timestamp);

            byte[] payloadBytes = (payload == null ? new byte[0] : payload);
            out.writeInt(payloadBytes.length);
            out.write(payloadBytes);

            out.flush();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to pack Message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        if (data == null) {
            return null;
        }
        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
                java.io.DataInputStream in = new java.io.DataInputStream(bais)) {
            Message msg = new Message();
            int magicLen = in.readInt();
            if (magicLen < 0 || magicLen > 1024)
                return null;
            byte[] magicBytes = new byte[magicLen];
            in.readFully(magicBytes);
            msg.magic = new String(magicBytes, java.nio.charset.StandardCharsets.UTF_8);

            msg.version = in.readInt();

            int typeLen = in.readInt();
            if (typeLen < 0 || typeLen > 1024)
                return null;
            byte[] typeBytes = new byte[typeLen];
            in.readFully(typeBytes);
            msg.type = new String(typeBytes, java.nio.charset.StandardCharsets.UTF_8);

            int senderLen = in.readInt();
            if (senderLen < 0 || senderLen > 1024)
                return null;
            byte[] senderBytes = new byte[senderLen];
            in.readFully(senderBytes);
            msg.sender = new String(senderBytes, java.nio.charset.StandardCharsets.UTF_8);

            int messageTypeLen = in.readInt();
            if (messageTypeLen < 0 || messageTypeLen > 1024)
                return null;
            byte[] messageTypeBytes = new byte[messageTypeLen];
            in.readFully(messageTypeBytes);
            msg.messageType = new String(messageTypeBytes, java.nio.charset.StandardCharsets.UTF_8);

            int studentIdLen = in.readInt();
            if (studentIdLen < 0 || studentIdLen > 1024)
                return null;
            byte[] studentIdBytes = new byte[studentIdLen];
            in.readFully(studentIdBytes);
            msg.studentId = new String(studentIdBytes, java.nio.charset.StandardCharsets.UTF_8);

            msg.timestamp = in.readLong();

            int payloadLen = in.readInt();
            if (payloadLen < 0 || payloadLen > (1 << 24))
                return null;
            byte[] payloadBytes = new byte[payloadLen];
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
