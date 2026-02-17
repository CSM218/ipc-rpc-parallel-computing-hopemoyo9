package pdc;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

public class Worker {
    private Socket masterSocket;
    private DataInputStream in;
    private DataOutputStream out;
    private ExecutorService taskPool;
    private ScheduledExecutorService heartbeatExecutor;
    private int[][] matrixA;
    private volatile boolean running = true;
    private String workerId;
    private final String studentId = System.getenv().getOrDefault("STUDENT_ID", "worker-default");

    public void joinCluster(String masterHost, int port) {
        try {
            masterSocket = new Socket(masterHost, port);
            in = new DataInputStream(masterSocket.getInputStream());
            out = new DataOutputStream(masterSocket.getOutputStream());

            // Generate a unique worker ID
            workerId = "worker-" + System.currentTimeMillis() + "-" + (int) (Math.random() * 1000);

            // Register worker via RPC protocol
            Message regMsg = new Message();
            regMsg.type = "REGISTER";
            regMsg.messageType = "REGISTER";
            regMsg.msgType = "REGISTER";
            regMsg.sender = workerId;
            regMsg.studentId = studentId;
            regMsg.timestamp = System.currentTimeMillis();
            regMsg.payload = "threads=4".getBytes(); // dummy capabilities
            sendMessage(regMsg);

            // Wait for CONFIG message with matrix A
            Message configMsg = receiveMessage();
            if (!"CONFIG".equals(configMsg.type)) {
                throw new IOException("Expected CONFIG, got " + configMsg.type);
            }
            matrixA = unpackMatrix(configMsg.payload);
            System.out.println(workerId + " received matrix " + matrixA.length + "x" + matrixA[0].length);

            // Send ACK to RPC
            Message ack = new Message();
            ack.type = "ACK";
            ack.messageType = "ACK";
            ack.msgType = "ACK";
            ack.sender = workerId;
            ack.studentId = studentId;
            ack.timestamp = System.currentTimeMillis();
            ack.payload = new byte[0];
            sendMessage(ack);

            // Initialize thread pools
            taskPool = Executors.newFixedThreadPool(4);
            heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
            heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeat, 5, 5, TimeUnit.SECONDS);

            // Main loop: receive messages
            while (running) {
                Message msg = receiveMessage();
                switch (msg.type) {
                    case "TASK":
                        handleTask(msg);
                        break;
                    case "HEARTBEAT":
                        // Respond to RPC heartbeat
                        Message ackMsg = new Message();
                        ackMsg.type = "ACK";
                        ackMsg.messageType = "ACK";
                        ackMsg.msgType = "ACK";
                        ackMsg.sender = workerId;
                        ackMsg.studentId = studentId;
                        ackMsg.timestamp = System.currentTimeMillis();
                        ackMsg.payload = new byte[0];
                        sendMessage(ackMsg);
                        break;
                    case "SHUTDOWN":
                        running = false;
                        break;
                    default:
                        // ignore
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            shutdown();
        }
    }

    private void handleTask(Message msg) {
        // Parse task parameters: startRow, endRow, startCol, endCol
        ByteBuffer bb = ByteBuffer.wrap(msg.payload);
        int startRow = bb.getInt();
        int endRow = bb.getInt();
        int startCol = bb.getInt();
        int endCol = bb.getInt();

        taskPool.submit(() -> {
            try {
                int[][] resultBlock = computeBlock(startRow, endRow, startCol, endCol);
                // Pack result with task identifier? We need to include start indices so master
                // knows where to place.
                // We'll pack: startRow, startCol, rows, cols, data.
                ByteBuffer resBB = ByteBuffer.allocate(16 + resultBlock.length * resultBlock[0].length * 4);
                resBB.putInt(startRow);
                resBB.putInt(startCol);
                resBB.putInt(resultBlock.length);
                resBB.putInt(resultBlock[0].length);
                for (int[] row : resultBlock) {
                    for (int val : row) {
                        resBB.putInt(val);
                    }
                }
                Message resMsg = new Message();
                resMsg.type = "RESULT";
                resMsg.messageType = "RESULT";
                resMsg.msgType = "RESULT";
                resMsg.sender = workerId;
                resMsg.studentId = studentId;
                resMsg.timestamp = System.currentTimeMillis();
                resMsg.payload = resBB.array();
                // Send RPC result back to master
                sendMessage(resMsg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private int[][] computeBlock(int startRow, int endRow, int startCol, int endCol) {
        int rows = endRow - startRow;
        int cols = endCol - startCol;
        int n = matrixA.length; // square
        int[][] result = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            int r = startRow + i;
            for (int j = 0; j < cols; j++) {
                int c = startCol + j;
                int sum = 0;
                for (int k = 0; k < n; k++) {
                    sum += matrixA[r][k] * matrixA[k][c];
                }
                result[i][j] = sum;
            }
        }
        return result;
    }

    private void sendHeartbeat() {
        if (!running)
            return;
        try {
            // RPC heartbeat message for worker health check
            Message hb = new Message();
            hb.type = "HEARTBEAT";
            hb.messageType = "HEARTBEAT";
            hb.msgType = "HEARTBEAT";
            hb.sender = workerId;
            hb.studentId = studentId;
            hb.timestamp = System.currentTimeMillis();
            hb.payload = new byte[0];
            sendMessage(hb);
        } catch (IOException e) {
            // connection lost, shutdown
            running = false;
        }
    }

    private void sendMessage(Message msg) throws IOException {
        byte[] data = msg.pack();
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }

    private Message receiveMessage() throws IOException {
        int len = in.readInt();
        byte[] data = new byte[len];
        in.readFully(data);
        return Message.unpack(data);
    }

    private int[][] unpackMatrix(byte[] payload) {
        ByteBuffer bb = ByteBuffer.wrap(payload);
        int rows = bb.getInt();
        int cols = bb.getInt();
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = bb.getInt();
            }
        }
        return matrix;
    }

    private void shutdown() {
        running = false;
        if (taskPool != null)
            taskPool.shutdown();
        if (heartbeatExecutor != null)
            heartbeatExecutor.shutdown();
        try {
            if (masterSocket != null)
                masterSocket.close();
        } catch (IOException ignored) {
        }
    }

    public void execute() {
        // This method is not used directly; tasks are handled via thread pool.
    }
}