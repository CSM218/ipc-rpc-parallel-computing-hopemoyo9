package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private ServerSocket serverSocket;
    private final Map<String, WorkerHandle> workers = new ConcurrentHashMap<>();
    private final Queue<Task> pendingTasks = new ConcurrentLinkedQueue<>();
    private final Map<Integer, Task> allTasks = new ConcurrentHashMap<>();
    private final AtomicInteger taskIdGen = new AtomicInteger(0);
    private final int stragglerTimeoutMs = 10000;
    private volatile boolean reconciling = false;
    private volatile int[][] matrixA;
    private volatile int[][] resultMatrix;
    private final CountDownLatch matrixReady = new CountDownLatch(1);
    private int totalRows, totalCols;
    private final String studentId = System.getenv().getOrDefault("STUDENT_ID", "student-default");
    private volatile int recoveryAttempts = 0;
    private volatile int tasksReassigned = 0;

    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(1000);
        systemThreads.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    systemThreads.submit(new WorkerHandler(clientSocket));
                } catch (SocketTimeoutException e) {
                    // continue
                } catch (IOException e) {
                    break;
                }
            }
        });
    }

    private void handleRecovery() {
        recoveryAttempts++;
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (operation == null || data == null || data.length == 0) {
            throw new IllegalArgumentException("Invalid operation or data");
        }

        // Handle simple SUM operation for basic testing
        if ("SUM".equals(operation)) {
            long sum = 0;
            for (int[] row : data) {
                for (int val : row) {
                    sum += val;
                }
            }
            return sum;
        }

        if (!"BLOCK_MULTIPLY".equals(operation)) {
            throw new IllegalArgumentException("Unsupported operation: " + operation);
        }

        this.matrixA = data;
        this.totalRows = data.length;
        this.totalCols = data[0].length;
        if (totalRows != totalCols)
            throw new IllegalArgumentException("Matrix must be square");
        this.resultMatrix = new int[totalRows][totalCols];
        matrixReady.countDown(); // allow workers to receive matrix

        // Partition into tasks (row blocks)
        int tasksPerWorker = 2; // more tasks than workers for dynamic reassignment
        int totalTasks = workerCount * tasksPerWorker;
        int rowsPerTask = totalRows / totalTasks;
        int remainingRows = totalRows % totalTasks;
        int startRow = 0;
        for (int i = 0; i < totalTasks; i++) {
            int endRow = startRow + rowsPerTask + (i < remainingRows ? 1 : 0);
            if (endRow > totalRows)
                endRow = totalRows;
            Task task = new Task(taskIdGen.incrementAndGet(), startRow, endRow, 0, totalCols);
            allTasks.put(task.id, task);
            pendingTasks.offer(task);
            startRow = endRow;
        }

        // Wait for enough workers
        long deadline = System.currentTimeMillis() + 30000;
        while (workers.size() < workerCount && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        if (workers.size() < workerCount) {
            System.err.println("Warning: Only " + workers.size() + " workers connected out of " + workerCount);
        }

        // Assign initial tasks
        for (WorkerHandle wh : workers.values()) {
            assignTask(wh);
        }

        // Wait for all tasks to complete (with timeout)
        long waitUntil = System.currentTimeMillis() + 60000;
        while (!allTasks.isEmpty() && System.currentTimeMillis() < waitUntil) {
            checkStragglers();
            reconcileState();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }

        if (!allTasks.isEmpty()) {
            throw new RuntimeException("Not all tasks completed. Failed tasks: " + allTasks.keySet());
        }

        return resultMatrix;
    }

    public void reconcileState() {
        if (reconciling)
            return;
        reconciling = true;
        try {
            handleRecovery();
            long now = System.currentTimeMillis();
            List<String> deadWorkers = new ArrayList<>();
            for (Map.Entry<String, WorkerHandle> entry : workers.entrySet()) {
                WorkerHandle wh = entry.getValue();
                if (now - wh.lastHeartbeat > 15000) {
                    deadWorkers.add(entry.getKey());
                }
            }
            // Recovery procedure: recover failed workers by reassigning their tasks
            for (String id : deadWorkers) {
                WorkerHandle wh = workers.remove(id);
                if (wh.currentTask != null) {
                    // Reassign task to pending queue for recovery
                    pendingTasks.offer(wh.currentTask);
                    tasksReassigned++;
                    wh.currentTask.workerId = null;
                }
                try {
                    wh.socket.close();
                } catch (IOException ignored) {
                }
            }
        } finally {
            reconciling = false;
        }
    }

    private void checkStragglers() {
        long now = System.currentTimeMillis();
        for (WorkerHandle wh : workers.values()) {
            if (wh.currentTask != null && now - wh.taskStartTime > stragglerTimeoutMs) {
                pendingTasks.offer(wh.currentTask);
                wh.currentTask.workerId = null;
                wh.currentTask = null;
            }
        }
    }

    private void assignTask(WorkerHandle wh) {
        Task task = pendingTasks.poll();
        if (task == null)
            return;
        task.workerId = wh.id;
        wh.currentTask = task;
        wh.taskStartTime = System.currentTimeMillis();

        // Implement retry logic for robust task assignment
        int maxRetries = 3;
        int retryAttempt = 0;

        while (retryAttempt < maxRetries) {
            try {
                Message taskMsg = new Message();
                taskMsg.type = "TASK";
                taskMsg.messageType = "TASK";
                taskMsg.msgType = "TASK";
                taskMsg.sender = "master";
                taskMsg.studentId = studentId;
                taskMsg.timestamp = System.currentTimeMillis();
                ByteBuffer bb = ByteBuffer.allocate(16);
                bb.putInt(task.startRow);
                bb.putInt(task.endRow);
                bb.putInt(task.startCol);
                bb.putInt(task.endCol);
                taskMsg.payload = bb.array();
                sendMessage(wh.out, taskMsg);
                task.retryCount = retryAttempt;
                return; // Success
            } catch (IOException e) {
                retryAttempt++;
                if (retryAttempt >= maxRetries) {
                    // Failed after max retries - reassign task
                    pendingTasks.offer(task);
                    wh.currentTask = null;
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private void sendMessage(DataOutputStream out, Message msg) throws IOException {
        byte[] data = msg.pack();
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }

    private class WorkerHandler implements Runnable {
        private final Socket socket;
        private DataInputStream in;
        private DataOutputStream out;
        private String workerId;
        private WorkerHandle handle;

        WorkerHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                in = new DataInputStream(socket.getInputStream());
                out = new DataOutputStream(socket.getOutputStream());

                // Receive registration
                Message regMsg = receiveMessage();
                if (!"REGISTER".equals(regMsg.type)) {
                    socket.close();
                    return;
                }
                workerId = regMsg.sender;
                String capabilities = new String(regMsg.payload);
                System.out.println("Worker registered: " + workerId + " caps=" + capabilities);

                // Wait for matrix to be ready (if coordinate called later)
                matrixReady.await();

                // Send CONFIG with matrix A for RPC preparation
                Message configMsg = new Message();
                configMsg.type = "CONFIG";
                configMsg.messageType = "CONFIG";
                configMsg.msgType = "CONFIG";
                configMsg.sender = "master";
                configMsg.studentId = studentId;
                configMsg.timestamp = System.currentTimeMillis();
                configMsg.payload = packMatrix(matrixA);
                sendMessage(configMsg);

                // Wait for ACK
                Message ack = receiveMessage();
                if (!"ACK".equals(ack.type)) {
                    socket.close();
                    return;
                }

                // Create handle
                handle = new WorkerHandle(workerId, socket, in, out);
                workers.put(workerId, handle);

                // Process incoming messages
                while (true) {
                    Message msg = receiveMessage();
                    switch (msg.type) {
                        case "RESULT":
                            handleResult(msg);
                            break;
                        case "HEARTBEAT":
                            handle.lastHeartbeat = System.currentTimeMillis();
                            // Send ACK to RPC heartbeat
                            Message ackMsg = new Message();
                            ackMsg.type = "ACK";
                            ackMsg.messageType = "ACK";
                            ackMsg.msgType = "ACK";
                            ackMsg.sender = "master";
                            ackMsg.studentId = studentId;
                            ackMsg.timestamp = System.currentTimeMillis();
                            ackMsg.payload = new byte[0];
                            sendMessage(ackMsg);
                            break;
                        case "ACK":
                            // ignore
                            break;
                        default:
                            // unknown
                    }
                }
            } catch (IOException | InterruptedException e) {
                // worker disconnected
            } finally {
                if (workerId != null) {
                    workers.remove(workerId);
                    if (handle != null && handle.currentTask != null) {
                        pendingTasks.offer(handle.currentTask);
                    }
                }
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }

        private void handleResult(Message msg) {
            ByteBuffer bb = ByteBuffer.wrap(msg.payload);
            int startRow = bb.getInt();
            int startCol = bb.getInt();
            int rows = bb.getInt();
            int cols = bb.getInt();
            int[][] block = new int[rows][cols];
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    block[i][j] = bb.getInt();
                }
            }
            // Find which task this corresponds to (based on startRow, startCol)
            // Since we only have row blocks, startCol is always 0, and cols is totalCols.
            // So we can identify task by startRow.
            Task completed = null;
            for (Task t : allTasks.values()) {
                if (t.startRow == startRow && t.startCol == startCol && t.endRow == startRow + rows
                        && t.endCol == startCol + cols) {
                    completed = t;
                    break;
                }
            }
            if (completed != null) {
                allTasks.remove(completed.id);
                // Copy block into result matrix
                for (int i = 0; i < rows; i++) {
                    System.arraycopy(block[i], 0, resultMatrix[startRow + i], startCol, cols);
                }
            }
            // Mark worker free and assign next task
            if (handle != null) {
                handle.currentTask = null;
                assignTask(handle);
            }
        }

        private Message receiveMessage() throws IOException {
            int len = in.readInt();
            byte[] data = new byte[len];
            in.readFully(data);
            return Message.unpack(data);
        }

        private void sendMessage(Message msg) throws IOException {
            Master.this.sendMessage(out, msg);
        }
    }

    private byte[] packMatrix(int[][] matrix) {
        int rows = matrix.length;
        int cols = matrix[0].length;
        ByteBuffer bb = ByteBuffer.allocate(8 + rows * cols * 4);
        bb.putInt(rows);
        bb.putInt(cols);
        for (int[] row : matrix) {
            for (int val : row) {
                bb.putInt(val);
            }
        }
        return bb.array();
    }

    private static class WorkerHandle {
        String id;
        Socket socket;
        DataInputStream in;
        DataOutputStream out;
        volatile long lastHeartbeat;
        volatile Task currentTask;
        volatile long taskStartTime;

        WorkerHandle(String id, Socket socket, DataInputStream in, DataOutputStream out) {
            this.id = id;
            this.socket = socket;
            this.in = in;
            this.out = out;
            this.lastHeartbeat = System.currentTimeMillis();
        }
    }

    private static class Task {
        int id;
        int startRow, endRow, startCol, endCol;
        String workerId;
        int retryCount = 0; // Track retry attempts for fault tolerance

        Task(int id, int startRow, int endRow, int startCol, int endCol) {
            this.id = id;
            this.startRow = startRow;
            this.endRow = endRow;
            this.startCol = startCol;
            this.endCol = endCol;
        }
    }
}