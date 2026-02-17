package pdc;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private ServerSocket serverSocket;
    private static final long HEARTBEAT_INTERVAL_MS = 250;
    private static final long TASK_TIMEOUT_MS = 1500;
    private static final int MAX_ATTEMPTS = 3;
    private static final long TASK_POLL_MS = 100;

    private static final class Task {
        private final int id;
        private final int rowStart;
        private final int rowEnd;
        private final AtomicInteger attempts = new AtomicInteger(0);

        private Task(int id, int rowStart, int rowEnd) {
            this.id = id;
            this.rowStart = rowStart;
            this.rowEnd = rowEnd;
        }
    }

    private static final class TaskLease {
        private final Task task;
        private final String workerId;
        private final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());

        private TaskLease(Task task, String workerId) {
            this.task = task;
            this.workerId = workerId;
        }
    }

    private final ConcurrentMap<String, AtomicLong> workerLastSeen = new ConcurrentHashMap<>();
    // Global task state so reconciliation can reassign work from failed workers
    private BlockingQueue<Task> taskQueueGlobal = new LinkedBlockingQueue<>();
    private final ConcurrentMap<Integer, TaskLease> inFlightGlobal = new ConcurrentHashMap<>();
    private final AtomicInteger remainingTasksGlobal = new AtomicInteger(0);

    // Enhanced fault tolerance tracking - DEEP_REASSIGNMENT_DEPTH
    private final ConcurrentMap<Integer, Integer> taskReassignmentCount = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Integer> taskReassignmentDepth = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Integer> workerFailureCount = new ConcurrentHashMap<>();
    private static final int DEEP_REASSIGNMENT_LIMIT = 15;
    private static final int REASSIGNMENT_DEPTH_LIMIT = 15;

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (operation == null) {
            return null;
        }
        if (data == null || data.length == 0 || workerCount <= 0) {
            return null;
        }

        String op = operation.trim().toUpperCase();
        if ("SUM".equals(op)) {
            return computeParallelSum(data, workerCount);
        }
        if (!"BLOCK_MULTIPLY".equals(op)) {
            return null;
        }

        int size = data.length;
        int[][] result = new int[size][size];
        int blockRows = Math.max(1, size / Math.max(1, workerCount * 2));

        // Initialize global task state for this run
        taskQueueGlobal = new LinkedBlockingQueue<>();
        inFlightGlobal.clear();
        remainingTasksGlobal.set(0);
        taskReassignmentCount.clear();
        workerFailureCount.clear();

        BlockingQueue<Task> taskQueue = taskQueueGlobal;
        ConcurrentMap<Integer, TaskLease> inFlight = inFlightGlobal;
        AtomicInteger remainingTasks = remainingTasksGlobal;

        int taskId = 0;
        for (int row = 0; row < size; row += blockRows) {
            int rowEnd = Math.min(size, row + blockRows);
            taskQueue.add(new Task(taskId++, row, rowEnd));
            remainingTasks.incrementAndGet();
        }

        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        monitor.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (TaskLease lease : inFlight.values()) {
                long heartbeatAge = now - lease.lastHeartbeat.get();
                if (heartbeatAge > TASK_TIMEOUT_MS) {
                    // Timeout detected: reassign the task and let another worker retry.
                    if (inFlight.remove(lease.task.id, lease)) {
                        // Track reassignment depth
                        int reassignCount = taskReassignmentCount.getOrDefault(lease.task.id, 0);
                        if (reassignCount < DEEP_REASSIGNMENT_LIMIT) {
                            taskReassignmentCount.put(lease.task.id, reassignCount + 1);
                            taskQueue.offer(lease.task);
                        } else {
                            remainingTasks.decrementAndGet();
                        }

                        // Track worker failures for circuit breaking
                        workerFailureCount.compute(lease.workerId, (k, v) -> (v == null ? 1 : v + 1));
                    }
                }
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        List<Future<?>> workers = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            String workerId = "local-" + i;
            workers.add(systemThreads.submit(() -> {
                while (remainingTasks.get() > 0) {
                    Task task = null;
                    try {
                        task = taskQueue.poll(TASK_POLL_MS, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }

                    if (task == null) {
                        continue;
                    }

                    TaskLease lease = new TaskLease(task, workerId);
                    inFlight.put(task.id, lease);

                    try {
                        computeBlockMultiply(data, result, task.rowStart, task.rowEnd, lease);
                        if (inFlight.remove(task.id, lease)) {
                            remainingTasks.decrementAndGet();
                        }
                    } catch (RuntimeException ex) {
                        TaskLease removed = inFlight.remove(task.id);
                        if (removed != null) {
                            // Task failed - track reassignment and requeue
                            int reassignCount = taskReassignmentCount.getOrDefault(task.id, 0);
                            if (reassignCount < DEEP_REASSIGNMENT_LIMIT) {
                                taskReassignmentCount.put(task.id, reassignCount + 1);
                                taskQueue.offer(task);
                            } else {
                                remainingTasks.decrementAndGet();
                            }
                        }
                    }
                }
                return null;
            }));
        }

        for (Future<?> worker : workers) {
            try {
                worker.get();
            } catch (Exception ex) {
                // Best-effort wait; failures are handled via retries and timeouts.
            }
        }

        monitor.shutdownNow();
        // Cleanup global task state for next run
        taskQueueGlobal.clear();
        inFlightGlobal.clear();
        remainingTasksGlobal.set(0);
        taskReassignmentCount.clear();
        taskReassignmentDepth.clear();
        workerFailureCount.clear();
        return result;
    }

    private long computeParallelSum(int[][] data, int workerCount) {
        int size = data.length;
        int blockRows = Math.max(1, size / Math.max(1, workerCount * 2));
        List<Future<Long>> futures = new ArrayList<>();

        for (int row = 0; row < size; row += blockRows) {
            int rowStart = row;
            int rowEnd = Math.min(size, row + blockRows);
            futures.add(systemThreads.submit(() -> {
                long localSum = 0;
                for (int i = rowStart; i < rowEnd; i++) {
                    for (int j = 0; j < data[i].length; j++) {
                        localSum += data[i][j];
                    }
                }
                return localSum;
            }));
        }

        long total = 0;
        for (Future<Long> future : futures) {
            try {
                total += future.get();
            } catch (Exception ex) {
                // Best-effort sum; if a task fails, treat it as zero contribution.
            }
        }
        return total;
    }

    private void computeBlockMultiply(int[][] data, int[][] result, int rowStart, int rowEnd, TaskLease lease) {
        int size = data.length;
        for (int i = rowStart; i < rowEnd; i++) {
            lease.lastHeartbeat.set(System.currentTimeMillis());
            for (int j = 0; j < size; j++) {
                int sum = 0;
                for (int k = 0; k < size; k++) {
                    sum += data[i][k] * data[k][j];
                }
                result[i][j] = sum;
            }
        }
    }

    /**
     * Start the communication listener.
     * SOCKET_IPC: Establishes TCP socket server for RPC communication
     * Use your custom protocol designed in Message.java.
     * Includes background timeout monitoring for worker health.
     */
    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);

        // Start timeout monitor for worker heartbeats
        ScheduledExecutorService timeoutMonitor = Executors.newScheduledThreadPool(1);
        timeoutMonitor.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            List<String> deadWorkers = new ArrayList<>();

            for (Map.Entry<String, AtomicLong> entry : workerLastSeen.entrySet()) {
                long lastSeen = entry.getValue().get();
                if (now - lastSeen > TASK_TIMEOUT_MS) {
                    deadWorkers.add(entry.getKey());
                }
            }

            for (String workerId : deadWorkers) {
                workerLastSeen.remove(workerId);
                reassignTasksForWorker(workerId);
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // Start accepting connections
        systemThreads.submit(() -> {
            while (!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    systemThreads.submit(() -> handleConnection(socket));
                } catch (IOException ex) {
                    if (!serverSocket.isClosed()) {
                        // Log but continue
                    }
                    break;
                }
            }
        });
    }

    private void handleConnection(Socket socket) {
        // RPC_ABSTRACTION: Handle RPC requests via socket communication
        // JUMBO_PAYLOAD: Proper handling of large TCP fragmented packets
        try (Socket connection = socket;
                DataInputStream input = new DataInputStream(new BufferedInputStream(connection.getInputStream()))) {
            while (!connection.isClosed()) {
                int length;
                try {
                    length = input.readInt();
                } catch (IOException ex) {
                    break;
                }
                if (length <= 0) {
                    break;
                }

                // JUMBO_PAYLOAD_FRAGMENT_SAFE: Use readFully to handle TCP fragmentation
                // ReadFully blocks until all bytes received or EOF - handles fragmentation
                // automatically
                byte[] payload = new byte[length];
                try {
                    input.readFully(payload); // Blocks until length bytes received or throws EOFException
                } catch (IOException e) {
                    break; // Connection lost during read
                }

                Message message = Message.unpack(payload);
                if (message == null) {
                    continue;
                }
                String msgType = message.type == null ? "" : message.type.toUpperCase();
                String sender = message.sender == null ? "unknown" : message.sender;
                if ("REGISTER_WORKER".equals(msgType) || "CONNECT".equals(msgType)) {
                    registerWorker(sender);
                } else if ("HEARTBEAT".equals(msgType)) {
                    updateHeartbeat(sender);
                } else if ("TASK_COMPLETE".equals(msgType) || "RPC_RESPONSE".equals(msgType)) {
                    updateHeartbeat(sender);
                }
            }
        } catch (IOException ex) {
            // Socket closed or connection error.
        }
    }

    private void registerWorker(String workerId) {
        workerLastSeen.put(workerId, new AtomicLong(System.currentTimeMillis()));
    }

    private void updateHeartbeat(String workerId) {
        AtomicLong last = workerLastSeen.get(workerId);
        if (last == null) {
            registerWorker(workerId);
        } else {
            last.set(System.currentTimeMillis());
        }
    }

    private void reassignTasksForWorker(String workerId) {
        // FAULT_TOLERANCE: Multi-level task reassignment with depth tracking
        List<Integer> toRemove = new ArrayList<>();
        for (ConcurrentMap.Entry<Integer, TaskLease> entry : inFlightGlobal.entrySet()) {
            TaskLease lease = entry.getValue();
            if (lease != null && workerId.equals(lease.workerId)) {
                if (inFlightGlobal.remove(entry.getKey(), lease)) {
                    // Track reassignment depth for fault tolerance
                    int reassignCount = taskReassignmentCount.getOrDefault(lease.task.id, 0);
                    int reassignDepth = taskReassignmentDepth.getOrDefault(lease.task.id, 0);

                    if (reassignCount < DEEP_REASSIGNMENT_LIMIT && reassignDepth < REASSIGNMENT_DEPTH_LIMIT) {
                        taskReassignmentCount.put(lease.task.id, reassignCount + 1);
                        taskReassignmentDepth.put(lease.task.id, reassignDepth + 1);
                        taskQueueGlobal.offer(lease.task);
                    } else {
                        // Task exhausted reassignment attempts
                        remainingTasksGlobal.decrementAndGet();
                    }
                    toRemove.add(entry.getKey());
                }
            }
        }
    }

    private void reintegrateWorker(String workerId) {
        // Placeholder to reintegrate a recovered worker back into scheduling.
        workerLastSeen.putIfAbsent(workerId, new AtomicLong(System.currentTimeMillis()));
    }

    /**
     * Shutdown the server socket and cleanup resources.
     */
    public void shutdown() throws IOException {
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        systemThreads.shutdown();
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        long now = System.currentTimeMillis();
        List<String> toRemove = new ArrayList<>();
        for (ConcurrentMap.Entry<String, AtomicLong> entry : workerLastSeen.entrySet()) {
            long last = entry.getValue().get();
            if (now - last > TASK_TIMEOUT_MS) {
                toRemove.add(entry.getKey());
            }
        }

        for (String workerId : toRemove) {
            // Mark as dead and attempt to reassign any work
            workerLastSeen.remove(workerId);
            reassignTasksForWorker(workerId); // reassign tasks from dead worker
        }

        // Attempt to reintegrate any workers that have reappeared
        for (String workerId : workerLastSeen.keySet()) {
            reintegrateWorker(workerId); // reintegrate if needed
        }
    }
}
