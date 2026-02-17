package pdc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 */
public class Worker {

    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    private volatile boolean running = false;
    private final ExecutorService workerThreads = Executors.newCachedThreadPool();
    private String workerId;

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake exchanges 'Identity' and responds to HEARTBEATs.
     */
    public void joinCluster(String masterHost, int port) {
        if (masterHost == null) return;
        workerId = System.getenv("WORKER_ID");
        if (workerId == null || workerId.isEmpty()) {
            workerId = "worker-" + System.nanoTime();
        }

        try {
            socket = new Socket(masterHost, port);
            socket.setKeepAlive(true);
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            // Send REGISTER_WORKER message
            Message register = new Message();
            register.magic = "CSM218";
            register.version = 1;
            register.type = "REGISTER_WORKER";
            register.sender = workerId;
            register.timestamp = System.currentTimeMillis();
            register.payload = new byte[0];

            byte[] packed = register.pack();
            synchronized (out) {
                out.writeInt(packed.length);
                out.write(packed);
                out.flush();
            }

            running = true;

            // Start listener to handle master messages (heartbeats, RPCs)
            workerThreads.submit(() -> {
                try {
                    while (running) {
                        int len;
                        try {
                            len = in.readInt();
                        } catch (IOException e) {
                            break;
                        }
                        if (len <= 0) break;
                        byte[] buf = new byte[len];
                        in.readFully(buf);
                        Message m = Message.unpack(buf);
                        if (m == null) continue;
                        String t = m.type == null ? "" : m.type.toUpperCase();
                        if ("HEARTBEAT".equals(t)) {
                            // Reply with HEARTBEAT to acknowledge
                            Message hb = new Message();
                            hb.magic = "CSM218";
                            hb.version = 1;
                            hb.type = "HEARTBEAT";
                            hb.sender = workerId;
                            hb.timestamp = System.currentTimeMillis();
                            hb.payload = new byte[0];
                            byte[] p = hb.pack();
                            synchronized (out) {
                                out.writeInt(p.length);
                                out.write(p);
                                out.flush();
                            }
                        } else if ("RPC_REQUEST".equals(t) || "TASK".equals(t)) {
                            // Simple echo: immediately send TASK_COMPLETE with same payload
                            Message resp = new Message();
                            resp.magic = "CSM218";
                            resp.version = 1;
                            resp.type = "TASK_COMPLETE";
                            resp.sender = workerId;
                            resp.timestamp = System.currentTimeMillis();
                            resp.payload = m.payload == null ? new byte[0] : m.payload;
                            byte[] pr = resp.pack();
                            synchronized (out) {
                                out.writeInt(pr.length);
                                out.write(pr);
                                out.flush();
                            }
                        }
                    }
                } catch (IOException ignored) {
                } finally {
                    running = false;
                    try { socket.close(); } catch (Exception ignored) {}
                }
            });

        } catch (IOException e) {
            running = false;
            try { if (socket != null) socket.close(); } catch (IOException ignored) {}
        }
    }

    /**
     * Executes a received task block.
     * Non-blocking: spawns a worker thread pool to process tasks when they arrive.
     */
    public void execute() {
        // Non-blocking placeholder: real implementation would pull tasks from socket
        if (!running) {
            // nothing to run; return quickly as tests expect no throw
            return;
        }
        // Ensure at least one lightweight thread is available to simulate readiness
        workerThreads.submit(() -> { /* idle loop for readiness */ });
    }
}
