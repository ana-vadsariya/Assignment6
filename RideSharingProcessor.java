import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RideSharingProcessor
 * - Uses a BlockingQueue for tasks
 * - Fixed thread pool workers consume tasks
 * - Results collected into a thread-safe list
 * - Writes results to a file at the end
 */
public class RideSharingProcessor {
    private static final Logger logger = Logger.getLogger(RideSharingProcessor.class.getName());

    // Simple task representation
    static class Task {
        final String id;
        final String payload;
        Task(String payload) {
            this.id = UUID.randomUUID().toString();
            this.payload = payload;
        }
        @Override
        public String toString() {
            return String.format("Task[id=%s,payload=%s]", id, payload);
        }
    }

    // Poison pill sentinel to stop workers
    private static final Task POISON = new Task("__POISON__");

    public static void main(String[] args) {
        int numWorkers = 4;
        int numTasks = 20;
        String outFile = "results.txt";

        BlockingQueue<Task> queue = new LinkedBlockingQueue<>();
        // Thread-safe results collection
        ConcurrentLinkedQueue<String> results = new ConcurrentLinkedQueue<>();

        ExecutorService workers = Executors.newFixedThreadPool(numWorkers);

        // Worker Runnable
        class Worker implements Runnable {
            private final int workerId;
            Worker(int id) { this.workerId = id; }
            @Override
            public void run() {
                logger.info(() -> "Worker " + workerId + " started.");
                try {
                    while (true) {
                        Task t = queue.take(); // blocking
                        if (t == POISON) {
                            // re-insert poison so other workers can stop too
                            queue.put(POISON);
                            logger.info(() -> "Worker " + workerId + " received poison and will exit.");
                            break;
                        }
                        try {
                            String res = processTask(workerId, t);
                            results.add(res);
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "Worker " + workerId + " encountered error processing " + t, e);
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.log(Level.WARNING, "Worker " + workerId + " interrupted", ie);
                } finally {
                    logger.info(() -> "Worker " + workerId + " finished.");
                }
            }
        }

        // Start workers
        for (int i = 0; i < numWorkers; i++) {
            workers.submit(new Worker(i + 1));
        }

        // Enqueue tasks
        for (int i = 0; i < numTasks; i++) {
            queue.add(new Task("ride-request-" + (i + 1)));
        }

        // Insert a single poison; workers will re-insert it to let all exit
        queue.add(POISON);

        // Shutdown and await termination
        workers.shutdown();
        try {
            if (!workers.awaitTermination(60, TimeUnit.SECONDS)) {
                logger.warning("Forcing shutdown of worker pool...");
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }

        // Write results to file
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outFile))) {
            for (String r : results) {
                bw.write(r);
                bw.newLine();
            }
            logger.info("Results written to " + outFile);
        } catch (IOException ioe) {
            logger.log(Level.SEVERE, "Failed to write results to file", ioe);
        }

        // Also print a small summary
        logger.info(() -> "Processing complete. Tasks processed: " + results.size());
    }

    // Simulated processing method (could raise an exception)
    private static String processTask(int workerId, Task t) throws Exception {
        // Simulate variable processing time
        long delay = 100 + (long)(Math.random() * 400);
        Thread.sleep(delay);

        // Occasionally simulate an error
        if (Math.random() < 0.05) {
            throw new RuntimeException("Simulated processing error for " + t.id);
        }

        String result = String.format("%s | processedBy=worker-%d | at=%s | payload=%s",
                t.id, workerId, Instant.now().toString(), t.payload);
        logger.fine(() -> "Worker " + workerId + " processed " + t.id);
        return result;
    }
}
