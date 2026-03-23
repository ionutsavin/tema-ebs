package org.example;
import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.json.JSONObject;

public class Main {

    // ── Config constants ──────────────────────────────────────────────────────
    private static final String PUBLICATIONS_OUTPUT_FILE = "publications.txt";
    private static final String SUBSCRIPTIONS_OUTPUT_FILE = "subscriptions.txt";
    private static final String CHECK_OUTPUT_FILE = "check-output.txt";

    // ── Runtime config (loaded from input.json + CLI args) ────────────────────
    private static int NUM_THREADS;
    private static int PUBLICATIONS;
    private static int SUBSCRIPTIONS;

    private static Map<String, Double> FIELD_WEIGHTS = new HashMap<>();
    private static Map<String, Double> EQUALITY_WEIGHTS = new HashMap<>();
    private static Map<String, FieldStructure> FIELD_STRUCTURE = new LinkedHashMap<>();

    private static Map<String, Integer> preciseFieldNumber = new HashMap<>();
    private static Map<String, Integer> preciseFieldEqualityNumber = new HashMap<>();
    private static final ReentrantLock preciseFieldNumberLock = new ReentrantLock();
    private static final ReentrantLock preciseFieldEqualityNumberLock = new ReentrantLock();
    private static final ReentrantLock publicationFileLock = new ReentrantLock();

    private static FIFOPriorityQueue<Subscription> subscriptions;
    private static final List<List<Object[]>> subscriptionsList = new ArrayList<>();

    private static final Random random = new Random();

    // ── Inner data class ──────────────────────────────────────────────────────
    static class FieldStructure {
        boolean isInterval;
        List<Object> values;
        List<String> operators;

        FieldStructure(boolean isInterval, List<Object> values, List<String> operators) {
            this.isInterval = isInterval;
            this.values = values;
            this.operators = operators;
        }
    }

    // ── Entry point ───────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        // Parse CLI arguments
        NUM_THREADS = parseCLIArgs(args);

        // Load config
        String content = Files.readString(Path.of("input.json"));
        JSONObject config = new JSONObject(content);
        JSONObject numbers = config.getJSONObject("numbers");
        JSONObject structure = config.getJSONObject("structure");

        PUBLICATIONS = numbers.getInt("publications");
        SUBSCRIPTIONS = numbers.getInt("subscriptions");

        // Field weights
        if (numbers.has("field_weights")) {
            JSONObject fw = numbers.getJSONObject("field_weights");
            for (String key : fw.keySet()) FIELD_WEIGHTS.put(key, fw.getDouble(key));
        }

        // Equality weights
        if (numbers.has("equality_weights")) {
            JSONObject ew = numbers.getJSONObject("equality_weights");
            for (String key : ew.keySet()) EQUALITY_WEIGHTS.put(key, ew.getDouble(key));
        }

        // Precise field numbers
        for (String field : FIELD_WEIGHTS.keySet()) {
            int count = (int) Math.round(SUBSCRIPTIONS * FIELD_WEIGHTS.get(field));
            if (count > 0) preciseFieldNumber.put(field, count);
        }

        // Precise equality numbers
        for (String field : EQUALITY_WEIGHTS.keySet()) {
            if (!preciseFieldNumber.containsKey(field)) continue;
            int count = (int) Math.ceil(preciseFieldNumber.get(field) * EQUALITY_WEIGHTS.get(field));
            preciseFieldEqualityNumber.put(field, count);
        }

        // Field structure
        for (String field : structure.keySet()) {
            JSONObject details = structure.getJSONObject(field);
            boolean isInterval = details.getBoolean("is_interval");

            List<Object> values = new ArrayList<>();
            for (Object v : details.getJSONArray("values")) values.add(v);

            List<String> operators = new ArrayList<>();
            for (Object op : details.getJSONArray("operators")) operators.add(op.toString());

            FIELD_STRUCTURE.put(field, new FieldStructure(isInterval, values, operators));
        }

        subscriptions = new FIFOPriorityQueue<>();

        generatePublications();
        generateSubscriptions();
        checkOutput();
    }

    // ── CLI parsing ───────────────────────────────────────────────────────────
    private static int parseCLIArgs(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--threads")) return Integer.parseInt(args[i + 1]);
        }
        throw new IllegalArgumentException("--threads argument is required");
    }

    // ── Random value generation ───────────────────────────────────────────────
    private static Object generateRandomValueForField(String fieldName) {
        FieldStructure fs = FIELD_STRUCTURE.get(fieldName);
        if (fs.isInterval) {
            Object lo = fs.values.get(0);
            Object hi = fs.values.get(1);
            if (fieldName.equals("rain")) {
                double low = ((Number) lo).doubleValue();
                double high = ((Number) hi).doubleValue();
                return low + (high - low) * random.nextDouble();
            } else {
                int low = ((Number) lo).intValue();
                int high = ((Number) hi).intValue();
                return low + random.nextInt(high - low + 1);
            }
        } else {
            return fs.values.get(random.nextInt(fs.values.size()));
        }
    }

    // ── Publication generation ────────────────────────────────────────────────
    private static void generatePublication(int numPublications) {
        List<List<Map<String, Object>>> bulk = new ArrayList<>();
        for (int i = 0; i < numPublications; i++) {
            List<Map<String, Object>> pub = new ArrayList<>();
            for (String fieldName : FIELD_STRUCTURE.keySet()) {
                Map<String, Object> entry = new HashMap<>();
                entry.put(fieldName, generateRandomValueForField(fieldName));
                pub.add(entry);
            }
            bulk.add(pub);
        }
        writeOutput(bulk.stream().map(Object::toString).collect(Collectors.toList()),
                PUBLICATIONS_OUTPUT_FILE, "a");
    }

    private static void generatePublications() throws Exception {
        long start = System.nanoTime();

        try { Files.deleteIfExists(Path.of(PUBLICATIONS_OUTPUT_FILE)); } catch (IOException ignored) {}

        int perTask = PUBLICATIONS / NUM_THREADS;
        int remainder = PUBLICATIONS % NUM_THREADS;

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < NUM_THREADS; i++) {
                int count = (i == NUM_THREADS - 1) ? perTask + remainder : perTask;
                futures.add(executor.submit(() -> generatePublication(count)));
            }
            for (Future<?> f : futures) f.get(); // wait for all
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException ie) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        double elapsed = (System.nanoTime() - start) / 1e9;
        System.out.printf("%nExecution time for generating publications: %.4f seconds%n", elapsed);
    }

    // ── Subscription generation ───────────────────────────────────────────────
    private static void addFieldToSubscription(int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            Subscription current = subscriptions.pop();

            String minField;
            preciseFieldNumberLock.lock();
            try {
                Set<String> availableFields = new HashSet<>(preciseFieldNumber.keySet());
                availableFields.removeAll(current.getUsedFields());

                minField = availableFields.stream()
                        .min(Comparator.comparingInt(preciseFieldNumber::get))
                        .orElseThrow();

                if (preciseFieldNumber.get(minField) == 1) {
                    preciseFieldNumber.remove(minField);
                } else {
                    preciseFieldNumber.merge(minField, -1, Integer::sum);
                }
            } finally {
                preciseFieldNumberLock.unlock();
            }

            String operator;
            preciseFieldEqualityNumberLock.lock();
            try {
                if (preciseFieldEqualityNumber.containsKey(minField)) {
                    operator = "=";
                    if (preciseFieldEqualityNumber.get(minField) == 1) {
                        preciseFieldEqualityNumber.remove(minField);
                    } else {
                        preciseFieldEqualityNumber.merge(minField, -1, Integer::sum);
                    }
                } else {
                    List<String> ops = FIELD_STRUCTURE.get(minField).operators;
                    operator = ops.get(random.nextInt(ops.size()));
                }
            } finally {
                preciseFieldEqualityNumberLock.unlock();
            }

            current.addValue(minField, operator, generateRandomValueForField(minField));
            subscriptions.push(current, current.getLength());
        }
    }

    private static void generateSubscriptions() throws Exception {
        long start = System.nanoTime();

        for (int i = 0; i < SUBSCRIPTIONS; i++) {
            subscriptions.push(new Subscription(), 0);
        }

        int totalFields = preciseFieldNumber.values().stream().mapToInt(Integer::intValue).sum();
        int perTask = totalFields / NUM_THREADS;
        int remainder = totalFields % NUM_THREADS;

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < NUM_THREADS; i++) {
                int count = (i == NUM_THREADS - 1) ? perTask + remainder : perTask;
                futures.add(executor.submit(() -> addFieldToSubscription(count)));
            }
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    System.err.println("Exception in thread: " + e.getCause());
                }
            }
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException ie) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        while (!subscriptions.isEmpty()) {
            subscriptionsList.add(subscriptions.pop().getValues());
        }

        writeOutput(
                subscriptionsList.stream().map(Object::toString).collect(Collectors.toList()),
                SUBSCRIPTIONS_OUTPUT_FILE, "w");

        double elapsed = (System.nanoTime() - start) / 1e9;
        System.out.printf("%nExecution time for generating subscriptions: %.4f seconds%n", elapsed);
    }

    // ── File I/O ──────────────────────────────────────────────────────────────
    private static void writeOutput(List<String> messages, String filePath, String mode) {
        boolean append = mode.equals("a");

        if (filePath.equals(PUBLICATIONS_OUTPUT_FILE)) {
            publicationFileLock.lock();
            try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter(filePath, append))) {
                for (String msg : messages) writer.write(msg + "\n");
            } catch (IOException e) {
                System.err.println("Error writing to file " + filePath + ": " + e.getMessage());
            } finally {
                publicationFileLock.unlock();
            }
        } else {
            try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter(filePath, append))) {
                for (String msg : messages) writer.write(msg + "\n");
            } catch (IOException e) {
                System.err.println("Error writing to file " + filePath + ": " + e.getMessage());
            }
        }
    }

    // ── Output verification ───────────────────────────────────────────────────
    private static void checkOutput() throws IOException {
        // Re-derive expected counts
        Map<String, Integer> expectedFieldNumber = new HashMap<>();
        Map<String, Integer> expectedEqualityNumber = new HashMap<>();

        for (String field : FIELD_WEIGHTS.keySet())
            expectedFieldNumber.put(field, (int) Math.round(SUBSCRIPTIONS * FIELD_WEIGHTS.get(field)));
        for (String field : EQUALITY_WEIGHTS.keySet())
            expectedEqualityNumber.put(field,
                    (int) Math.ceil(expectedFieldNumber.get(field) * EQUALITY_WEIGHTS.get(field)));

        // Count publications
        long generatedPublications = Files.lines(Path.of(PUBLICATIONS_OUTPUT_FILE)).count();

        // Count subscriptions and field occurrences
        Map<String, Integer> restrictedFields = new HashMap<>();
        Map<String, Integer> restrictedEqualityFields = new HashMap<>();
        for (String k : expectedFieldNumber.keySet()) restrictedFields.put(k, 0);
        for (String k : expectedEqualityNumber.keySet()) restrictedEqualityFields.put(k, 0);

        long generatedSubscriptions = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(SUBSCRIPTIONS_OUTPUT_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                generatedSubscriptions++;
                // Simple parse: extract (field, operator, value) tuples from the toString format
                // Expected format: [[field, op, val], [field, op, val], ...]
                String[] tokens = line.replaceAll("[\\[\\]()']", "").split(",\\s*");
                for (int i = 0; i + 2 < tokens.length; i += 3) {
                    String key = tokens[i].trim();
                    String op = tokens[i + 1].trim();
                    if (restrictedFields.containsKey(key))
                        restrictedFields.merge(key, 1, Integer::sum);
                    if (restrictedEqualityFields.containsKey(key) && op.equals("="))
                        restrictedEqualityFields.merge(key, 1, Integer::sum);
                }
            }
        }

        // Write report
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(CHECK_OUTPUT_FILE))) {
            writer.write("Expected Publications: " + PUBLICATIONS + "\n");
            writer.write("Generated Publications: " + generatedPublications + "\n");
            writer.write("Expected subscriptions: " + SUBSCRIPTIONS + "\n");
            writer.write("generated_subscriptions: " + generatedSubscriptions + "\n");
            for (String key : restrictedFields.keySet()) {
                writer.write("Field " + key + " expected times : " + expectedFieldNumber.get(key) + "\n");
                writer.write("Field " + key + " generated times : " + restrictedFields.get(key) + "\n");
            }
            for (String key : restrictedEqualityFields.keySet()) {
                writer.write("Field " + key + " expected at least equality times : " + expectedEqualityNumber.get(key) + "\n");
                writer.write("Field " + key + " generated equality times : " + restrictedEqualityFields.get(key) + "\n");
            }
        }
    }
}