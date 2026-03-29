package org.example;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Main {

    private static final String PUBLICATIONS_OUTPUT_FILE = "publications.txt";
    private static final String SUBSCRIPTIONS_OUTPUT_FILE = "subscriptions.txt";
    private static final String CHECK_OUTPUT_FILE = "check-output.txt";

    public static void main(String[] args) throws Exception {
        Config config = Config.fromJson();

        // Generate publications
        long pStart = System.nanoTime();
        List<String> pubs = generateAllPublications(config);
        writeLines(Path.of(PUBLICATIONS_OUTPUT_FILE), pubs);
        double pSec = (System.nanoTime() - pStart) / 1e9;
        System.out.printf("\nExecution time for generating publications: %.4f seconds%n", pSec);

        // Generate subscriptions
        long sStart = System.nanoTime();
        List<Subscription> subs = generateAllSubscriptions(config);
        List<String> subsStr = subscriptionsToStrings(subs);
        writeLines(Path.of(SUBSCRIPTIONS_OUTPUT_FILE), subsStr);
        double sSec = (System.nanoTime() - sStart) / 1e9;
        System.out.printf("\nExecution time for generating subscriptions: %.4f seconds%n", sSec);

        // Validate output
        checkOutput(config, Path.of(PUBLICATIONS_OUTPUT_FILE), Path.of(SUBSCRIPTIONS_OUTPUT_FILE), Path.of(CHECK_OUTPUT_FILE));
    }

    // ---- Random value generation ----
    private static Object randomValueForField(Config config, String field, Random random) {
        Config.FieldStructure fs = config.getFieldStructure().get(field);
        if (fs == null) throw new IllegalArgumentException("Unknown field: " + field);

        if (fs.isInterval()) {
            double low = ((Number) fs.values().get(0)).doubleValue();
            double high = ((Number) fs.values().get(1)).doubleValue();

            double result = low + (high - low) * random.nextDouble();

            return Math.round(result * 100.0) / 100.0;
        } else {
            List<Object> vals = fs.values();
            return vals.get(random.nextInt(vals.size()));
        }
    }

    // ---- Publications generation ----
    private static List<String> generatePublicationsForSlice(Config config, ThreadSlice slice, Random rnd) {
        List<String> lines = new ArrayList<>(slice.getPublicationsCount());
        List<String> fields = new ArrayList<>(config.getFieldStructure().keySet());
        for (int i = 0; i < slice.getPublicationsCount(); i++) {
            Publication pub = new Publication();
            for (String f : fields) {
                pub.put(f, randomValueForField(config, f, rnd));
            }
            lines.add(pub.toString());
        }
        return lines;
    }

    private static List<String> generateAllPublications(Config config) {
        List<ThreadSlice> slices = ThreadSlice.fromConfig(config);
        ExecutorService es = Executors.newFixedThreadPool(config.getNumThreads());
        try {
            List<Future<List<String>>> futures = new ArrayList<>();
            for (ThreadSlice s : slices) {
                futures.add(es.submit(() -> generatePublicationsForSlice(config, s, new Random())));
            }
            List<String> all = new ArrayList<>(config.getPublications());
            for (Future<List<String>> f : futures) {
                try { all.addAll(f.get()); } catch (Exception e) { throw new RuntimeException(e); }
            }
            return all;
        } finally {
            es.shutdownNow();
        }
    }

    // ---- Subscriptions generation ----
    private static String chooseOperator(Config config, String field, Map<String, Integer> equalityLeft, Random rnd) {
        int left = equalityLeft.getOrDefault(field, 0);
        List<String> ops = config.getFieldStructure().get(field).operators();
        if (left > 0 && ops.contains("=")) {
            equalityLeft.put(field, left - 1);
            return "=";
        }
        return ops.get(rnd.nextInt(ops.size()));
    }

    private static List<Subscription> generateSubscriptionsForSlice(Config config, ThreadSlice slice, Random rnd) {
        int subsCount = slice.getSubscriptionsCount();
        List<Subscription> list = new ArrayList<>(subsCount);
        for (int i = 0; i < subsCount; i++) list.add(new Subscription());

        Map<String, Integer> quotas = new LinkedHashMap<>(slice.getFieldQuotas());
        Map<String, Integer> eqLeft = new LinkedHashMap<>(slice.getEqualityQuotas());

        PriorityQueue<Integer> pq = new PriorityQueue<>(Comparator.comparingInt(i -> list.get(i).size()));
        for (int i = 0; i < subsCount; i++) pq.offer(i);

        int totalLeft = quotas.values().stream().mapToInt(Integer::intValue).sum();
        int guard = Math.max(1, totalLeft) * 4;

        while (totalLeft > 0 && guard-- > 0) {
            Integer idx = pq.poll();
            if (idx == null) break;
            Subscription sub = list.get(idx);

            String bestField = null;
            int bestCount = Integer.MAX_VALUE;
            Set<String> used = sub.getUsedFields();
            for (Map.Entry<String, Integer> e : quotas.entrySet()) {
                int c = e.getValue();
                if (c <= 0) continue;
                String f = e.getKey();
                if (!used.contains(f) && c < bestCount) {
                    bestCount = c;
                    bestField = f;
                }
            }
            if (bestField == null) {
                for (Map.Entry<String, Integer> e : quotas.entrySet()) {
                    if (e.getValue() > 0) { bestField = e.getKey(); break; }
                }
                if (bestField == null) {
                    pq.offer(idx);
                    break;
                }
            }

            String op = chooseOperator(config, bestField, eqLeft, rnd);
            Object val = randomValueForField(config, bestField, rnd);
            sub.addConstraint(bestField, op, val);

            quotas.put(bestField, quotas.get(bestField) - 1);
            totalLeft--;

            pq.offer(idx);
        }

        return list;
    }

    private static List<Subscription> generateAllSubscriptions(Config config) {
        List<ThreadSlice> slices = ThreadSlice.fromConfig(config);
        ExecutorService es = Executors.newFixedThreadPool(config.getNumThreads());
        try {
            List<Future<List<Subscription>>> futures = new ArrayList<>();
            for (ThreadSlice s : slices) {
                futures.add(es.submit(() -> generateSubscriptionsForSlice(config, s, new Random())));
            }
            List<Subscription> all = new ArrayList<>(config.getSubscriptions());
            for (Future<List<Subscription>> f : futures) {
                try { all.addAll(f.get()); } catch (Exception e) { throw new RuntimeException(e); }
            }
            return all;
        } finally {
            es.shutdownNow();
        }
    }

    // ---- Utils ----
    private static List<String> subscriptionsToStrings(List<Subscription> subs) {
        return subs.stream().map(Subscription::toString).collect(Collectors.toList());
    }

    private static void writeLines(Path path, List<String> lines) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path.toFile(), false))) {
            for (String s : lines) {
                bw.write(s);
                bw.newLine();
            }
        }
    }

    // ---- Validation ----
    private static long safeCountLines(Path path) {
        if (path == null) return 0L;
        if (!Files.exists(path)) return 0L;
        try (var lines = Files.lines(path)) {
            return lines.count();
        } catch (IOException e) {
            return 0L;
        }
    }

    private static void accumulate(Map<String, Long> map, String key) {
        if (key == null) return;
        map.put(key, map.getOrDefault(key, 0L) + 1L);
    }

    private static void countSubscriptions(Path subsPath, Map<String, Long> fieldCounts, Map<String, Long> equalityCounts) {
        fieldCounts.clear();
        equalityCounts.clear();
        if (subsPath == null || !Files.exists(subsPath)) return;
        try (BufferedReader br = new BufferedReader(new FileReader(subsPath.toFile()))) {
            String line;
            while ((line = br.readLine()) != null) {
                int from = 0;
                while (true) {
                    int l = line.indexOf('(', from);
                    if (l < 0) break;
                    int c1 = line.indexOf(',', l + 1);
                    if (c1 < 0) break;
                    String field = line.substring(l + 1, c1);
                    int c2 = line.indexOf(',', c1 + 1);
                    if (c2 < 0) break;
                    String op = line.substring(c1 + 1, c2);
                    accumulate(fieldCounts, field);
                    if ("=".equals(op)) accumulate(equalityCounts, field);
                    int r = line.indexOf(')', c2 + 1);
                    if (r < 0) break;
                    from = r + 1;
                }
            }
        } catch (IOException e) {
            // ignore individual line errors; leave counts as-is
        }
    }

    private static void checkOutput(Config config, Path pubsPath, Path subsPath, Path reportPath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(reportPath.toFile()))) {
            long pubs = safeCountLines(pubsPath);
            long subs = safeCountLines(subsPath);

            // Header similar to the requested format
            writer.write("Expected Publications: " + config.getPublications() + "\n");
            writer.write("Generated Publications: " + pubs + "\n");
            writer.write("Expected subscriptions: " + config.getSubscriptions() + "\n");
            writer.write("generated_subscriptions: " + subs + "\n");

            // Count generated occurrences per field and equality usages
            Map<String, Long> genFieldCounts = new LinkedHashMap<>();
            Map<String, Long> genEqCounts = new LinkedHashMap<>();
            countSubscriptions(subsPath, genFieldCounts, genEqCounts);

            // Per-field totals (expected vs generated)
            for (Map.Entry<String, Integer> e : config.getPreciseFieldNumber().entrySet()) {
                String field = e.getKey();
                long expected = e.getValue();
                long generated = genFieldCounts.getOrDefault(field, 0L);
                writer.write("Field " + field + " expected times : " + expected + "\n");
                writer.write("Field " + field + " generated times : " + generated + " \n");
            }

            // Equality counts (expected at least vs generated)
            for (Map.Entry<String, Integer> e : config.getPreciseEqualityNumber().entrySet()) {
                String field = e.getKey();
                long expectedAtLeast = e.getValue();
                long generatedEq = genEqCounts.getOrDefault(field, 0L);
                writer.write("Field " + field + " expected at least  equality times : " + expectedAtLeast + "\n");
                writer.write("Field " + field + " generated equality times : " + generatedEq + " \n");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}