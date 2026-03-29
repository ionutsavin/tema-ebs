package org.example;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class Main {

    private static final String PUBLICATIONS_OUTPUT_FILE = "publications.txt";
    private static final String SUBSCRIPTIONS_OUTPUT_FILE = "subscriptions.txt";
    private static final String CHECK_OUTPUT_FILE = "check-output.txt";

    public static void main(String[] args) throws Exception {
        Config config = Config.fromJson();

        // Generate publications
        long pStart = System.nanoTime();
        List<String> pubs = Publication.generateAll(config);
        writeLines(Path.of(PUBLICATIONS_OUTPUT_FILE), pubs);
        double pSec = (System.nanoTime() - pStart) / 1e9;
        System.out.printf("\nExecution time for generating publications: %.4f seconds%n", pSec);

        // Generate subscriptions
        long sStart = System.nanoTime();
        List<Subscription> subs = Subscription.generateAll(config);
        List<String> subsStr = Subscription.toStrings(subs);
        writeLines(Path.of(SUBSCRIPTIONS_OUTPUT_FILE), subsStr);
        double sSec = (System.nanoTime() - sStart) / 1e9;
        System.out.printf("\nExecution time for generating subscriptions: %.4f seconds%n", sSec);

        // Validate output
        checkOutput(config, Path.of(PUBLICATIONS_OUTPUT_FILE), Path.of(SUBSCRIPTIONS_OUTPUT_FILE), Path.of(CHECK_OUTPUT_FILE));
    }

    // ---- Utils ----
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

            writer.write("Expected Publications: " + config.getPublications() + "\n");
            writer.write("Generated Publications: " + pubs + "\n");
            writer.write("Expected subscriptions: " + config.getSubscriptions() + "\n");
            writer.write("generated_subscriptions: " + subs + "\n");

            Map<String, Long> genFieldCounts = new LinkedHashMap<>();
            Map<String, Long> genEqCounts = new LinkedHashMap<>();
            countSubscriptions(subsPath, genFieldCounts, genEqCounts);

            for (Map.Entry<String, Integer> e : config.getPreciseFieldNumber().entrySet()) {
                String field = e.getKey();
                long expected = e.getValue();
                long generated = genFieldCounts.getOrDefault(field, 0L);
                writer.write("Field " + field + " expected times : " + expected + "\n");
                writer.write("Field " + field + " generated times : " + generated + " \n");
            }

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
