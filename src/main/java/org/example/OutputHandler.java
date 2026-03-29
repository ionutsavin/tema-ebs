package org.example;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OutputHandler {

    private final Path publicationsPath;
    private final Path subscriptionsPath;
    private final Path checkOutputPath;

    public OutputHandler(Path publicationsPath, Path subscriptionsPath, Path checkOutputPath) {
        this.publicationsPath = publicationsPath;
        this.subscriptionsPath = subscriptionsPath;
        this.checkOutputPath = checkOutputPath;
    }

    public void writePublications(List<String> pubs) throws IOException {
        writeLines(publicationsPath, pubs);
    }

    public void writeSubscriptions(List<String> subs) throws IOException {
        writeLines(subscriptionsPath, subs);
    }

    public void checkOutput(Config config) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(checkOutputPath.toFile()))) {
            long pubs = countLines(publicationsPath);
            long subs = countLines(subscriptionsPath);

            writer.write("Expected Publications: " + config.getPublications() + "\n");
            writer.write("Generated Publications: " + pubs + "\n");
            writer.write("Expected subscriptions: " + config.getSubscriptions() + "\n");
            writer.write("generated_subscriptions: " + subs + "\n");

            Map<String, Long> genFieldCounts = new LinkedHashMap<>();
            Map<String, Long> genEqCounts = new LinkedHashMap<>();
            countSubscriptions(subscriptionsPath, genFieldCounts, genEqCounts);

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
        }
    }

    private void writeLines(Path path, List<String> lines) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path.toFile(), false))) {
            for (String s : lines) {
                bw.write(s);
                bw.newLine();
            }
        }
    }

    public long countLines(Path path) {
        if (path == null) return 0L;
        if (!Files.exists(path)) return 0L;
        try (var lines = Files.lines(path)) {
            return lines.count();
        } catch (IOException e) {
            return 0L;
        }
    }

    private void accumulate(Map<String, Long> map, String key) {
        if (key == null) return;
        map.put(key, map.getOrDefault(key, 0L) + 1L);
    }

    public void countSubscriptions(Path subsPath, Map<String, Long> fieldCounts, Map<String, Long> equalityCounts) {
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
        }
    }
}
