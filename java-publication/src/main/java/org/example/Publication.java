package org.example;

import java.util.*;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Publication {
    private final LinkedHashMap<String, Object> values = new LinkedHashMap<>();



    public void put(String field, Object value) {
        values.put(field, value);
    }

    public Object get(String field) {
        return values.get(field);
    }



    public static String hashText(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString().substring(0, 16);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(";", "{", "}");
        for (Map.Entry<String, Object> e : values.entrySet()) {
            joiner.add("(" + e.getKey() + "," + stringify(e.getValue()) + ")");
        }
        return joiner.toString();
    }

    static String stringify(Object v) {
        if (v == null)
            return "null";
        if (v instanceof Number || v instanceof Boolean)
            return String.valueOf(v);
        String s = String.valueOf(v);
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    public static List<String> generateForSlice(Config config, ThreadSlice slice, Random rnd) {
        List<String> lines = new ArrayList<>(slice.getPublicationsCount());
        List<String> fields = new ArrayList<>(config.getFieldStructure().keySet());
        for (int i = 0; i < slice.getPublicationsCount(); i++) {
            Publication pub = new Publication();
            for (String f : fields) {
                pub.put(f, config.getFieldStructure().get(f).generateRandomValue(rnd));
            }
            lines.add(pub.toString());
        }
        return lines;
    }

    public static List<String> generateAll(Config config) {
        List<ThreadSlice> slices = ThreadSlice.fromConfig(config);
        ExecutorService es = Executors.newFixedThreadPool(config.getNumThreads());
        long start = System.nanoTime();
        try {
            List<Future<List<String>>> futures = new ArrayList<>();
            for (ThreadSlice s : slices) {
                futures.add(es.submit(() -> generateForSlice(config, s, new Random())));
            }
            List<String> all = new ArrayList<>(config.getPublications());
            for (Future<List<String>> f : futures) {
                try {
                    all.addAll(f.get());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            long end = System.nanoTime();
            System.out.printf("Execution time for generating publications: %.4f seconds%n", (end - start) / 1e9);
            return all;
        } finally {
            es.shutdownNow();
        }
    }


}