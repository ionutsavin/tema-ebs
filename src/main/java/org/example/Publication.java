package org.example;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Random;


public class Publication {
    private final LinkedHashMap<String, Object> values = new LinkedHashMap<>();

    public void put(String field, Object value) {
        values.put(field, value);
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
        if (v == null) return "null";
        if (v instanceof Number || v instanceof Boolean) return String.valueOf(v);
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
        try {
            List<Future<List<String>>> futures = new ArrayList<>();
            for (ThreadSlice s : slices) {
                futures.add(es.submit(() -> generateForSlice(config, s, new Random())));
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
}
