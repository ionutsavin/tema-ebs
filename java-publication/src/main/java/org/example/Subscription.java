package org.example;

import java.util.*;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.Random;

public class Subscription implements Comparable<Subscription> {

    public record Constraint(String field, String operator, Object value) {
    }

    private final List<Constraint> constraints;

    public Subscription() {
        this.constraints = new ArrayList<>();
    }

    public void addConstraint(String field, String operator, Object value) {
        this.constraints.add(new Constraint(field, operator, value));
    }

    public int numConstraints() {
        return constraints.size();
    }

    public Set<String> getUsedFields() {
        Set<String> fields = new HashSet<>();
        for (Constraint c : constraints)
            fields.add(c.field);
        return fields;
    }

    @Override
    public int compareTo(Subscription other) {
        return Integer.compare(this.numConstraints(), other.numConstraints());
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(";", "{", "}");
        for (Constraint c : constraints) {
            joiner.add("(" + c.field + "," + c.operator + "," + Publication.stringify(c.value) + ")");
        }
        return joiner.toString();
    }

    public static String chooseOperator(Config config, String field, Map<String, Integer> equalityLeft, Random rnd) {
        int left = equalityLeft.getOrDefault(field, 0);
        FieldStructure fs = config.getFieldStructure().get(field);
        if (left > 0 && fs.hasOperator("=")) {
            equalityLeft.put(field, left - 1);
            return "=";
        }
        return fs.getRandomOperator(rnd);
    }

    public static List<Subscription> generateForSlice(Config config, ThreadSlice slice, Random rnd) {
        int subsCount = slice.getSubscriptionsCount();
        List<Subscription> subs = new ArrayList<>(subsCount);
        for (int i = 0; i < subsCount; i++)
            subs.add(new Subscription());

        Map<String, Integer> quotas = new LinkedHashMap<>(slice.getFieldQuotas());
        Map<String, Integer> eqLeft = new LinkedHashMap<>(slice.getEqualityQuotas());

        PriorityQueue<Integer> pq = new PriorityQueue<>(Comparator.comparingInt(i -> subs.get(i).numConstraints()));
        for (int i = 0; i < subsCount; i++)
            pq.offer(i);

        int totalLeft = quotas.values().stream().mapToInt(Integer::intValue).sum();
        int guard = Math.max(1, totalLeft) * 4;

        while (totalLeft > 0 && guard-- > 0) {
            Integer idx = pq.poll();
            if (idx == null)
                break;
            Subscription sub = subs.get(idx);

            String bestField = null;
            int bestCount = Integer.MAX_VALUE;
            Set<String> used = sub.getUsedFields();
            for (Map.Entry<String, Integer> e : quotas.entrySet()) {
                int c = e.getValue();
                if (c <= 0)
                    continue;
                String f = e.getKey();
                if (!used.contains(f) && c < bestCount) {
                    bestCount = c;
                    bestField = f;
                }
            }
            if (bestField == null) {
                for (Map.Entry<String, Integer> e : quotas.entrySet()) {
                    if (e.getValue() > 0) {
                        bestField = e.getKey();
                        break;
                    }
                }
                if (bestField == null) {
                    pq.offer(idx);
                    break;
                }
            }

            String op = chooseOperator(config, bestField, eqLeft, rnd);
            Object val = config.getFieldStructure().get(bestField).generateRandomValue(rnd);
            sub.addConstraint(bestField, op, val);

            quotas.put(bestField, quotas.get(bestField) - 1);
            totalLeft--;

            pq.offer(idx);
        }

        return subs;
    }

    public static List<Subscription> generateAll(Config config) {
        List<ThreadSlice> slices = ThreadSlice.fromConfig(config);
        ExecutorService es = Executors.newFixedThreadPool(config.getNumThreads());
        long start = System.nanoTime();
        try {
            List<Future<List<Subscription>>> futures = new ArrayList<>();
            for (ThreadSlice s : slices) {
                futures.add(es.submit(() -> generateForSlice(config, s, new Random())));
            }
            List<Subscription> all = new ArrayList<>(config.getSubscriptions());
            for (Future<List<Subscription>> f : futures) {
                try {
                    all.addAll(f.get());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            long end = System.nanoTime();
            System.out.printf("Execution time for generating subscriptions: %.4f seconds%n", (end - start) / 1e9);
            return all;
        } finally {
            es.shutdownNow();
        }
    }

    public static List<String> toStrings(List<Subscription> subs) {
        return subs.stream().map(Subscription::toString).collect(Collectors.toList());
    }
}
