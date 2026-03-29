package org.example;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;

@Getter
public class ThreadSlice {

    private final int publicationsCount;
    private final int subscriptionsCount;
    private final Map<String, Integer> fieldQuotas;
    private final Map<String, Integer> equalityQuotas;

    public ThreadSlice(int publicationsCount, int subscriptionsCount, Map<String, Integer> fieldQuotas,
                       Map<String, Integer> equalityQuotas) {
        this.publicationsCount = publicationsCount;
        this.subscriptionsCount = subscriptionsCount;
        this.fieldQuotas = new LinkedHashMap<>(fieldQuotas);
        this.equalityQuotas = new LinkedHashMap<>(equalityQuotas);
    }

    public static List<ThreadSlice> fromConfig(Config config) {
        int T = config.getNumThreads();
        List<ThreadSlice> slices = new ArrayList<>(T);

        int pubsPer = config.getPublications() / T;
        int pubsRem = config.getPublications() % T;
        int subsPer = config.getSubscriptions() / T;
        int subsRem = config.getSubscriptions() % T;

        List<Map<String, Integer>> fieldMaps = new ArrayList<>(T);
        List<Map<String, Integer>> eqMaps = new ArrayList<>(T);
        for (int i = 0; i < T; i++) {
            fieldMaps.add(new LinkedHashMap<>());
            eqMaps.add(new LinkedHashMap<>());
            for (String k : config.getPreciseFieldNumber().keySet()) fieldMaps.get(i).put(k, 0);
            for (String k : config.getPreciseEqualityNumber().keySet()) eqMaps.get(i).put(k, 0);
        }

        for (Map.Entry<String, Integer> e : config.getPreciseFieldNumber().entrySet()) {
            String k = e.getKey();
            int total = e.getValue();
            int base = total / T;
            int rem = total % T;
            for (int i = 0; i < T; i++) {
                fieldMaps.get(i).put(k, fieldMaps.get(i).get(k) + base + (i < rem ? 1 : 0));
            }
        }
        for (Map.Entry<String, Integer> e : config.getPreciseEqualityNumber().entrySet()) {
            String k = e.getKey();
            int total = e.getValue();
            int base = total / T;
            int rem = total % T;
            for (int i = 0; i < T; i++) {
                eqMaps.get(i).put(k, eqMaps.get(i).get(k) + base + (i < rem ? 1 : 0));
            }
        }

        for (int i = 0; i < T; i++) {
            int pubs = pubsPer + (i < pubsRem ? 1 : 0);
            int subs = subsPer + (i < subsRem ? 1 : 0);
            slices.add(new ThreadSlice(pubs, subs, fieldMaps.get(i), eqMaps.get(i)));
        }
        return slices;
    }
}
