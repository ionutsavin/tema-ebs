package org.example;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;


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
}
