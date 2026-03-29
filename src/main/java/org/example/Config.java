package org.example;

import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

public class Config {

    @Getter
    @Setter
    private int numThreads = 4;

    @Getter
    @Setter
    private int publications;

    @Getter
    @Setter
    private int subscriptions;

    @Getter
    private final Map<String, Double> fieldWeights = new LinkedHashMap<>();

    @Getter
    private final Map<String, Double> equalityWeights = new LinkedHashMap<>();

    @Getter
    private final Map<String, FieldStructure> fieldStructure = new LinkedHashMap<>();

    @Getter
    private final Map<String, Integer> preciseFieldNumber = new LinkedHashMap<>();

    @Getter
    private final Map<String, Integer> preciseEqualityNumber = new LinkedHashMap<>();

    public record FieldStructure(boolean isInterval, List<Object> values, List<String> operators) {
    }

    private static final String DEFAULT_INPUT_JSON = "input.json";

    private String inputFile;

    public Config() {
        this.inputFile = DEFAULT_INPUT_JSON;
    }

    public Config(String inputFile) {
        this.inputFile = inputFile;
    }

    public static Config fromJson(String inputFile) throws IOException {
        Config config = new Config(inputFile);
        config.load();
        return config;
    }

    public static Config fromJson() throws IOException {
        return fromJson(DEFAULT_INPUT_JSON);
    }

    private void load() throws IOException {
        String content = Files.readString(Path.of(inputFile));
        JSONObject json = new JSONObject(content);
        JSONObject numbers = json.getJSONObject("numbers");
        JSONObject structure = json.getJSONObject("structure");

        this.publications = numbers.getInt("publications");
        this.subscriptions = numbers.getInt("subscriptions");

        if (numbers.has("fieldWeights")) {
            JSONObject fw = numbers.getJSONObject("fieldWeights");
            for (String key : fw.keySet()) {
                this.fieldWeights.put(key, fw.getDouble(key));
            }
        }
        if (numbers.has("equalityWeights")) {
            JSONObject ew = numbers.getJSONObject("equalityWeights");
            for (String key : ew.keySet()) {
                this.equalityWeights.put(key, ew.getDouble(key));
            }
        }

        for (String field : structure.keySet()) {
            JSONObject details = structure.getJSONObject(field);
            boolean isInterval = details.getBoolean("isInterval");

            List<Object> values = new ArrayList<>();
            for (Object v : details.getJSONArray("values")) {
                values.add(v);
            }

            List<String> operators = new ArrayList<>();
            for (Object op : details.getJSONArray("operators")) {
                operators.add(op.toString());
            }

            this.fieldStructure.put(field, new FieldStructure(isInterval, values, operators));
        }

        computeQuotas();
    }

    private void computeQuotas() {
        for (String field : this.fieldWeights.keySet()) {
            int count = (int) Math.round(this.subscriptions * this.fieldWeights.get(field));
            if (count > 0) {
                this.preciseFieldNumber.put(field, count);
            }
        }
        for (String field : this.equalityWeights.keySet()) {
            if (!this.preciseFieldNumber.containsKey(field)) continue;
            int count = (int) Math.ceil(this.preciseFieldNumber.get(field) * this.equalityWeights.get(field));
            this.preciseEqualityNumber.put(field, count);
        }
    }
}
