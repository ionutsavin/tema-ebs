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

    public static Config fromJson(Path inputFile) throws IOException {
        Config config = new Config();
        config.load(inputFile);
        return config;
    }

    private void load(Path inputFile) throws IOException {
        String content = Files.readString(inputFile);
        JSONObject json = new JSONObject(content);
        JSONObject structure = json.getJSONObject("structure");

        this.numThreads = json.optInt("numThreads", 4);
        this.publications = json.getInt("publications");
        this.subscriptions = json.getInt("subscriptions");

        if (json.has("fieldWeights")) {
            JSONObject fw = json.getJSONObject("fieldWeights");
            for (String key : fw.keySet()) {
                this.fieldWeights.put(key, fw.getDouble(key));
            }
        }
        if (json.has("equalityWeights")) {
            JSONObject ew = json.getJSONObject("equalityWeights");
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
            if (!this.preciseFieldNumber.containsKey(field))
                continue;
            int count = (int) Math.ceil(this.preciseFieldNumber.get(field) * this.equalityWeights.get(field));
            this.preciseEqualityNumber.put(field, count);
        }
    }
}
