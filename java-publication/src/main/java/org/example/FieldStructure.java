package org.example;

import java.util.List;
import java.util.Random;

import lombok.Getter;

public class FieldStructure {
    @Getter
    private final boolean isInterval;
    @Getter
    private final List<Object> values;
    @Getter
    private final List<String> operators;

    public FieldStructure(boolean isInterval, List<Object> values, List<String> operators) {
        this.isInterval = isInterval;
        this.values = values;
        this.operators = operators;
    }

    public boolean hasOperator(String op) {
        return operators.contains(op);
    }

    public Object generateRandomValue(Random random) {
        if (isInterval) {
            double low = ((Number) values.get(0)).doubleValue();
            double high = ((Number) values.get(1)).doubleValue();
            double result = low + (high - low) * random.nextDouble();
            return Math.round(result * 100.0) / 100.0;
        } else {
            return values.get(random.nextInt(values.size()));
        }
    }

    public String getRandomOperator(Random random) {
        return operators.get(random.nextInt(operators.size()));
    }
}
