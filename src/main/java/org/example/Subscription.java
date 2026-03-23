package org.example;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Subscription implements Comparable<Subscription> {

    private final List<Object[]> values;

    public Subscription() {
        this.values = new ArrayList<>();
    }

    public void addValue(Object field, Object operator, Object value) {
        this.values.add(new Object[]{field, operator, value});
    }

    public int getLength() {
        return values.size();
    }

    public Set<Object> getUsedFields() {
        Set<Object> fields = new HashSet<>();
        for (Object[] value : values) {
            fields.add(value[0]);
        }
        return fields;
    }

    public List<Object[]> getValues() {
        return values;
    }

    @Override
    public int compareTo(Subscription other) {
        return Integer.compare(this.getLength(), other.getLength());
    }

    public boolean isGreaterThan(Subscription other) {
        return this.getLength() > other.getLength();
    }

    public boolean isLessThan(Subscription other) {
        return this.getLength() < other.getLength();
    }
}
