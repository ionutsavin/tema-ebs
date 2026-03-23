package org.example;

import java.util.*;
import java.util.StringJoiner;


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

    public int size() { return constraints.size(); }

    public Set<String> getUsedFields() {
        Set<String> fields = new HashSet<>();
        for (Constraint c : constraints) fields.add(c.field);
        return fields;
    }

    @Override
    public int compareTo(Subscription other) {
        return Integer.compare(this.size(), other.size());
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(";", "{", "}");
        for (Constraint c : constraints) {
            joiner.add("(" + c.field + "," + c.operator + "," + Publication.stringify(c.value) + ")");
        }
        return joiner.toString();
    }
}
