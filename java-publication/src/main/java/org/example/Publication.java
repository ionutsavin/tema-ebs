package org.example;

import java.util.*;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

// Asigura-te ca ai importat clasa generata de Protobuf
import ebs.PublicationOuterClass;

public class Publication {
    private final LinkedHashMap<String, Object> values = new LinkedHashMap<>();

    // --- Cheile secrete pentru Order-Preserving Encryption ---
    private static final double OPE_MULTIPLIER = 143.77;
    private static final double OPE_SHIFT = 8921.45;

    public void put(String field, Object value) {
        values.put(field, value);
    }

    public Object get(String field) {
        return values.get(field);
    }

    // --- Functii de criptare ---
    public static double opeEncrypt(double value) {
        return (value * OPE_MULTIPLIER) + OPE_SHIFT;
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

    public static void streamToKafka(Config config, KafkaProducerClient kafkaClient, String topic, long durationMinutes) {
        long durationMs = durationMinutes * 60 * 1000;
        long startTime = System.currentTimeMillis();
        Random rnd = new Random();
        List<String> fields = new ArrayList<>(config.getFieldStructure().keySet());

        System.out.println("\n[Publisher] Incepem trimiterea continua de publicatii criptate timp de " + durationMinutes + " minute...");
        long count = 0;

        while (System.currentTimeMillis() - startTime < durationMs) {
            Publication pub = new Publication();
            for (String f : fields) {
                pub.put(f, config.getFieldStructure().get(f).generateRandomValue(rnd));
            }
            long currentTs = System.currentTimeMillis();
            pub.put("_ts", currentTs);

            // Construim Protobuf-ul aplicand criptarea pe loc
            PublicationOuterClass.Publication proto = PublicationOuterClass.Publication.newBuilder()
                    .setCompany(hashText((String) pub.get("company")))
                    .setDate(hashText((String) pub.get("date")))
                    .setValue(opeEncrypt(((Number) pub.get("value")).doubleValue()))
                    .setDrop(opeEncrypt(((Number) pub.get("drop")).doubleValue()))
                    .setVariation(opeEncrypt(((Number) pub.get("variation")).doubleValue()))
                    .setTs(currentTs) // Metadata ramane in clar
                    .build();

            String encoded = Base64.getEncoder().encodeToString(proto.toByteArray());
            kafkaClient.send(topic, encoded);
            count++;

            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            if (count % 10000 == 0) {
                System.out.println("  -> Trimise: " + count + " publicatii...");
            }
        }

        System.out.println("[Publisher] Timpul a expirat. S-au generat si livrat " + count + " publicatii criptate.");
    }
}