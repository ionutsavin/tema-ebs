package org.example;

import java.nio.file.Path;
import java.util.List;

public class Main {

    private static final String DEFAULT_CONFIG_FILE = "input.json";
    private static final String SUBSCRIPTIONS_FILE = "subscriptions.txt";

    public static void main(String[] args) throws Exception {
        String configFile = DEFAULT_CONFIG_FILE;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-c") || args[i].equals("--config")) {
                configFile = args[++i];
            }
        }

        Config config = Config.fromJson(Path.of(configFile));
        OutputHandler handler = new OutputHandler(null, Path.of(SUBSCRIPTIONS_FILE), Path.of("check-output.txt"));

//        // PASUL 1: Generam subscriptiile si le scriem in fisierul citit de Python
//        System.out.println("Generam subscriptiile...");
//        long sStart = System.nanoTime();
//        List<String> subsStr = Subscription.toStrings(Subscription.generateAll(config));
//        handler.writeSubscriptions(subsStr);
//        double sSec = (System.nanoTime() - sStart) / 1e9;
//        System.out.printf("S-au scris %d subscriptii in fisierul %s in %.4f secunde.%n", config.getSubscriptions(), SUBSCRIPTIONS_FILE, sSec);

        // PASUL 2: Conectarea la Kafka si inceperea testului de 3 minute
        KafkaProducerClient kafkaClient = new KafkaProducerClient("localhost:9092");

        try {
            // Trimitem datele in mod streaming timp de 3 minute
            Publication.streamToKafka(config, kafkaClient, "raw-publications", 3);
        } finally {
            kafkaClient.close();
            System.out.println("Conexiunea cu Kafka a fost inchisa cu succes.");
        }
    }
}
