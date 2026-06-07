package org.example;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Main {

    private static final String DEFAULT_CONFIG_FILE = "input.json";

    public static void main(String[] args) throws Exception {
        String configFile = DEFAULT_CONFIG_FILE;
        String outputDir = ".";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-c", "--config" -> configFile = args[++i];
                case "-o", "--output-dir" -> outputDir = args[++i];
            }
        }

        Config config = Config.fromJson(Path.of(configFile));
        Path subsPath = Path.of(outputDir, "subscriptions.txt");
        Path pubsPath = Path.of(outputDir, "publications.txt");
        OutputHandler handler = new OutputHandler(pubsPath, subsPath, Path.of(outputDir, "check-output.txt"));

        System.out.println("Generating subscriptions...");
        long sStart = System.nanoTime();
        List<String> subsStr = Subscription.toStrings(Subscription.generateAll(config));
        handler.writeSubscriptions(subsStr);
        double sSec = (System.nanoTime() - sStart) / 1e9;
        System.out.printf("Wrote %d subscriptions to %s in %.4f seconds.%n",
                config.getSubscriptions(), subsPath, sSec);

        // generate only as many as would be sent in 3 min at 2ms each
        int pubCount = (int) (3 * 60 * 1000 / 2.0);
        System.out.println("Generating " + pubCount + " publications...");
        long pStart = System.nanoTime();
        List<String> pubsStr = Publication.generateForSlice(config,
                new ThreadSlice(pubCount, 0, Map.of(), Map.of()), new Random());
        handler.writePublications(pubsStr);
        double pSec = (System.nanoTime() - pStart) / 1e9;
        System.out.printf("Wrote %d publications to %s in %.4f seconds.%n",
                pubsStr.size(), pubsPath, pSec);
    }
}
