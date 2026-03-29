package org.example;

import java.nio.file.Path;
import java.util.List;

public class Main {

    private static final String DEFAULT_PUBLICATIONS_FILE = "publications.txt";
    private static final String DEFAULT_SUBSCRIPTIONS_FILE = "subscriptions.txt";
    private static final String DEFAULT_CHECK_OUTPUT_FILE = "check-output.txt";

    public static void main(String[] args) throws Exception {
        String pubsFile = DEFAULT_PUBLICATIONS_FILE;
        String subsFile = DEFAULT_SUBSCRIPTIONS_FILE;
        String checkFile = DEFAULT_CHECK_OUTPUT_FILE;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p", "--publications" -> pubsFile = args[++i];
                case "-s", "--subscriptions" -> subsFile = args[++i];
                case "-c", "--check" -> checkFile = args[++i];
            }
        }

        Config config = Config.fromJson();
        OutputHandler handler = new OutputHandler(
                Path.of(pubsFile),
                Path.of(subsFile),
                Path.of(checkFile));

        long pStart = System.nanoTime();
        List<String> pubs = Publication.generateAll(config);
        handler.writePublications(pubs);
        double pSec = (System.nanoTime() - pStart) / 1e9;
        System.out.printf("%nExecution time for generating publications: %.4f seconds%n", pSec);

        long sStart = System.nanoTime();
        List<String> subsStr = Subscription.toStrings(Subscription.generateAll(config));
        handler.writeSubscriptions(subsStr);
        double sSec = (System.nanoTime() - sStart) / 1e9;
        System.out.printf("%nExecution time for generating subscriptions: %.4f seconds%n", sSec);

        handler.checkOutput(config);
    }
}
