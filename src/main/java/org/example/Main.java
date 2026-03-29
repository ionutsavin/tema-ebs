package org.example;

import java.nio.file.Path;
import java.util.List;

public class Main {

    private static final String PUBLICATIONS_OUTPUT_FILE = "publications.txt";
    private static final String SUBSCRIPTIONS_OUTPUT_FILE = "subscriptions.txt";
    private static final String CHECK_OUTPUT_FILE = "check-output.txt";

    public static void main(String[] args) throws Exception {
        Config config = Config.fromJson();
        OutputHandler handler = new OutputHandler(
                Path.of(PUBLICATIONS_OUTPUT_FILE),
                Path.of(SUBSCRIPTIONS_OUTPUT_FILE),
                Path.of(CHECK_OUTPUT_FILE));

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
