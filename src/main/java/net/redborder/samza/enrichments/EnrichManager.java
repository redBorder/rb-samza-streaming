package net.redborder.samza.enrichments;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class manages the different local enrichments.
 * It stores references to a list of IEnrich classes that will enrich messages.
 * An enrichment is a class that implements the IEnrich interface. Theses
 * classes enrich messages with local information (without K/V stores), like
 * file databases, or connections with third-party services like a remote DB.
 */

public class EnrichManager {
    // The list of available enrichments
    List<IEnrich> enrichments;

    // Constructs a new enrichment manager
    public EnrichManager() {
        enrichments = new ArrayList<>();
    }

    /**
     * Adds a new enrichment to the manager
     * @param enrich The enrichment to be added
     */

    public void addEnrichment(IEnrich enrich) {
        enrichments.add(enrich);
    }

    /**
     * Enriches a given message.
     * This method calls the #enrich method from every stored IEnrich
     * class that were added with the #addEnrichment method and returns
     * the enriched message.
     * @param message The message to enrich
     * @return The given message enriched
     */

    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> enrichments = new HashMap<>();
        enrichments.putAll(message);

        for (IEnrich enrich : this.enrichments) {
            enrichments.putAll(enrich.enrich(message));
        }

        return enrichments;
    }
}
