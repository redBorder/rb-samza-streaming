package net.redborder.samza.enrichments;

import net.redborder.samza.store.StoreManager;

import java.util.Map;

/**
 * This class defines an interface that can be implemented by classes
 * to create an Enrichment class. An enrichment class implements a method
 * #enrich that, given a message, enriches it with some information, modifying or
 * adding new fields to the message, and returns it.
 */

public abstract class Enrich {
    StoreManager storeManager;

    public Enrich(StoreManager storeManager) {
        this.storeManager = storeManager;
    }

    /**
     * Enriches a given message.
     * This method modify or adds a new field (or fields) to the given message.
     *
     * @param message The message to enrich
     * @return The given message enriched
     */

    public abstract Map<String, Object> enrich(Map<String, Object> message);


}
