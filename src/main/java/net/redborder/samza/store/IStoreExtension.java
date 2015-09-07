package net.redborder.samza.store;

import java.util.List;
import java.util.Map;

/**
 * This interface defines the method that a class must implement
 * in order to create a store extension. A store extension is a backend
 * that has the same capabilities than key-value stores from Apache Samza.
 */

public interface IStoreExtension {

    /**
     * Adds an element to the store
     *
     * @param namespace The entry namespace
     * @param collection The entry collection
     * @param key The entry key
     * @param columns The values' columns from the entry
     * @param values The columns' values
     */

    void put(String namespace, String collection, String key, List<String> columns, List<Object> values);

    /**
     * Gets a collection of values associated with a key
     *
     * @param namespace The entry namespace
     * @param collection The entry collection
     * @param key The entry key
     * @return A map where each entry is a column with its value
     */

    Map<String, Object> get(String namespace, String collection, String key);

    /**
     * Increments the columns given with the values given
     *
     * @param namespace The entry namespace
     * @param collection  The entry collection
     * @param key The entry key
     * @param columns The values' columns from the entry
     * @param values The quantity of increments for each column
     */

    void increment(String namespace, String collection, String key, List<String> columns, List<Integer> values);

    /**
     * Decrements the columns given with the values given
     *
     * @param namespace The entry namespace
     * @param collection  The entry collection
     * @param key The entry key
     * @param columns The values' columns from the entry
     * @param values The quantity of decrements for each column
     */

    void decrement(String namespace, String collection, String key, List<String> columns, List<Integer> values);

    /**
     * @return An object representing the client connection
     */

    Object getClient();
}
