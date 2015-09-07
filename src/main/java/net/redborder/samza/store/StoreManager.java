package net.redborder.samza.store;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;

import java.lang.Object;
import java.lang.String;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class manages a list of key-value store-based enrinchments.
 * Its purpose is to enrich messages with some key-value stores that has been
 * previously populated with messages from other streams.
 * It also manages stores extensions, which are external implementations
 * of stores (f.e. Aerospike).
 */

public class StoreManager {
    // Stores each store instance with a key
    private static Map<String, Store> stores = new LinkedHashMap<>();

    // Stores each extension instance with a key
    private static Map<String, IStoreExtension> extensions = new HashMap<>();

    // The key of the partition
    private String partitionCacheKey;

    /**
     * Constructs a new store manager.
     * It gets the list of stores to use as enrichments from the
     * property "redborder.stores" from the config file.
     *
     * <p>Each store from the list of stores should have a key specified
     * in the config file under the property "redborder.store.storeName.key".
     * This key will be used to enrich messages based on that field from the
     * original message</p>
     *
     * <p>Also, you can set the overwrite status in a store with the property
     * "redborder.store.storeName.overwrite". Stores with the overwrite property
     * set as true will overwrite the value of an enriched field if it is already
     * present. Its default value is true.</p>
     *
     * @param config The task config
     * @param context The task context
     * @param defaultKey The default key in case no key is specified on a store
     */

    public StoreManager(Config config, TaskContext context, String defaultKey) {
        List<String> storesList = config.getList("redborder.stores", Collections.<String>emptyList());

        if (defaultKey == null)
            defaultKey = "";

        for (String store : storesList) {
            Store storeData = new Store();
            storeData.setKey(config.get("redborder.store." + store + ".key", defaultKey));
            storeData.setOverwrite(config.getBoolean("redborder.store." + store + ".overwrite", true));
            storeData.setStore((KeyValueStore<String, Map<String, Object>>) context.getStore(store));
            stores.put(store, storeData);
        }
    }

    /**
     * Constructs a new store manager specifying a partition cache key.
     *
     * @param config The task config
     * @param context The task context
     * @param partitionCacheKey The partition key
     * @param defaultKey The default key in case no key is specified on a store
     * @see #StoreManager(Config, TaskContext, String)
     */

    public StoreManager(Config config, TaskContext context, String partitionCacheKey, String defaultKey) {
        this(config, context, defaultKey);

        if (partitionCacheKey == null)
            partitionCacheKey = "";

        this.partitionCacheKey = partitionCacheKey;
    }

    /**
     * Returns the KeyValueStore associated with the given key
     *
     * @param store The key from the KeyValueStore that will be returned
     * @return A KeyValueStore instance related to the given key
     */

    public KeyValueStore<String, Map<String, Object>> getStore(String store) {
        Store storeData = stores.get(store);
        KeyValueStore<String, Map<String, Object>> keyValueStore = null;

        if (storeData != null) {
            keyValueStore = storeData.getStore();
        }

        return keyValueStore;
    }

    /**
     * @param store The key from the store
     * @return The overwrite status from the store specified
     */

    public boolean hasOverwriteEnabled(String store) {
        Store storeData = stores.get(store);
        boolean overwrite = true;

        if (storeData != null) {
            overwrite = storeData.mustOverwrite();
        }

        return overwrite;
    }

    /**
     * Enriches a given message and returns the result.
     * @param message The message to enrich
     * @return The message given enriched
     */

    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> enrichment = new HashMap<>();
        enrichment.putAll(message);

        for (Map.Entry<String, Store> store : stores.entrySet()) {
            Store storeData = store.getValue();

            String key = (String) message.get(storeData.getKey());
            String namespace_id = message.get(partitionCacheKey) == null ? "" : String.valueOf(message.get(partitionCacheKey));
            KeyValueStore<String, Map<String, Object>> keyValueStore = storeData.getStore();

            Map<String, Object> contents = keyValueStore.get(key + namespace_id);

            if (contents != null) {
                if (storeData.mustOverwrite()) {
                    enrichment.putAll(contents);
                } else {
                    contents.putAll(enrichment);
                    enrichment = contents;
                }
            }
        }

        return enrichment;
    }

    /**
     * Adds an extension to the list of extensions with a given key.
     * @param extensionName The extension key
     * @param extension The extension instance
     */

    public void addExtension(String extensionName, IStoreExtension extension) {
        extensions.put(extensionName, extension);
    }

    /**
     * Returns an extension from its key
     * @param extensionName The extension key
     * @return The extension instance
     */

    public IStoreExtension getExtension(String extensionName){
        return extensions.get(extensionName);
    }

    /**
     * This class stores the relevant data associated with an store.
     */

    private class Store {
        // The store key
        private String key;

        // Should the store's enrichment overwrite fields already present?
        private boolean overwrite;

        // The actual key-value store
        private KeyValueStore<String, Map<String, Object>> store;

        // Getters and setters

        public void setStore(KeyValueStore<String, Map<String, Object>> store) {
            this.store = store;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public void setOverwrite(boolean overwrite) {
            this.overwrite = overwrite;
        }

        public KeyValueStore<String, Map<String, Object>> getStore() {
            return store;
        }

        public String getKey() {
            return key;
        }

        public boolean mustOverwrite() {
            return overwrite;
        }

    }
}
