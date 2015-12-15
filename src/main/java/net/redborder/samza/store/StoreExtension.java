package net.redborder.samza.store;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This abstract class defines the method that a class must implement
 * in order to create a store extension. A store extension is a backend
 * that has the same capabilities than key-value stores from Apache Samza.
 * <p/>
 * Example: Aerospike, Memcached, Redis
 */

public abstract class StoreExtension<T> {
    private String name;
    private boolean overwrite;
    private List<StoreExtensionKey> storeExtensionKeys;
    private static final Logger log = LoggerFactory.getLogger(StoreExtension.class);
    private KeyValueStore<String, Map<String, Object>> localStore;
    private TransformProcess transformProcess;

    public StoreExtension(String name, Config config) {
        this(name, config, null);
    }

    public StoreExtension(String name, Config config, TransformProcess transformProcess) {
        this(name, config, transformProcess, null);
    }

    public StoreExtension(String name, Config config, TransformProcess transformProcess,
                          KeyValueStore<String, Map<String, Object>> localStore) {
        this.name = name;
        this.overwrite = config.getBoolean("redborder.stores.extension." + name + ".overwrite", false);
        this.localStore = localStore;
        this.storeExtensionKeys = new LinkedList<>();
        this.transformProcess = transformProcess;
        List<String> extensionKeys = config.getList("redborder.stores.extension." + name + ".keys", Collections.EMPTY_LIST);


        for (String extensionKey : extensionKeys) {
            String[] keys = extensionKey.split(" ");

            if (keys.length == 3) {
                storeExtensionKeys.add(
                        new StoreExtensionKey.Builder()
                                .namespace(keys[0])
                                .collection(keys[1])
                                .key(keys[2])
                                .transform(false)
                                .build());
            } else if (keys.length == 4) {
                Boolean transform = false;
                if (keys[3].equals("true") || keys[3].equals("false")) {
                    transform = Boolean.parseBoolean(keys[3]);
                }
                storeExtensionKeys.add(
                        new StoreExtensionKey.Builder()
                                .namespace(keys[0])
                                .collection(keys[1])
                                .key(keys[2])
                                .transform(transform)
                                .build());
            } else {
                log.error("You must use store keys like this: namespace collection keyName");
                log.info("If you dont use some of them, You can use some random value.");
            }
        }
    }

    public void put(StoreExtensionKey extensionKey, Map<String, Object> data, Boolean worksWithLocalStore) {
        if (worksWithLocalStore) {
            putOnLocalStore(extensionKey, data);
        }

        put(extensionKey.namespace, extensionKey.collection, extensionKey.key, data.keySet(), data.values());
    }

    public void put(StoreExtensionKey extensionKey, Map<String, Object> data) {
        put(extensionKey, data, false);
    }

    public Map<String, Object> get(StoreExtensionKey extensionKey) {
        return get(extensionKey, false);
    }

    public Map<String, Object> get(StoreExtensionKey extensionKey, Boolean worksWithLocalStore) {
        Map<String, Object> result;

        if (localStore != null) {
            if (worksWithLocalStore) {
                Map<String, Object> localResult = getFromLocalSore(extensionKey);
                if (localResult == null) {
                    result = get(extensionKey.namespace, extensionKey.collection, extensionKey.key);
                } else {
                    result = localResult;
                }

                if (result != null) {
                    putOnLocalStore(extensionKey, result);
                }
            } else {
                result = get(extensionKey.namespace, extensionKey.collection, extensionKey.key);
            }
        } else {
            result = get(extensionKey.namespace, extensionKey.collection, extensionKey.key);

            if (worksWithLocalStore) {
                log.warn("You are trying use worksWithLocalStore but you localStore is disable!! StoreExtension[{}]", name);
            }
        }

        return result;
    }

    public Boolean exist(StoreExtensionKey extensionKey) {
        return exist(extensionKey.namespace, extensionKey.collection, extensionKey.key);
    }

    public boolean mustOverwrite() {
        return overwrite;
    }

    public List<StoreExtensionKey> getExtensionsKeys() {
        return storeExtensionKeys;
    }

    public void putOnLocalStore(StoreExtensionKey storeExtensionKey, Map<String, Object> data) {
        if (localStore != null) {
            localStore.put(storeExtensionKey.mergeKey, data);
        } else {
            log.warn("You are trying use worksWithLocalStore but you localStore is disable!! StoreExtension[{}]", name);
        }
    }

    public Map<String, Object> getFromLocalSore(StoreExtensionKey storeExtensionKey) {
        Map<String, Object> result = null;

        if (localStore != null) {
            result = localStore.get(storeExtensionKey.mergeKey);
        } else {
            log.warn("You are trying use worksWithLocalStore but you localStore is disable!! StoreExtension[{}]", name);
        }

        return result;
    }

    public void removeFromLocalSore(StoreExtensionKey storeExtensionKey) {
        Map<String, Object> result = null;

        if (localStore != null) {
            localStore.delete(storeExtensionKey.mergeKey);
        } else {
            log.warn("You are trying use worksWithLocalStore but you localStore is disable!! StoreExtension[{}]", name);
        }
    }

    public Map<String, Object> transform(Map<String, Object> cacheData) {
        if (cacheData != null && transformProcess != null) {
            return transformProcess.transform(cacheData);
        } else {
            return cacheData;
        }
    }

    /**
     * Adds an element to the store
     *
     * @param namespace  The entry namespace
     * @param collection The entry collection
     * @param key        The entry key
     * @param columns    The values' columns from the entry
     * @param values     The columns' values
     */

    public abstract void put(String namespace, String collection, String key, Set<String> columns, Collection<Object> values);

    /**
     * Removes an element to the store
     *
     * @param namespace  The entry namespace
     * @param collection The entry collection
     * @param key        The entry key
     */

    public abstract void remove(String namespace, String collection, String key);


    public void remove(StoreExtensionKey storeExtensionKey) {
        remove(storeExtensionKey, false);
    }

    public void remove(StoreExtensionKey storeExtensionKey, Boolean worksWithLocalStore) {
        if (worksWithLocalStore) {
            localStore.delete(storeExtensionKey.mergeKey);
        }

        remove(storeExtensionKey.namespace, storeExtensionKey.collection, storeExtensionKey.key);
    }

    /**
     * Gets a collection of values associated with a key
     *
     * @param namespace  The entry namespace
     * @param collection The entry collection
     * @param key        The entry key
     * @return A map where each entry is a column with its value
     */

    public abstract Map<String, Object> get(String namespace, String collection, String key);

    /**
     * Check if a record exist
     *
     * @param namespace  The entry namespace
     * @param collection The entry collection
     * @param key        The entry key
     * @return A boolean
     */

    public abstract Boolean exist(String namespace, String collection, String key);

    /**
     * Increments the columns given with the values given
     *
     * @param namespace  The entry namespace
     * @param collection The entry collection
     * @param key        The entry key
     * @param columns    The values' columns from the entry
     * @param values     The quantity of increments for each column
     */

    public abstract void increment(String namespace, String collection, String key, Set<String> columns, Collection<Integer> values);

    /**
     * Decrements the columns given with the values given
     *
     * @param namespace  The entry namespace
     * @param collection The entry collection
     * @param key        The entry key
     * @param columns    The values' columns from the entry
     * @param values     The quantity of decrements for each column
     */

    public abstract void decrement(String namespace, String collection, String key, Set<String> columns, Collection<Integer> values);

    /**
     * @return An object representing the client connection
     */

    public abstract T getClient();
}
