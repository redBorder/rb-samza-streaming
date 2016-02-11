package net.redborder.samza.store;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Object;
import java.lang.String;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

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
    private static Map<String, WindowStore> windowStores = new LinkedHashMap<>();

    // Stores each extension instance with a key
    private static Map<String, StoreExtension> extensionsHash = new LinkedHashMap<>();

    private static final Logger log = LoggerFactory.getLogger(StoreManager.class);
    private List<String> storesList;

    /**
     * Constructs a new store manager.
     * It gets the list of stores to use as enrichments from the
     * property "redborder.stores" from the config file.
     * <p/>
     * <p>Each store from the list of stores should have a key specified
     * in the config file under the property "redborder.stores.storeName.key".
     * This key will be used to enrich messages based on that field from the
     * original message</p>
     * <p/>
     * <p>Also, you can set the overwrite status in a store with the property
     * "redborder.stores.storeName.overwrite". Stores with the overwrite property
     * set as true will overwrite the value of an enriched field if it is already
     * present. Its default value is true.</p>
     *
     * @param config  The task config
     * @param context The task context
     */

    public StoreManager(Config config, TaskContext context) {
        initStores(config, context);
        initWindowStores(config);
        initStoresExtensions(config, context);
    }

    private void initStores(Config config, TaskContext context) {
        storesList = config.getList("redborder.stores", Collections.<String>emptyList());

        log.info("Making stores: ");
        for (String store : storesList) {
            if (!stores.containsKey(store)) {
                Store storeData = new Store();
                storeData.setKeys(config.getList("redborder.stores." + store + ".keys", Collections.singletonList("default_key")));
                storeData.setOverwrite(config.getBoolean("redborder.stores." + store + ".overwrite", true));
                storeData.setStore((KeyValueStore<String, Map<String, Object>>) context.getStore(store));
                String className = config.get("redborder.stores." + store + ".transform.class");
                if (className != null && !className.equals("")) {
                    try {
                        Class foundClass = Class.forName(className);
                        TransformProcess transformProcess = (TransformProcess) foundClass.newInstance();
                        storeData.setTransformProcess(transformProcess);
                    } catch (InstantiationException | IllegalAccessException e) {
                        log.error("Couldn't create the instance associated with the TransformProcess " + className, e);
                    } catch (ClassNotFoundException e) {
                        log.error("Couldn't find the class associated with the TransformProcess " + className, e);
                    }
                }

                log.info("  * Store: {} {}", store, storeData.toString());
                stores.put(store, storeData);
            }
        }
    }

    private void initStoresExtensions(Config config, TaskContext context) {
        List<String> extensionsNames = config.getList("redborder.stores.extensions", Collections.<String>emptyList());

        log.info("Making stores extensions: ");
        for (String extensionName : extensionsNames) {
            if (!extensionsHash.containsKey(extensionName)) {
                try {
                    String transformClassName = config.get("redborder.stores.extension" + extensionName + ".transform.class");
                    TransformProcess transformProcess = null;
                    if (transformClassName != null && !transformClassName.equals("")) {
                        try {
                            Class transformClass = Class.forName(transformClassName);
                            transformProcess = (TransformProcess) transformClass.newInstance();
                        } catch (InstantiationException | IllegalAccessException e) {
                            log.error("Couldn't create the instance associated with the TransformProcess " + transformClassName, e);
                        } catch (ClassNotFoundException e) {
                            log.error("Couldn't find the class associated with the TransformProcess " + transformClassName, e);
                        }
                    }

                    String className = config.get("redborder.stores.extension." + extensionName + ".class");
                    if (className != null) {
                        Boolean useLocalStore = config.getBoolean("redborder.stores.extension." + extensionName + ".useLocalStore", false);
                        Class foundClass = Class.forName(className);
                        StoreExtension extension;

                        if (useLocalStore) {
                            KeyValueStore<String, Map<String, Object>> keyValueStore = (KeyValueStore<String, Map<String, Object>>) context.getStore(extensionName);
                            Constructor constructor = foundClass.getConstructor(String.class, Config.class, TransformProcess.class, KeyValueStore.class);
                            extension = (StoreExtension) constructor.newInstance(extensionName, config, transformProcess, keyValueStore);
                        } else {
                            Constructor constructor = foundClass.getConstructor(String.class, Config.class, TransformProcess.class);
                            extension = (StoreExtension) constructor.newInstance(extensionName, config, transformProcess);
                        }

                        extensionsHash.put(extensionName, extension);
                        log.info("  * Store Extension: {} {}", extensionName, extension.getExtensionsKeys());
                    } else {
                        log.warn("Extension {} is declared, but doesn't have associated class. Don't create it!", extensionName);
                    }
                } catch (ClassNotFoundException e) {
                    log.error("Couldn't find the class associated with the extension " + extensionName, e);
                } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                    log.error("Couldn't create the instance associated with the extension " + extensionName, e);
                }
            }
        }
    }

    private void initWindowStores(Config config) {
        List<String> windowStoresNames = config.getList("redborder.stores.windows", Collections.<String>emptyList());

        log.info("Making stores windows: ");
        for (String windowStoreName : windowStoresNames) {
            try {
                String className = config.get("redborder.stores.window." + windowStoreName + ".class");
                if (className != null) {
                    Class foundClass = Class.forName(className);
                    Constructor constructor = foundClass.getConstructor(String.class, Config.class, KeyValueStore.class);
                    WindowStore windowStore = (WindowStore) constructor.newInstance(windowStoreName, config, getStore(windowStoreName));
                    windowStore.prepare(config);
                    windowStores.put(windowStoreName, windowStore);
                } else {
                    log.warn("WindowStore {} is declared, but doesn't have associated class. Don't create it!", windowStoreName);
                }
                log.info("  * Store Window: {}", windowStoreName);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the WindowStore: " + windowStoreName, e);
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the WindowStore: " + windowStoreName, e);
            }
        }
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
     *
     * @param message The message to enrich
     * @return The message given enriched
     */
    public Map<String, Object> enrichWithLocal(Map<String, Object> message) {
        return enrichWithLocal(message, null);
    }

    /**
     * Enriches a given message and returns the result.
     *
     * @param message   The message to enrich
     * @param useStores List contains the stores's name that you want use it.
     * @return The message given enriched
     */
    public Map<String, Object> enrichWithLocal(Map<String, Object> message, List<String> useStores) {
        Map<String, Object> enrichment = new HashMap<>();
        enrichment.putAll(message);
        List<String> enrichWithStores;

        if (useStores == null) {
            enrichWithStores = storesList;
        } else {
            enrichWithStores = useStores;
        }

        for (String store : enrichWithStores) {
            Store storeData = stores.get(store);
            if (storeData != null) {
                List<String> allKeys = storeData.getKeys();

                for (String allKey : allKeys) {
                    String[] keys = allKey.split(":");
                    StringBuilder builder = new StringBuilder();

                    for (String key : keys) {
                        String kv = (String) enrichment.get(key);
                        if (kv != null) {
                            builder.append(kv);
                        }
                    }

                    String mergeKey = builder.toString();
                    KeyValueStore<String, Map<String, Object>> keyValueStore = storeData.getStore();
                    Map<String, Object> contents = keyValueStore.get(mergeKey);
                    Map<String, Object> transform = storeData.transform(contents);

                    log.debug("Query KV store[{}] key[{}], value[" + contents + "]", store, mergeKey);

                    if (transform != null) {
                        if (storeData.mustOverwrite()) {
                            enrichment.putAll(transform);
                        } else {
                            Map<String, Object> newData = new HashMap<>();
                            newData.putAll(transform);
                            newData.putAll(enrichment);
                            enrichment = newData;
                        }

                        break;
                    }
                }
            } else {
                log.warn("The store [{}] isn't a available store!!!", store);
            }
        }

        return enrichment;
    }

    public Map<String, Object> enrichFull(Map<String, Object> message, List<String> useStoresAndExtensions) {
        Map<String, Object> local = enrichWithLocal(message, useStoresAndExtensions);
        return enrichWithExtensions(local, useStoresAndExtensions);
    }

    public Map<String, Object> enrichFull(Map<String, Object> message) {
        return enrichFull(message, null);
    }

    public Map<String, Object> enrichWithExtensions(Map<String, Object> message) {
        return enrichWithExtensions(message, null);
    }

    public Map<String, Object> enrichWithExtensions(Map<String, Object> message, List<String> useExtensions) {
        Map<String, Object> enrichment = new HashMap<>();
        enrichment.putAll(message);
        List<String> enrichWithExtensions = new ArrayList<>();

        if (useExtensions == null) {
            enrichWithExtensions.addAll(extensionsHash.keySet());
        } else {
            enrichWithExtensions.addAll(useExtensions);
        }

        for (String extensionName : enrichWithExtensions) {
            StoreExtension storeExtension = extensionsHash.get(extensionName);

            if (storeExtension != null) {
                List<StoreExtensionKey> storeExtensionKeys = storeExtension.getExtensionsKeys();

                for (StoreExtensionKey storeExtensionKey : storeExtensionKeys) {
                    Map<String, Object> contents = storeExtension.get(storeExtensionKey);
                    Map<String, Object> transform;

                    if (storeExtensionKey.transform) {
                        transform = storeExtension.transform(contents);
                    } else {
                        transform = contents;
                    }

                    if (transform != null) {
                        if (storeExtension.mustOverwrite()) {
                            enrichment.putAll(transform);
                        } else {
                            Map<String, Object> newData = new HashMap<>();
                            newData.putAll(transform);
                            newData.putAll(enrichment);
                            enrichment = newData;
                        }
                    }
                }
            } else {
                log.warn("The extension [{}] isn't a available extension!!!", extensionName);
            }
        }

        return enrichment;
    }


    /**
     * Returns an extension from its name
     *
     * @param extensionName The extension name
     * @return The extension instance
     */

    public StoreExtension getExtension(String extensionName) {
        return extensionsHash.get(extensionName);
    }

    /**
     * Returns an windowStore from its name
     *
     * @param windowStoreName The window store name
     * @return The windowStore instance
     */

    public WindowStore getWindowStore(String windowStoreName) {
        return windowStores.get(windowStoreName);
    }

    /**
     * Returns all available extensions
     *
     * @return The extension name list
     */

    public List<String> getAvailableExtensions() {
        List<String> extensions = Collections.emptyList();
        extensions.addAll(extensionsHash.keySet());
        return extensions;
    }

    public void refreshWindowStores() {
        for (WindowStore windowStore : windowStores.values()) {
            windowStore.refresh();
        }
    }
}
