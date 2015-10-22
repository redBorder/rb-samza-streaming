package net.redborder.samza.store;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Object;
import java.lang.String;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StoreManager {

    private static Map<String, Store> stores = new LinkedHashMap<>();
    private static Map<String, IStoreExtension> extensions = new HashMap<>();
    private String partitionCacheKey;
    private static final Logger log = LoggerFactory.getLogger(StoreManager.class);
    private List<String> storesList;

    public StoreManager(Config config, TaskContext context, String defaultKey) {
        storesList = config.getList("redborder.stores", Collections.<String>emptyList());

        if (defaultKey == null)
            defaultKey = "";

        log.info("Making stores: ");
        for (String store : storesList) {
            if (!stores.containsKey(store)) {
                Store storeData = new Store();
                storeData.setKey(config.get("redborder.store." + store + ".key", defaultKey));
                storeData.setOverwrite(config.getBoolean("redborder.store." + store + ".overwrite", true));
                storeData.setStore((KeyValueStore<String, Map<String, Object>>) context.getStore(store));
                log.info("  * Store: {} {}", store, storeData.toString());
                stores.put(store, storeData);
            }
        }
    }

    public StoreManager(Config config, TaskContext context, String partitionCacheKey, String defaultKey) {
        this(config, context, defaultKey);

        if (partitionCacheKey == null)
            partitionCacheKey = "";

        this.partitionCacheKey = partitionCacheKey;
    }

    public KeyValueStore<String, Map<String, Object>> getStore(String store) {
        Store storeData = stores.get(store);
        KeyValueStore<String, Map<String, Object>> keyValueStore = null;

        if (storeData != null) {
            keyValueStore = storeData.getStore();
        }

        return keyValueStore;
    }

    public boolean hasOverwriteEnabled(String store) {
        Store storeData = stores.get(store);
        boolean overwrite = true;

        if (storeData != null) {
            overwrite = storeData.mustOverwrite();
        }

        return overwrite;
    }

    public Map<String, Object> enrich(Map<String, Object> message){
        return enrich(message, null);
    }

    public Map<String, Object> enrich(Map<String, Object> message, List<String> useStores) {
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

            if(storeData != null) {
                String key = (String) message.get(storeData.getKey());
                String namespace_id = message.get(partitionCacheKey) == null ? "" : String.valueOf(message.get(partitionCacheKey));
                KeyValueStore<String, Map<String, Object>> keyValueStore = storeData.getStore();

                Map<String, Object> contents = keyValueStore.get(key + namespace_id);

                log.debug(store + "  client: {} - namesapce: {} - contents: " + contents, key, namespace_id);

                if (contents != null) {
                    if (storeData.mustOverwrite()) {
                        enrichment.putAll(contents);
                    } else {
                        Map<String, Object> newData = new HashMap<>();
                        newData.putAll(contents);
                        newData.putAll(enrichment);
                        enrichment = newData;
                    }
                }
            } else {
                log.warn("The store [{}] isn't a available store!!!", store);
            }
        }

        return enrichment;
    }


    public void addExtension(String extensionName, IStoreExtension extension) {
        extensions.put(extensionName, extension);
    }

    public IStoreExtension getExtension(String extensionName) {
        return extensions.get(extensionName);
    }

    private class Store {
        private String key;
        private boolean overwrite;
        private KeyValueStore<String, Map<String, Object>> store;

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

        @Override
        public String toString() {
            return new StringBuffer()
                    .append("KEY: ").append(key).append(" ")
                    .append("OVERWRITE: ").append(overwrite).toString();
        }
    }
}
