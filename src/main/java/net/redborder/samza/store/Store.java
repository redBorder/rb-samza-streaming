package net.redborder.samza.store;

import org.apache.samza.storage.kv.KeyValueStore;

import java.util.List;
import java.util.Map;

public class Store {
    private List<String> keys;
    private boolean overwrite;
    private KeyValueStore<String, Map<String, Object>> store;
    private TransformProcess transformProcess;

    public void setTransformProcess(TransformProcess transformProcess) {
        this.transformProcess = transformProcess;
    }

    public Map<String, Object> transform(Map<String, Object> cacheData) {
        if (cacheData != null && transformProcess != null) {
            return transformProcess.transform(cacheData);
        } else {
            return cacheData;
        }
    }

    public void setStore(KeyValueStore<String, Map<String, Object>> store) {
        this.store = store;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public KeyValueStore<String, Map<String, Object>> getStore() {
        return store;
    }

    public List<String> getKeys() {
        return keys;
    }

    public boolean mustOverwrite() {
        return overwrite;
    }

    @Override
    public String toString() {
        return new StringBuffer()
                .append("KEYS: ").append(keys).append(" ")
                .append("OVERWRITE: ").append(overwrite).toString();
    }
}
