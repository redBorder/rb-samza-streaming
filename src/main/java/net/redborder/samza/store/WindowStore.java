package net.redborder.samza.store;


import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class WindowStore {
    KeyValueStore<String, Map<String, Object>> store;
    private Boolean lastUpdate;

    public WindowStore(String name, Config config, KeyValueStore<String, Map<String, Object>> store){
        this.store = store;
        this.lastUpdate = config.getBoolean("redborder.stores.window." + name + ".lastUpdate", true);
        prepare();
    }

    public Map<String, Object> getData(String key){
        return store.get(key);
    }

    public void refresh(){
        Map<String, Map<String, Object>> allData = update();

        if(lastUpdate) {
            KeyValueIterator<String, Map<String, Object>> iterator = store.all();
            List<String> toRemove = new ArrayList<>();

            while (iterator.hasNext()) {
                Entry<String, Map<String, Object>> entry = iterator.next();
                String key = entry.getKey();

                if (!allData.containsKey(key)) {
                    toRemove.add(key);
                }
            }

            for (String key : toRemove) {
                store.delete(key);
            }
        }

        for(Map.Entry<String, Map<String, Object>> data : allData.entrySet()){
            String key = data.getKey();
            Map<String, Object> value = data.getValue();

            store.put(key, value);
        }
    }

    abstract public void prepare();
    abstract public Map<String, Map<String, Object>> update();
}
