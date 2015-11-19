package net.redborder.samza.enrichments;

import net.redborder.samza.store.StoreManager;

import java.util.HashMap;
import java.util.Map;

public class CachedEnrich extends Enrich{
    private Map<String, Object> cache = new HashMap<>();

    public CachedEnrich(StoreManager storeManager) {
        super(storeManager);
        cache.put("AAA", 1);
        cache.put("BBB", 2);
    }

    @Override
    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> result = new HashMap<>();
        result.putAll(message);

        String key = (String) message.get("KEY");

        if(key != null && cache.containsKey(key)){
            result.put("VALUE", cache.get(key));
        }
        return result;
    }
}
