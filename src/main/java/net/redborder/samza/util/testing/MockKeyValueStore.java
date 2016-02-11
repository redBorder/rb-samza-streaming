package net.redborder.samza.util.testing;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockKeyValueStore implements KeyValueStore<String, Map<String, Object>> {
    Map<String, Map<String, Object>> store = new HashMap<>();

    public boolean isEmpty() {
        return store.isEmpty();
    }

    @Override
    public Map<String, Object> get(String s) {
        return store.get(s);
    }

    @Override
    public void put(String s, Map<String, Object> o) {
        store.put(s, o);
    }

    @Override
    public void putAll(List<Entry<String, Map<String, Object>>> list) {
    }

    @Override
    public Map<String, Map<String, Object>> getAll(List<String> list) {
        Map<String, Map<String, Object>> result = new HashMap<>();

        for(String key : list){
            result.put(key, store.get(key));
        }

        return result;
    }

    @Override
    public void deleteAll(List<String> list) {
        for(String key : list) {
            store.remove(key);
        }
    }

    @Override
    public void delete(String s) {
        store.remove(s);
    }

    @Override
    public KeyValueIterator<String, Map<String, Object>> range(String s, String k1) {
        return null;
    }

    @Override
    public KeyValueIterator<String, Map<String, Object>> all() {
        return null;
    }

    @Override
    public void close() {
        store.clear();
        store = null;
    }

    @Override
    public void flush() {
        store.clear();
    }
}