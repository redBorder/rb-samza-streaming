package net.redborder.samza.store;

import java.util.List;
import java.util.Map;

public interface IStoreExtension {
    void put(String namespace, String collection, String key, List<String> columns, List<Object> values);
    Map<String, Object> get(String namespace, String collection, String key);
    void increment(String namespace, String collection, String key, List<String> columns, List<Integer> values);
    void decrement(String namespace, String collection, String key, List<String> columns, List<Integer> values);
}
