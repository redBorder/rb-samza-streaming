package net.redborder.samza.store;


import java.util.Map;

public interface TransformProcess {
    Map<String, Object> transform(Map<String, Object> cacheData);
}
