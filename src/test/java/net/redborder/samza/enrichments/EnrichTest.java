package net.redborder.samza.enrichments;

import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.testing.MockTaskContext;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EnrichTest {
    static StoreManager storeManager;

    @Mock
    static Config config;

    static TaskContext context;
    static Properties properties = new Properties();
    static List<String> stores = new ArrayList<>();

    @BeforeClass
    public static void initTest() throws IOException {
        InputStream inputStream = new FileInputStream("src/test/resources/streaming.properties");
        properties.load(inputStream);

        config = mock(Config.class);
        when(config.getList("redborder.stores", Collections.<String>emptyList())).thenReturn(stores);
        String storesListAsString = properties.getProperty("redborder.stores");

        for (String store : storesListAsString.split(",")) {
            List<String> keys = Arrays.asList(properties.getProperty("redborder.stores." + store + ".keys").split(","));
            String storeOverwriteStr = properties.getProperty("redborder.stores." + store + ".overwrite");
            boolean storeOverwrite = (storeOverwriteStr == null || storeOverwriteStr.equals("true"));

            when(config.getList("redborder.stores." + store + ".keys", Collections.singletonList("key"))).thenReturn(keys);
            when(config.getBoolean("redborder.stores." + store + ".overwrite", true)).thenReturn(storeOverwrite);
            stores.add(store);
        }


        context = new MockTaskContext();
        storeManager = new StoreManager(config, context);
    }

    @Test
    public void basicEnrich() {
        CachedEnrich cachedEnrich = new CachedEnrich(storeManager);

        Map<String, Object> input = new HashMap<>();
        input.put("TIME", 10000L);
        input.put("KEY", "AAA");

        Map<String, Object> result = cachedEnrich.enrich(input);

        Map<String, Object> expected = new HashMap<>();
        expected.putAll(input);
        expected.put("VALUE", 1);

        assertEquals(expected, result);

        input.put("KEY", "BBB");
        result = cachedEnrich.enrich(input);
        expected.putAll(input);
        expected.put("VALUE", 2);
        assertEquals(expected, result);
    }




}
