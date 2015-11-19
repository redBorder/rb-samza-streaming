package net.redborder.samza.processors;

import net.redborder.samza.enrichments.Enrich;
import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public abstract class Processor {
    private static final Logger log = LoggerFactory.getLogger(Processor.class);
    private static Map<String, List<Processor>> processors = new HashMap<>();

    protected StoreManager storeManager;
    protected EnrichManager enrichManager;
    protected Config config;
    protected TaskContext context;

    public Processor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        this.storeManager = storeManager;
        this.enrichManager = enrichManager;
        this.config = config;
        this.context = context;
    }

    public static List<Processor> getProcessors(String streamName, Config config, TaskContext context, StoreManager storeManager) {
        if (!processors.containsKey(streamName)) {
            List<String> processorNames = config.getList("redborder.processors." + streamName);
            List<Processor> processorsList = new ArrayList<>();

            log.info("Asked for processors " + streamName + " but them weren't found. Lets try to create them.");

            for (String processorName : processorNames) {
                List<String> enrichments;
                EnrichManager enrichManager = new EnrichManager();

                try {
                    enrichments = config.getList("redborder.enrichments.processors." + processorName);
                } catch (ConfigException e) {
                    log.info("Processor " + processorName + " does not have enrichments enabled");
                    enrichments = new ArrayList<>();
                }

                for (String enrichment : enrichments) {
                    try {
                        String className = config.get("redborder.enrichments.types." + enrichment + ".class");

                        if (className != null) {
                            Class enrichClass = Class.forName(className);
                            Constructor constructor = enrichClass.getConstructor(StoreManager.class);
                            Enrich enrich = (Enrich)  constructor.newInstance(storeManager);

                            enrichManager.addEnrichment(enrich);
                        } else {
                            log.warn("Couldn't find property redborder.enrichments.types." + enrichment + ".class on config properties");
                        }
                    } catch (ClassNotFoundException e) {
                        log.error("Couldn't find the class associated with the enrichment " + enrichment);
                    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                        log.error("Couldn't create the instance associated with the enrichment " + enrichment, e);
                    }
                }

                try {
                    String className = config.get("redborder.processors.types." + processorName + ".class");
                    Class foundClass = Class.forName(className);
                    Constructor constructor = foundClass.getConstructor(StoreManager.class, EnrichManager.class, Config.class, TaskContext.class);
                    Processor processor = (Processor) constructor.newInstance(storeManager, enrichManager, config, context);
                    processorsList.add(processor);
                } catch (ClassNotFoundException e) {
                    log.error("Couldn't find the class associated with the stream " + processorName);
                    processorsList.add(new DummyProcessor());
                } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                    log.error("Couldn't create the instance associated with the stream " + processorName, e);
                    processorsList.add(new DummyProcessor());
                }
            }

            processors.put(streamName, processorsList);
        }

        return processors.get(streamName);
    }

    public abstract void process(Map<String, Object> message, MessageCollector collector);

    public abstract String getName();
}
