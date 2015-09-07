package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.enrichments.IEnrich;
import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.NoSuchMethodException;
import java.lang.Object;
import java.lang.String;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class serves as a base class to implement different processors.
 * A processor is a class that process one particular Kafka topic. Every
 * processor should extend this class and implement its abstract methods to
 * serve messages coming from a topic.
 *
 * <p>To allow a processor to serve messages from a specific topic, you should
 * specify the class and its topic on the Samza task config file, with the
 * following format:
 *
 * <code>redborder.processors.topicName=your.classes.packages.TopicNameProcessor
 *
 * @param <T> The type of the messages coming from the topic. When they are
 *           JSON messages, it's usually some king of Map
 */

public abstract class Processor<T> {
    private static final Logger log = LoggerFactory.getLogger(Processor.class);

    // A map that establishes a relation between a topic name (key) and
    // its Processor class implementation
    private static Map<String, Processor> processors = new HashMap<>();

    // The store manager with the stores used to enrich this topic
    protected StoreManager storeManager;

    // The enrich manager with the enrichments used to enrich this topic
    protected EnrichManager enrichManager;

    // The task config
    protected Config config;

    // The task context
    protected TaskContext context;

    /**
     * Creates a new Processor instance for a certain topic.
     * @param storeManager A store manager with the stores that will be used to enrich this topic
     * @param enrichManager A enrich manager with the enrichments that will be used to enrich this topic
     * @param config The task config
     * @param context The task context
     */

    public Processor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        this.storeManager = storeManager;
        this.enrichManager = enrichManager;
        this.config = config;
        this.context = context;
    }

    /**
     * This method returns an instance of a Processor based on the given stream name.
     * The rest of parameters are used to build the instance of the Processor in case it wasn't
     * previously created by another call to this method.
     *
     * <p>To associate a processor instance with a stream name, you must specify it in
     * the task config file, with the following property:
     * <code>redborder.processors.topicName=your.classes.packages.TopicNameProcessor</code></p>
     *
     * <p>In case the processor instance associated with streamName wasn't instantiated before,
     * it will be created when calling this method. To do so, the property
     * <code>redborder.enrichments.streams.streamName</code> will be used to set the list of enrichments
     * on a new instance of EnrichManager that will be passed to the Processor constructor.
     *
     * @param streamName The stream name associated with a Processor instance
     * @param config The task config
     * @param context The task context
     * @param storeManager The task store manager
     * @return The Processor instance related with the given streamName
     */

    public static Processor getProcessor(String streamName, Config config, TaskContext context, StoreManager storeManager) {
        if (!processors.containsKey(streamName)) {
            List<String> enrichments;
            EnrichManager enrichManager = new EnrichManager();

            log.info("Asked for processor " + streamName + " but it wasn't found. Lets try to create it.");

            try {
                enrichments = config.getList("redborder.enrichments.streams." + streamName);
            } catch (ConfigException e) {
                log.info("Stream " + streamName + " does not have enrichments enabled");
                enrichments = new ArrayList<>();
            }

            for (String enrichment : enrichments) {
                try {
                    String className = config.get("redborder.enrichments.types." + enrichment);

                    if (className != null) {
                        Class enrichClass = Class.forName(className);
                        IEnrich enrich = (IEnrich) enrichClass.newInstance();
                        enrichManager.addEnrichment(enrich);
                    } else {
                        log.warn("Couldn't find property redborder.enrichments.types." + enrichment + " on config properties");
                    }
                } catch (ClassNotFoundException e) {
                    log.error("Couldn't find the class associated with the enrichment " + enrichment);
                } catch (InstantiationException | java.lang.IllegalAccessException e) {
                    log.error("Couldn't create the instance associated with the enrichment " + enrichment, e);
                }
            }

            try {
                String className = config.get("redborder.processors." + streamName);
                Class foundClass = Class.forName(className);
                Constructor constructor = foundClass.getConstructor(StoreManager.class, EnrichManager.class, Config.class, TaskContext.class);
                Processor processor = (Processor) constructor.newInstance(new Object[]{storeManager, enrichManager, config, context});
                processors.put(streamName, processor);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the stream " + streamName);
                processors.put(streamName, new DummyProcessor());
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the stream " + streamName, e);
                processors.put(streamName, new DummyProcessor());
            }
        }

        return processors.get(streamName);
    }

    /**
     * Process a message from the topic associated with the Processor instance
     * @param message The message from the topic
     * @param collector The collector that will collect the produced messages from this processor
     */

    public abstract void process(T message, MessageCollector collector);

    /**
     * @return Returns the processor name
     */

    public abstract String getName();
}
