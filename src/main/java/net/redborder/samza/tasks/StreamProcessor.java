package net.redborder.samza.tasks;

import net.redborder.samza.processors.Processor;
import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class StreamProcessor implements StreamTask, InitableTask, WindowableTask {
    private static final Logger log = LoggerFactory.getLogger(StreamProcessor.class);

    private Config config;
    private StoreManager storeManager;
    private TaskContext context;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.config = config;
        this.context = context;
        this.storeManager = new StoreManager(config, context);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String stream = envelope.getSystemStreamPartition().getSystemStream().getStream();
        Object message = envelope.getMessage();

        List<Processor> processors = Processor.getProcessors(stream, this.config, this.context, this.storeManager);
        if (message instanceof Map) {
            for (Processor processor : processors) {
                processor.process((Map<String, Object>) message, collector);
            }
        } else {
            log.warn("This message is not a map class: " + message);
        }
    }

    @Override
    public void window(MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        storeManager.refreshWindowStores();
    }
}
