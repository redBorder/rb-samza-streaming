package net.redborder.samza.processors;

import org.apache.samza.task.MessageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Object;import java.lang.Override;import java.lang.String;import java.util.Map;

/**
 * This DummyProcessor will be used by the Processor base class if the class associated
 * with a streamName is not present or was not found. It does nothing except logging a warning
 * to let the user know that there is a topic with an unknown class.
 */

public class DummyProcessor extends Processor {
    private static final Logger log = LoggerFactory.getLogger(DummyProcessor.class);

    public DummyProcessor() {
        super(null, null, null, null);
    }

    @Override
    public String getName() {
        return "dummy";
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        log.warn("The dummy process method was called!");
    }
}
