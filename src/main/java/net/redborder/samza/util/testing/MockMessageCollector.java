package net.redborder.samza.util.testing;

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.MessageCollector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MockMessageCollector implements MessageCollector {

   List<Map<String, Object>> list = new ArrayList<>();

    @Override
    public void send(OutgoingMessageEnvelope outgoingMessageEnvelope) {
        list.add((Map<String, Object>) outgoingMessageEnvelope.getMessage());
    }

    public List<Map<String, Object>> getResult(){
        List<Map<String, Object>> currentList = new ArrayList<>();
        currentList.addAll(list);
        list.clear();
        return currentList;
    }
}
