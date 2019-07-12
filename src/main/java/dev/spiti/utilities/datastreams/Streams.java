package dev.spiti.utilities.datastreams;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * Created by naresh.g@spititech.com on 2019-05-01
 */
public interface Streams {
  
  boolean produceMessage(String topicName, Object message);
  int produceMessages(String topicName, List<Object> messages);
  int produceMessages(String topicName, Object[] messages);
  int produceFile(String topicName, String pathToFile);
  List<ConsumerRecord<String, String>> consumeMessages(String topicName);
  List<ConsumerRecord<String, String>> consumeMessages(String topicName, int count);
  
}
