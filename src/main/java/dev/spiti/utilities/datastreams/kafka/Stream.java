package dev.spiti.utilities.datastreams.kafka;

import dev.spiti.utilities.datastreams.Streams;
import dev.spiti.utilities.datastreams.utils.KafkaAuthConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by kon1299 on 2019-07-12
 */
public class Stream implements Streams {
  
  private static final Properties properties = new Properties();
  
  // max.poll.records
  //
  
  static {
    properties.put("key.deserializer", StringDeserializer.class.getName());
    properties.put("value.deserializer", StringDeserializer.class.getName());
    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", StringSerializer.class.getName());
    properties.put("auto.offset.reset", "earliest");
    properties.put("zookeeper.session.timeout.ms", "5000");
    properties.put("enable.auto.commit", true);
    properties.put("reset.offset.on.start", true);
  }
  
  public Stream(String hosts, String groupId, String username, String password) {
    properties.put("bootstrap.servers", hosts);
    properties.put("group.id", groupId);
    KafkaAuthConfig config = new KafkaAuthConfig();
    config.setUsername(username);
    config.setPassword(password);
    properties.putAll(config.additionalConsumerProps());
  }
  
  public boolean produceMessage(String topicName, Object message) {
    boolean status = false;
    try {
      Producer<String, String> producer = new KafkaProducer<>(properties);
      producer.send(new ProducerRecord<String, String>(topicName, "key", String.valueOf(message)));
      Thread.sleep(1000);
      producer.close();
      status = true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return status;
  }
  
  public int produceMessages(String topicName, List<Object> messages) {
    int i = 0;
    for (Object object : messages) {
      if(produceMessage(topicName, object))
        i++;
    }
    return i;
  }
  
  public int produceMessages(String topicName, Object[] messages) {
    List<Object> messageList = Arrays.asList(messages);
    return produceMessages(topicName, messageList);
  }
  
  public int produceFile(String topicName, String pathToFile) {
    String line;
    List<Object> messageList = new ArrayList<>();
    try {
      BufferedReader reader = new BufferedReader(new FileReader(new File(pathToFile)));
      while ((line = reader.readLine()) != null) {
        if(line.length() > 0) {
          messageList.add(line);
        }
      }
    } catch (FileNotFoundException fnfe) {
      fnfe.printStackTrace();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return produceMessages(topicName, messageList);
  }
  
  public List<ConsumerRecord<String, String>> consumeMessages(String topicName) {
    KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
    
    return null;
  }
  
  public List<ConsumerRecord<String, String>> consumeMessages(String topicName, int count) {
    properties.put("max.poll.records", count);
    KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
    return null;
  }
  
  private List<ConsumerRecord<String, String>> consume(KafkaConsumer<String, String> consumer) {
    List<ConsumerRecord<String, String>> messages = new ArrayList<>();
    ConsumerRecords<String, String> records = consumer.poll(1000);
    for (ConsumerRecord<String, String> record : records) {
      messages.add(record);
    }
    return messages;
  }
  
  public boolean isFullyConsumed(String topicname) {
    KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
    consumer.subscribe(Arrays.asList(topicname));
    return consumer.endOffsets(consumer.assignment())
            .entrySet()
            .stream()
            .allMatch(e -> consumer.committed(e.getKey()).offset() == e.getValue());
  }
}
