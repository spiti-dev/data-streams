package dev.spiti.utilities.datastreams.utils;

import java.util.Map;
import com.google.common.collect.ImmutableMap;

import static java.lang.String.format;

/**
 * Created by kon1299 on 2019-07-12
 */
public class KafkaAuthConfig implements AdditionalKafkaProducerProperties,
        AdditionalKafkaConsumerProperties {
  
  private static final String JAAS_CONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
          + "serviceName=\"kafka\"\n"
          + "username=\"%s\"\n"
          + "password=\"%s\";";
  
  private String username;
  private String password;
  private boolean enabled;
  
  private Map<String, String> additionalProps() {
    return ImmutableMap.of(
            "security.protocol", "SASL_PLAINTEXT",
            "sasl.mechanism", "PLAIN",
            "sasl.jaas.config", format(JAAS_CONFIG, username, password));
  }
  
  public String getUsername() {
    return username;
  }
  
  public void setUsername(String username) {
    this.username = username;
  }
  
  public String getPassword() {
    return password;
  }
  
  public void setPassword(String password) {
    this.password = password;
  }
  
  public boolean isEnabled() {
    return enabled;
  }
  
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }
  
  @Override public Map<String, String> additionalProducerProps() {
    return additionalProps();
  }
  
  @Override public Map<String, String> additionalConsumerProps() {
    return additionalProps();
  }
}
