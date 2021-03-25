package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // producer configurations
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.181.131:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create producer using configurations
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        // careate a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("ide_topic", "hello from the ide");
        // send the record to kafka
        try {
            producer.send(record);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        producer.close();
    }
}
