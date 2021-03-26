package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    // create configuration
    private Properties createConsumerConfigs(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ide-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
    // create consumer
    private KafkaConsumer<String, String> createConsumer(){
        return new KafkaConsumer<String, String>(createConsumerConfigs());
    }
    // subscribe to topic
    // run the poll loop
    private void subscribeAndConsume(String topic){
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList(topic));
        try{
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if(records.count() > 0) {
                    for (ConsumerRecord<String, String> record : records) {
                        String recordValue = record.value();
                        String recordKey = record.key();
                        System.out.println("Record key: " + recordKey +
                                "\nValue: " + recordValue +
                                "\nPartition: " + record.partition() +
                                "\nOffset: " + record.offset());
                    }
                    consumer.commitSync();
                }
            }
        } catch (Exception ex){
            System.out.println("Some problem occured: " + ex.getMessage());
        }
    }

    public static void main(String[] args) {
        ConsumerDemo consumerDemo = new ConsumerDemo();
        consumerDemo.subscribeAndConsume("keys-ide-cb-topic");
    }
}
