package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

public class ProducerWithKeysCallback {
    // create confiugrations
    private Properties createConfigs(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.181.131:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
    // create producer
    private KafkaProducer<String, String> createProducer(){
        Properties configs = createConfigs();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);
        return producer;
    }
    // create records
    private ArrayList<ProducerRecord<String, String>> createProducerRecords(){
        String [] cities = new String[]{"Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Kolkatta", "Jaipur", "Mohali"};
        String [] statuses = new String[]{"Won", "Lost", "Draw"};
        ArrayList<ProducerRecord<String, String>> records = new ArrayList<ProducerRecord<String, String>>();

        for (int i = 0; i < 10; i++){
        String keyCity = cities[new Random().nextInt(cities.length)];
        String status = statuses[new Random().nextInt(statuses.length)];
        ProducerRecord<String, String> record = new ProducerRecord<>("keys-ide-cb-topic", keyCity, status);
        records.add(record);
        }
        return records;
    }
    // send records with callback
    private void sendRecordsToKafka(){
        KafkaProducer producer = createProducer();
        ArrayList<ProducerRecord<String, String>> producerRecords = createProducerRecords();
        try{
            producerRecords.forEach(prec -> producer.send(prec));
        } catch (Exception ex){
            System.out.println("some problem occured " + ex.getStackTrace());
        } finally {
            producer.close();
        }
    }

    public static void main(String[] args) {
        ProducerWithKeysCallback producer = new ProducerWithKeysCallback();
        producer.sendRecordsToKafka();
    }
}
