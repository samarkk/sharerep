package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ProducerFileClient {
    private static String bootStrapservers = "192.168.181.131:9092";
    private static String topic = "nsefotopic";

    public static void main(String[] args) throws IOException {
        // create properties for producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapservers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
//        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "15000");
//        props.put(ProducerConfig.)
//        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "130000");
        // create producer using properties
        KafkaProducer<String, String> producer = new KafkaProducer<String,
                String>(props);

        FileReader fr = new FileReader("D:/tmp/fo01JAN2020bhav.csv");
        BufferedReader br = new BufferedReader(fr);
        br.readLine();
        String line = "";
        try {
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, line);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        String sendDetails =
                                "Producer Record sent to topic: " + metadata.topic() +
                                        "\nPartition: " + metadata.partition() +
                                        "\nKey: " + record.key() +
                                        "\nOffset: " + metadata.offset() +
                                        "\nTimestamp: " + metadata.timestamp();
                        System.out.println(sendDetails);
                    }
                });
            }
            producer.flush();
        } catch (Exception ex) {
            System.out.println("Exception underway");
            ex.printStackTrace();
        } finally {
            System.out.println("Finally block in action");
            producer.close();
        }
    }
}
