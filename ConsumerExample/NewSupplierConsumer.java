import java.util.*;
import java.io.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class NewSupplierConsumer{

    public static void main(String[] args) throws Exception{

        String topicName = "SupplierTopic";
        String groupName = "SupplierTopicGroup";
        Properties props = new Properties();
        //props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        //props.put("group.id", groupName);
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", "SupplierDeserializer");
        //recall that consumers need to deserialize

        InputStream input = null;
        KafkaConsumer<String, Supplier> consumer = null;

        try {
            input = new FileInputStream("SupplierConsumer.properties");
            props.load(input);
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topicName)); //want to read data from the topics in the list topicName

            while (true){
                ConsumerRecords<String, Supplier> records = consumer.poll(100);
                //poll method handles all the corrdination, partition rebalances and heart beat for group corrdinator
                //and provides you a clean and straitforward api. 
                //the poll methods returns messanges, you process them and gets more messages
                //the .poll() paramter ie 100 is a timeout, it determines how long the poll will return with
                //or without data
                //if you dont poll for a while, the corrdinator may assume the consumer is dead
                //and trigger a partition rebalance.
                for (ConsumerRecord<String, Supplier> record : records){
                    System.out.println("Supplier id= " + String.valueOf(record.value().getID()) + " Supplier  Name = " + record.value().getName() + " Supplier Start Date = " + record.value().getStartDate().toString());
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            input.close();
            consumer.close();
        }
    }
}
