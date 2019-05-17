import java.util.*;
import org.apache.kafka.clients.producer.*;
public class SimpleProducer {
  
   public static void main(String[] args) throws Exception{
           
      String topicName = "SimpleProducerTopic";
	  String key = "Key1"; // this key and the value below are sent to the topic topicName
	  String value = "Value-1";
      
      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092,localhost:9093"); //boostrap servers is a list of kafka brokers
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");//kafka only accepts an array of bytes, so u need to 
	   //serialize the key and value, depending on its type, it will be serialized as int, string, double etc
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	
//step 1. make the kafka producer object   
      Producer<String, String> producer = new KafkaProducer <>(props); // to create the object u need the property configurations with at least
	   //three main configurations, which is located at line 11
	   //here, we pass the property object through the <>(props) object constructor, and instantiate a producer
	   //in <String, String>, the first string is the type of key, the other is the type of value. this is contextual to the type of message
	   //ur sending
	
	   //step 2. create a producer record object 
	  ProducerRecord<String, String> record = new ProducerRecord<>(topicName,key,value); // the producerRecord requires topicName,
	   //key and value which pass
	   //through a constructor object, and we then instantiate a producer record, this object is our message
	   //the producerRecord should be given to producer, so the producer can send it to kafka broker
	
	   //  st.3. send the record to the producer
	   producer.send(record); //here we make a call to send method on Producer object and handover the 
	   //recordObject.
	   //now were done and its now the producers responsibility to deliver this message to the broker
      producer.close(); //after sending all the messages u need to close the Producer object
	  
	  System.out.println("SimpleProducer Completed.");
   }
}
