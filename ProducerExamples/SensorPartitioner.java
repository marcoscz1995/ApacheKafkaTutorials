import java.util.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.utils.*;
import org.apache.kafka.common.record.*;

public class SensorPartitioner implements Partitioner { //this custom partinioner must implemetnt Partitioner interface
     //a custom partionor must implment three methods: 1.configure, 2.partition, 3.close

     
     private String speedSensorName;
     
     //the configure and close methods are like initialization and clean up methods. they are called once at the time of 
     //instantiating your producer
     public void configure(Map<String, ?> configs) {
          speedSensorName = configs.get("speed.sensor.name").toString(); //extracting 

     }

     public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
//this is where we grab a message to determine which partition it will go to, thus it will return an int
          //st.1 we dtermine the number of partitions and reserve x amount for specific messages
           List<PartitionInfo> partitions = cluster.partitionsForTopic(topic); //get a list of all the partitions available
           int numPartitions = partitions.size(); //how many partitions there are
           int sp = (int)Math.abs(numPartitions*0.3); //how many partitions we want to reserve
           int p=0;

            if ( (keyBytes == null) || (!(key instanceof String)) ) //make sure the message has a key
                 throw new InvalidRecordException("All messages must have sensor name as key");

            if ( ((String)key).equals(speedSensorName) ) //determine if it will go to the 30% partition or the other partitiosn
                 p = Utils.toPositive(Utils.murmur2(valueBytes)) % sp; //p is in 0,1,2 
               //we hash value
            else
                 p = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions-sp) + sp ; //p is in 3,6...9

                 System.out.println("Key = " + (String)key + " Partition = " + p );
                 return p;
  }
      public void close() {}

}
