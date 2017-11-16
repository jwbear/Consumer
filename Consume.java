package consume.kafka_consume;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 
 * Not thread safe
 * 
 * Server:  kafka-server-start /usr/local/etc/kafka/server.properties
 */
public class Consume 
{
    static KafkaConsumer<String, String> consumer;

    public static void main( String[] args )
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        //allow auto commits 
        props.put("enable.auto.commit", "true");
        //set at movement of robot
        props.put("auto.commit.interval.ms", "1");
        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("consumer.timeout.ms", "10");
      //fixes bug were metadata hands send
 	   //props.put("metadata.fetch.timeout.ms", 1);
        consumer = new KafkaConsumer<String, String>(props);
        
        //designate separate consumer for different topics???
        consumer.subscribe(Arrays.asList("odom", "laser", "nasdaq"));
        
        //decide to commit messages from Kafka once they reach this size
        //final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer =
        		new ArrayList<ConsumerRecord<String,String>>();
        try{
        	 //Thread t = new Thread(new MessageLoop());
             //t.start();
        	
	        while (true) {
	        	System.out.println("ctic");
	        	//specify timeout
	            ConsumerRecords<String, String> records = consumer.poll(1);
	            //System.out.println(records.isEmpty());
	            for (ConsumerRecord<String, String> record : records) {
	                if(record.topic().equals("odom")){
		            	System.out.println("keyo: " + record.key() + " value: " + record.value());

	                }else if(record.topic().equals("laser")){
		            	System.out.println("keyl: " + record.key() + " value: " + record.value());
		            	
	                }else if(record.topic().equals("nasdaq")){
		            	System.out.println("keyl: " + record.key() + " value: " + record.value());


	               }
	            }
	            /*for (ConsumerRecord<String, String> record : records) {
	            	System.out.println("key: " + record.key() + " value: " + record.value());
	                //buffer.add(record);
	            }*/
	            //System.out.println("ctic1");
	            /*if (buffer.size() >= minBatchSize) {
	            	//save to file oro give to Spark Streaming
	                //insertIntoDb(buffer);
	                consumer.commitSync();
	                buffer.clear();
	            }*/
	        }
        } finally {
	        //close consumer prevent leak
	        consumer.close();
        }
    }
}

