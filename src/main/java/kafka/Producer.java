package kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import server.proto.PersonOuterClass.Person;

public class Producer {
	
	public static void producer(String topic,server.proto.PersonOuterClass.Person person) {

		if (topic == null) {
			System.err.println("Topic is null");
			System.exit(-1);
		}
		
		String topicName = topic;
		Properties configProperties = new Properties();
		KafkaProducer<String, Person> producer = null;
		
		try
		{
			//Configure the Producer
			
			configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
			
			configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
			
			ProducerRecord<String, Person> rec = null;
			
			configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"kafka.PersonSerializer");
			
			producer = new KafkaProducer<String, Person>(configProperties);

			rec = new ProducerRecord<String, Person>(topicName, person);
						
			producer.send(rec);
		
		}
		catch(Exception e)
		{
		
			System.out.println(e);
			System.out.println(e.getStackTrace());
			
		}
		
		producer.close();
		
		System.out.println("Object pushed to Kafka Successfully!!!");
	}

}