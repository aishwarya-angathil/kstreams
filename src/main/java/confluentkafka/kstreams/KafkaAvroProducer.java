package confluentkafka.kstreams;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class KafkaAvroProducer {

	    public static void main(String[] args) throws IOException {
	        Properties properties = new Properties();
	        // normal producer
	        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
	        properties.setProperty("acks", "all");
	        properties.setProperty("retries", "10");
	        // avro part
	        properties.setProperty("key.serializer", StringSerializer.class.getName());
	        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
	        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
	        
	        
	        Schema schema = new Schema.Parser().parse(new File("C:\\Users\\ravir\\Downloads\\Customer.avsc"));
	        
	        GenericRecord Customer = new GenericData.Record(schema);

	        Customer.put("Name", "Eric");
	        Customer.put("Age", 65);
	        Customer.put("City", "Mumbai");

	     // construct kafka producer.
	        KafkaProducer producer = new KafkaProducer(properties);

	        String topic = "customer-avro1";

	      
	        ProducerRecord<Object, Object> record = new ProducerRecord<>("topic1", null, Customer);
	        
	        try {
	        	  producer.send(record);
	        	} catch(SerializationException e) {
	        	  // may need to do something with it
	        		System.out.println("Execption Found");
	        	}
	        	// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
	        	// then close the producer to free its resources.
	        	finally {
	        	  producer.flush();
	        	  producer.close();
	        	}


	       


	}

}
