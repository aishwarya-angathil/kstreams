package confluentkafka.kstreams;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import com.training.Customer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class KafkaAvroProducer {

	    public static void main(String[] args) throws IOException {
	        Properties properties = new Properties();
	        Properties allConfig = new Properties();
	        // normal producer
	       
	        properties.setProperty("acks", "all");
	        properties.setProperty("retries", "10");
	        // avro part
	        properties.setProperty("key.serializer", StringSerializer.class.getName());
	        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
	       
	    	String outputTopic = null;
	        
	    
	    	if(args.length!=2) {
	    		 System.err.println("java -jar <jar file> <properties file> <avsc file>");
	   	      return;
	    		
	    	}
	        	InputStream inputConf = new FileInputStream(args[0]);
	        	allConfig.load(inputConf);
	        
	        
	        if(!allConfig.isEmpty()) {
	        	properties.put(StreamsConfig.APPLICATION_ID_CONFIG, allConfig.getProperty("app")+"producer");
	        	properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, allConfig.getProperty("bootstrap")); // confluent Bootstrap Servers
	        	
	        	properties.put("schema.registry.url", allConfig.getProperty("schemaregistry"));// Schema Registry URL
	        	properties.put(SaslConfigs.SASL_MECHANISM, allConfig.getProperty("mechanism"));
	        	properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,allConfig.getProperty("protocol"));
	        	properties.put(SaslConfigs.SASL_JAAS_CONFIG, allConfig.getProperty("jaasmodule")+" required username=\""+allConfig.getProperty("jaasuser")+"\" password=\""+allConfig.getProperty("jaaspwd")+"\";");
	   
	        	 outputTopic = allConfig.getProperty("inputtopic");

	        }else {
	        	properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-t0618producer");
	        	properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9071"); // confluent Bootstrap Servers
	        	properties.put("schema.registry.url", "http://schemaregistry:8081");// Schema Registry URL
	        	properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
	        	properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT");
	        	properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
	   	 outputTopic = "customer";
	        }
	        
	        
	        
	        
	        Schema schema = new Schema.Parser().parse(new FileInputStream(args[1]));
	        
			/*
			 * GenericRecord Customer = new GenericData.Record(schema);
			 * 
			 * Customer.put("Name", "Eric"); Customer.put("Age", 65); Customer.put("City",
			 * "Mumbai");
			 */

	        
	        Customer data = Customer.newBuilder().setName("ABCD").setAge(20).setCity("Delhi").build();
	        Customer dataInvalid = Customer.newBuilder().setName("EFGH").setAge(30).setCity("Mumbai").build();
	        		//String val = "{'name':{'string':'ABCD'},'age':{'long':20},'city':{'string':'New DELHI'}}";
	     // construct kafka producer.
	        KafkaProducer producer = new KafkaProducer(properties);

	      
	        ProducerRecord<Object, Object> record = new ProducerRecord<>(outputTopic, null, data);
	        ProducerRecord<Object, Object> recordInvalid = new ProducerRecord<>(outputTopic, null, dataInvalid);
	        
	        try {
	        	while(true) {
	        	  producer.send(record);
	        	  producer.send(recordInvalid);
	        	}
	        	} catch(SerializationException e) {
	        	  // may need to do something with it
	        		System.out.println("Execption Found "+e.getMessage());
	        	}
	        	// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
	        	// then close the producer to free its resources.
	        	finally {
	        	  producer.flush();
	        	  producer.close();
	        	}


	       


	}

}
