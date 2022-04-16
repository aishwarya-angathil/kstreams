package confluentkafka.kstreams;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.log4j.Logger;

import com.training.Address;
import com.training.ExternalIdentifiers;
import com.training.Individual;
import com.training.JsonSchema;
import com.training.Party;

import confluentUtils.PartyService;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
public class Messager implements Runnable{
	private ArrayList<ProducerRecord<String,JsonSchema>> records;
	private ArrayList<ProducerRecord<String,Individual>> compactedrecords;
	private ArrayList<ProducerRecord<String,ExternalIdentifiers>> compactedrecords1;
	private SpecificAvroSerde<JsonSchema> partySerde;
	private SpecificAvroSerde<Individual> addressSerde;
	private SpecificAvroSerde<ExternalIdentifiers> externalSerde;
	private Properties properties;
	private String topic;
	private int count ;
	static Logger logger = Logger.getLogger(Messager.class);

	public Messager(String topic ,ArrayList<ProducerRecord<String,JsonSchema>> records,ArrayList<ProducerRecord<String,Individual>> compactedrecords,ArrayList<ProducerRecord<String,ExternalIdentifiers>> compactedrecords1, Properties properties,SpecificAvroSerde<JsonSchema> partySerde,SpecificAvroSerde<Individual> addressSerde,SpecificAvroSerde<ExternalIdentifiers> externalSerde,int count ) {
		super();
		this.records = records;
		this.compactedrecords=compactedrecords;
		this.compactedrecords1 = compactedrecords1;
		this.count=count;
		this.partySerde = partySerde;
		this.addressSerde = addressSerde;
		this.externalSerde = externalSerde;
		this.properties = properties;
		this.topic = topic;
		
		logger.debug("Creating Messager producerRecords -->"+this.records+" compactedproducerRecords -->"+this.compactedrecords+" partySerd->"+this.partySerde+" addressSerde->"+this.addressSerde+" properties->"+this.properties+" topic->"+this.topic+" count->"+this.count);
	
	}
	@Override
	public void run() {
		
		Iterator allprop = properties.keySet().iterator();
		while(allprop.hasNext()) {
			String k = (String) allprop.next();
			logger.debug("Properties set "+k +" : "+properties.getProperty(k));
		}
		// TODO Auto-generated method stub
		if(this.partySerde!=null && this.records!=null) {
			
			
			
			KafkaProducer<String,JsonSchema> producer = new KafkaProducer<>(this.properties, Serdes.String().serializer(), this.partySerde.serializer());//raw topic
			
			try {
		int i = 0;
    	while(i<this.count) {
    		logger.debug("Sending data to "+this.topic +" topic");
    		logger.debug("Total records to be sent to "+this.topic +" topic ->"+records.size());
    		this.records.forEach(x -> {
    			logger.debug("Data raw to be sent -->"+x);
    			producer.send(x);
    		
        	});
    	  i++;
    	}
		} catch(SerializationException e) {
      	  // may need to do something with it
      		logger.debug("Exception Found in Producer "+e.getMessage());
      		e.printStackTrace();
      	}catch (Exception e1) {
      		System.out.println("Exception Found in Producer "+e1.getMessage());
      		e1.printStackTrace();
      	}
      	// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
      	// then close the producer to free its resources.
      	finally {
      		
      		try {
      			
      			//logger.debug("Closing serdes");
	        	//  customerSerde.close();
      	  //partySerde.close();
      	  logger.debug("Flushing and closing producer");
      	  producer.flush();
      	  producer.close();
      	  
      	  logger.debug("Flushing and closing complete ");
      		}catch(Exception e) {
      			logger.debug("Exception Found in Producer Finally "+e.getMessage());
	        		e.printStackTrace();
      		}
      	}
		}
		
		if(this.addressSerde!=null && this.compactedrecords!=null) {
			
			  KafkaProducer<String,Individual> compactedProducer = new KafkaProducer<>(properties, Serdes.String().serializer(), addressSerde.serializer() ); //compacted topic
			
			  
			  try {
			      logger.debug("Sending data to "+this.topic +"compacted topic");
	    		logger.debug("Total records to be sent to "+this.topic +" topic ->"+compactedrecords.size());
	    		this.compactedrecords.forEach(x -> {
	    			logger.debug("Data compacted to be sent -->"+x);
	    			compactedProducer.send(x);
	    		
	        	});
			
		} catch(SerializationException e) {
      	  // may need to do something with it
      		logger.debug("Exception Found in Producer "+e.getMessage());
      		e.printStackTrace();
      	}catch (Exception e1) {
      		logger.debug("Exception Found in Producer "+e1.getMessage());
      		e1.printStackTrace();
      	}
      	// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
      	// then close the producer to free its resources.
      	finally {
      		
      		try {
      			
      			//logger.debug("Closing serdes");
      	  //inputCustomerSerde.close();
      	  //addressSerde.close();
      	  logger.debug("Flushing and closing producer");
      	  compactedProducer.flush();
      	  compactedProducer.close();
      	  
      	  logger.debug("Flushing and closing complete ");
      		}catch(Exception e) {
      			logger.debug("Execption Found in Producer Finally "+e.getMessage());
	        		e.printStackTrace();
      		}
      	}

		
	}
		if(this.externalSerde!=null && this.compactedrecords1!=null) {
			  KafkaProducer<String,ExternalIdentifiers> compactedProducer= new KafkaProducer<>(properties, Serdes.String().serializer(), externalSerde.serializer() ); //compacted topic for exteranl Identifiers
			  try {
			      logger.debug("Sending data to "+this.topic +"compacted topic");
	    		logger.debug("Total records to be sent to "+this.topic +" topic ->"+compactedrecords1.size());
	    		this.compactedrecords1.forEach(x -> {
	    			logger.debug("Data compacted to be sent -->"+x);
	    			compactedProducer.send(x);
	    		
	        	});
			
		}
			  catch(SerializationException e) {
		      	  // may need to do something with it
		      		logger.debug("Exception Found in Producer "+e.getMessage());
		      		e.printStackTrace();
		      	}catch (Exception e1) {
		      		logger.debug("Exception Found in Producer "+e1.getMessage());
		      		e1.printStackTrace();
		      	}
		      	// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
		      	// then close the producer to free its resources.
		      	finally {
		      		try {
		      			
		      			//logger.debug("Closing serdes");
		      	  //inputCustomerSerde.close();
		      	  //addressSerde.close();
		      	  logger.debug("Flushing and closing producer");
		      	  compactedProducer.flush();
		      	  compactedProducer.close();
		      	  
		      	  logger.debug("Flushing and closing complete ");
		      		}catch(Exception e) {
		      			logger.debug("Execption Found in Producer Finally "+e.getMessage());
			        		e.printStackTrace();
		      		}
		      	}

				
			}
		      	
		      		
			  
		


	}
}
