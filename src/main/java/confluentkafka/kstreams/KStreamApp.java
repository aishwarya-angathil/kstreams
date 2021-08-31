package confluentkafka.kstreams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import com.training.Customer;
import com.training.InputCustomer;
import com.training.OutputCustomer;
import com.training.UpdatedCustomer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KStreamApp {
	static Properties allConfig = new Properties();
    public static void main(String[] args) throws Exception {
    	System.out.println("KStreamApp started main method");
        
        if(args.length>0) {
        	System.out.println("Args passed ->"+args.length);
        	System.out.println ( "Loading property file  ->"+args[0]);
        	InputStream inputConf = new FileInputStream(args[0]);
        	allConfig.load(inputConf);
        }
        
        if(args[1].equalsIgnoreCase("C")) {
        	System.out.println("Setting consumer because args ->"+args[1]);
        	Properties props = new Properties();
        	
        	String inputTopic=null;
        	String outputTopic = null;
        	String exceptionTopic = null;
        	String compactedTopic = null;
        if(!allConfig.isEmpty()) {
        	System.out.println("Setting consumer properties from prop file");
        	if(allConfig.getProperty("app")!=null && !allConfig.getProperty("app").isBlank() )
        		props.put(StreamsConfig.APPLICATION_ID_CONFIG, allConfig.getProperty("app"));
        	
        	if(allConfig.getProperty("bootstrap")!=null && !allConfig.getProperty("bootstrap").isBlank())
        		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, allConfig.getProperty("bootstrap")); // confluent Bootstrap Servers
            
        	props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
            
            if(allConfig.getProperty("schemaregistry")!= null && !allConfig.getProperty("schemaregistry").isBlank())
            	props.put("schema.registry.url", allConfig.getProperty("schemaregistry"));// Schema Registry URL
            
            if(allConfig.getProperty("mechanism")!=null && !allConfig.getProperty("mechanism").isBlank())
            	props.put(SaslConfigs.SASL_MECHANISM, allConfig.getProperty("mechanism"));
            
            if(allConfig.getProperty("protocol")!=null && !allConfig.getProperty("protocol").isBlank())
            	props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,allConfig.getProperty("protocol"));
            
            if(allConfig.getProperty("jaasmodule")!=null &&!allConfig.getProperty("jaasmodule").isBlank() &&
            		allConfig.getProperty("jaasuser")!=null && !allConfig.getProperty("jaasuser").isBlank() &&
            		allConfig.getProperty("jaaspwd")!=null && !allConfig.getProperty("jaaspwd").isBlank())
            			props.put(SaslConfigs.SASL_JAAS_CONFIG, allConfig.getProperty("jaasmodule")+" required username=\""+allConfig.getProperty("jaasuser")+"\" password=\""+allConfig.getProperty("jaaspwd")+"\";");
            
            inputTopic=allConfig.getProperty("inputtopic");
        	 outputTopic = allConfig.getProperty("outputtopic");
        	 exceptionTopic = allConfig.getProperty("exceptiontopic");

        }else {
        	System.out.println("Setting default consumer properties ");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-t0618");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9071"); // confluent Bootstrap Servers
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class); 
        props.put("schema.registry.url", "http://schemaregistry:8081");// Schema Registry URL
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
        inputTopic="customer";
   	 outputTopic = "outputcustomer";
   	 exceptionTopic = "exceptiontopic";
   	compactedTopic = "compactedTopic";
        }
        final StreamsBuilder builder = new StreamsBuilder();
        SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<Customer>();
        SpecificAvroSerde<InputCustomer> inputCustomerSerde = new SpecificAvroSerde<InputCustomer>();

        KStream<String, Customer> source = builder.stream(inputTopic,Consumed.with(Serdes.String(), customerSerde));
        KTable<String, InputCustomer> tble = builder.table(compactedTopic, Consumed.with(Serdes.String(), inputCustomerSerde));
        System.out.println("Building Kstream and Ktable");
        @SuppressWarnings("unchecked") // can we check type of datatype for al fields?
		KStream<String, Customer>[] branch = source
        		 .branch((key, appearance) -> (appearance.getName().equalsIgnoreCase("Namrata")),
                         (key, appearance) -> (!appearance.getName().equalsIgnoreCase("Namrata")));
        
        
       // KStream<String, Customer>[] branched = branch[0].branch((key, appearance) -> (tble.filter((key1, appearance1) -> appearance1.getId().equals(appearance.getId())).));

      
       branch[1].to(exceptionTopic);
       System.out.println("Sending data to exception topic");
        
        
       // KStream<String, UpdatedCustomer> dest = branch[0].mapValues(v->transformEvents(v));
       
       
       final Joiner joiner = new Joiner();
       System.out.println("Joining valid data and Ktable data");
       KStream<String, UpdatedCustomer> dest = branch[0].join(tble, joiner);
       
    
        dest.print(Printed.toSysOut());
        System.out.println("Sending updated data to output topic");
        dest.to(outputTopic); // do we need to uncomment for writing data to output tiopic?



        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
        
    }
        
        if(args[1].equals("P")) {

        	System.out.println("Setting consumer because args ->"+args[1]);
	        Properties properties = new Properties();
	        // normal producer
	       
	        properties.put("acks", "all");
	        properties.put("retries", "10");
	        // avro part
	       // properties.setProperty("key.serializer", StringSerializer.class.getName());
	       // properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
	        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
	                org.apache.kafka.common.serialization.StringSerializer.class.getName());
	        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
	                io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());
	       
	    	String outputTopic = null;
	        
	    	String compactedTopic = null;
	    	String key = null;
	        
	        if(!allConfig.isEmpty()) {
	        	System.out.println("Setting producer properties from prop file");
	        	if(allConfig.getProperty("app")!=null && !allConfig.getProperty("app").isBlank())
	        	properties.put(StreamsConfig.APPLICATION_ID_CONFIG, allConfig.getProperty("app")+"producer");
	        	
	        	if(allConfig.getProperty("bootstrap")!=null && !allConfig.getProperty("bootstrap").isBlank())
	        	properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, allConfig.getProperty("bootstrap")); // confluent Bootstrap Servers
	        	
	        	if(allConfig.getProperty("schemaregistry")!=null && !allConfig.getProperty("schemaregistry").isBlank())
	        	properties.put("schema.registry.url", allConfig.getProperty("schemaregistry"));// Schema Registry URL
	        	
	        	if(allConfig.getProperty("mechanism")!=null && !allConfig.getProperty("mechanism").isBlank())
	        	properties.put(SaslConfigs.SASL_MECHANISM, allConfig.getProperty("mechanism"));
	        	
	        	if(allConfig.getProperty("protocol")!=null && !allConfig.getProperty("protocol").isBlank())
	        	properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,allConfig.getProperty("protocol"));
	        	

	            if(allConfig.getProperty("jaasmodule")!=null &&!allConfig.getProperty("jaasmodule").isBlank() &&
	            		allConfig.getProperty("jaasuser")!=null && !allConfig.getProperty("jaasuser").isBlank() &&
	            		allConfig.getProperty("jaaspwd")!=null && !allConfig.getProperty("jaaspwd").isBlank())
	            			properties.put(SaslConfigs.SASL_JAAS_CONFIG, allConfig.getProperty("jaasmodule")+" required username=\""+allConfig.getProperty("jaasuser")+"\" password=\""+allConfig.getProperty("jaaspwd")+"\";");
	   
	        	 outputTopic = allConfig.getProperty("inputtopic");
	        	 compactedTopic = allConfig.getProperty("compactedTopic");
	        	 
	        	 if(allConfig.getProperty("key")!= null && !allConfig.getProperty("key").isBlank())
	        		 key = allConfig.getProperty("key");

	        }else {
	        	
	        	System.out.println("Setting default producer properties");
	        	properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-t0618producer");
	        	properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9071"); // confluent Bootstrap Servers
	        	properties.put("schema.registry.url", "http://schemaregistry:8081");// Schema Registry URL
	        	properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
	        	properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT");
	        	properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
	   	 outputTopic = "customer";
	   	compactedTopic = "compactedTopic";
	        }
	        
	        
	        
	        
	      //  Schema schema = new Schema.Parser().parse(new FileInputStream(args[1]));
	        
			/*
			 * GenericRecord Customer = new GenericData.Record(schema);
			 * 
			 * Customer.put("Name", "Eric"); Customer.put("Age", 65); Customer.put("City",
			 * "Mumbai");
			 */
	        System.out.println("Creating data for raw and compacted topic");
	        
	        Customer data = Customer.newBuilder().setId(1).setName("Namrata").setAge(20).setCity("Delhi").build();
	        InputCustomer compctedData = InputCustomer.newBuilder().setId(1).setFirstName("Namrata").setLastName("Kasana").setAddress("Gurgain,Haryana,Delhi").setEmail("namitakasana@gmail.com").setLevel("1").build();
	        InputCustomer compctedDataNew = InputCustomer.newBuilder().setId(1).setFirstName("Namrata").setLastName("Kasana").setAddress("TCS Gurgaon,Haryana,Delhi,Manchester").setEmail("namitakasana@gmail.com").setLevel("1").build();
	        Customer dataInvalid = Customer.newBuilder().setId(5).setName("Aishwarya").setAge(25).setCity("Chennai").build();
	        		//String val = "{'name':{'string':'ABCD'},'age':{'long':20},'city':{'string':'New DELHI'}}";
	     // construct kafka producer.
	        
	        System.out.println("Creating producers for raw and compacted topic");
	        KafkaProducer<String,Customer> producer = new KafkaProducer<String,Customer>(properties);
	        KafkaProducer<String,InputCustomer> compactedProducer = new KafkaProducer<String,InputCustomer>(properties);
	        
	      
	        ProducerRecord<String,Customer> record = new ProducerRecord<>(outputTopic, data);
	        ProducerRecord<String,Customer> recordInvalid = new ProducerRecord<>(outputTopic, dataInvalid);
	        ProducerRecord<String,InputCustomer> compactedRecord = null;
	        ProducerRecord<String,InputCustomer> compactedRecordNew = null;
	        
	        if(null!=key) {
	        	System.out.println("Key to be provided for compacted is -> "+key);
	        	if(key.contains("name")) {
	        		
	        		System.out.println("setting key as firstname");
			        compactedRecord = new ProducerRecord<>(compactedTopic, compctedData.getFirstName(),compctedData);
			        compactedRecordNew = new ProducerRecord<>(compactedTopic, compctedDataNew.getFirstName() , compctedDataNew);
			      }
	        	
	        	else if(key.contains("id")) {
	        		
	        		System.out.println("Setting key as Id");
			        compactedRecord = new ProducerRecord<>(compactedTopic, String.valueOf(compctedData.getId()),compctedData);
			        compactedRecordNew = new ProducerRecord<>(compactedTopic, String.valueOf(compctedDataNew.getId()) , compctedDataNew);
			      }
	        	
	        	else {
	        		
	        		System.out.println("No key has been set as the value doesnt match name or id");
	        		 compactedRecord = new ProducerRecord<>(compactedTopic,compctedData);
				        compactedRecordNew = new ProducerRecord<>(compactedTopic, compctedDataNew);
	        	}
	        }else {
	        	
	        	System.out.println("No key set as key is null");
       		 		compactedRecord = new ProducerRecord<>(compactedTopic,compctedData);
			        compactedRecordNew = new ProducerRecord<>(compactedTopic, compctedDataNew);
       	}

	        
	        try {
	        	
	        	System.out.println("Sending data to compacted topic");
	        	compactedProducer.send(compactedRecord);
	        	compactedProducer.send(compactedRecordNew);
	        	while(true) {
	        		System.out.println("Sending data to raw topic");
	        	  producer.send(record);
	        	  producer.send(recordInvalid);
	        	  
	        	}
	        	} catch(SerializationException e) {
	        	  // may need to do something with it
	        		System.out.println("Execption Found in Producer "+e.getMessage());
	        		e.printStackTrace();
	        	}catch (Exception e1) {
	        		System.out.println("Execption Found in Producer "+e1.getMessage());
	        		e1.printStackTrace();
	        	}
	        	// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
	        	// then close the producer to free its resources.
	        	finally {
	        		
	        		try {
	        			System.out.println("Flushing and closing producers");
	        	  compactedProducer.flush();
	        	  compactedProducer.close();
	        	  producer.flush();
	        	  producer.close();
	        	  System.out.println("Flushing and closing complete ");
	        		}catch(Exception e) {
	        			System.out.println("Execption Found in Producer Finally "+e.getMessage());
		        		e.printStackTrace();
	        		}
	        	}


	       


	
        }
    }
    
    public static UpdatedCustomer transformEvents(Customer customer){
   

    UpdatedCustomer updatedCustomer= UpdatedCustomer.newBuilder()
    		.setFirstName(customer.get("Name").toString())
            .setLastName("SomeName")
            .setAge((int) customer.get("Age"))
            .setCity(customer.get("City").toString())
            .build();
    
    
    return updatedCustomer;
    
    }
    
    
    
    private static SpecificAvroSerde<UpdatedCustomer> UpdatedCustomerAvroSerde() {
        SpecificAvroSerde<UpdatedCustomer> updatedAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        if(!allConfig.isEmpty() && allConfig.containsKey("schemaregistry"))
        	serdeConfig.put("schema.registry.url",allConfig.getProperty("schemaregistry"));
        else
        serdeConfig.put("schema.registry.url","http://localhost:8081");

        updatedAvroSerde.configure(serdeConfig, false);
        return updatedAvroSerde;
    }
    
    private static SpecificAvroSerde<InputCustomer> InputCustomerAvroSerde() {
        SpecificAvroSerde<InputCustomer> inputAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        if(!allConfig.isEmpty() && allConfig.containsKey("schemaregistry"))
        	serdeConfig.put("schema.registry.url",allConfig.getProperty("schemaregistry"));
        else
        serdeConfig.put("schema.registry.url","http://localhost:8081");

        inputAvroSerde.configure(serdeConfig, false);
        return inputAvroSerde;
    }
    
    
    }
