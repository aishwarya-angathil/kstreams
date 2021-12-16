package confluentkafka.kstreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import org.apache.log4j.Logger;

//import com.training.Customer;
//import com.training.InputCustomer;
//import com.training.UpdatedCustomer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import com.training.Address;
import com.training.Enriched;
import com.training.Party;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KStreamApp {
	
	 static Logger logger = Logger.getLogger(KStreamApp.class);
	static Properties allConfig = new Properties();
    public static void main(String[] args) throws Exception {
     final ScheduledExecutorService schedulerToSendMessage = Executors.newScheduledThreadPool(1);

     logger.debug("KStreamApp started main method");
        
        if(args.length>0) {
        	logger.debug("Args passed ->"+args.length);
        	logger.debug( "Loading property file  ->"+args[0]);
        	InputStream inputConf = new FileInputStream(args[0]);
        	allConfig.load(inputConf);
        }
        
        if(args[1].equalsIgnoreCase("C")) {
        	logger.debug("Setting consumer because args ->"+args[1]);
        	Properties props = new Properties();
        	
        	String inputTopic=null;
        	String outputTopic = null;
        	String exceptionTopic = null;
        	String compactedTopic = null;
        	
        	
 	      // props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
         //  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);  // magic byte problem
           
           props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
	                org.apache.kafka.common.serialization.StringDeserializer.class.getName());
	        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
	                io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());
	        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
	        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_json");
       
	       // SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<Customer>();
	        //SpecificAvroSerde<InputCustomer> inputCustomerSerde = new SpecificAvroSerde<InputCustomer>();
	        
	        //SpecificAvroSerde<UpdatedCustomer> updatedCustomerSerde = new SpecificAvroSerde<UpdatedCustomer>();
	        
	        
	        SpecificAvroSerde<Party> partySerde = new SpecificAvroSerde<Party>();
	        SpecificAvroSerde<Address> addressSerde = new SpecificAvroSerde<Address>();
	        
	        SpecificAvroSerde<Enriched> enrichedSerde = new SpecificAvroSerde<Enriched>();
	        
	        if(!allConfig.isEmpty()) {
	        	logger.debug("Setting consumer properties from prop file");
        	if(allConfig.getProperty("app")!=null && !allConfig.getProperty("app").isBlank() )
        		props.put(StreamsConfig.APPLICATION_ID_CONFIG, allConfig.getProperty("app"));
        	
        	if(allConfig.getProperty("bootstrap")!=null && !allConfig.getProperty("bootstrap").isBlank())
        		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, allConfig.getProperty("bootstrap")); // confluent Bootstrap Servers
            
        	
            
            if(allConfig.getProperty("schemaregistry")!= null && !allConfig.getProperty("schemaregistry").isBlank()) {
            	props.put("schema.registry.url", allConfig.getProperty("schemaregistry"));// Schema Registry URL
            	props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG , allConfig.getProperty("schemaregistry"));
            	//map with 1 prop SR url need to be set in each serde.. hence it was giving SR is null error .
            	Map<String, String> serdeConfig=Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, allConfig.getProperty("schemaregistry"));
            	//customerSerde.configure(serdeConfig, false);
            	//inputCustomerSerde.configure(serdeConfig, false);
            	//updatedCustomerSerde.configure(serdeConfig, false);//join of raw and compacted
            	
            	partySerde.configure(serdeConfig, false);
            	addressSerde.configure(serdeConfig, false);
            	enrichedSerde.configure(serdeConfig, false);//join of raw and compacted
            	
            }
            	
            
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
        	 compactedTopic = allConfig.getProperty("compactedTopic");

        }else {
        	logger.debug("Setting default consumer properties ");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-t0618");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9071"); // confluent Bootstrap Servers
        
        props.put("schema.registry.url", "http://schemaregistry:8081");// Schema Registry URL
        
        Map<String, String> serdeConfig=Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8081");
        //customerSerde.configure(serdeConfig, false);
    	//inputCustomerSerde.configure(serdeConfig, false);
    	//updatedCustomerSerde.configure(serdeConfig, false);
        
        partySerde.configure(serdeConfig, false);
    	addressSerde.configure(serdeConfig, false);
    	enrichedSerde.configure(serdeConfig, false);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
        inputTopic="customer";
   	 outputTopic = "outputcustomer";
   	 exceptionTopic = "exceptiontopic";
   	compactedTopic = "compactedTopic";
        }
        final StreamsBuilder builder = new StreamsBuilder();
       
        //KStream<String, Customer> source = builder.stream(inputTopic,Consumed.with(Serdes.String(), customerSerde));  // raw topic  topic /key / value
        //KTable<String, InputCustomer> tble = builder.table(compactedTopic, Consumed.with(Serdes.String(), inputCustomerSerde)); // compacted topic
        KStream<String, Party> source = builder.stream(inputTopic,Consumed.with(Serdes.String(), partySerde));  // raw topic  topic /key / value
        KTable<String, Address> tble = builder.table(compactedTopic, Consumed.with(Serdes.String(), addressSerde)); // compacted topic
      
        
        logger.debug("Building Kstream and Ktable");
        @SuppressWarnings("unchecked") // can we check type of datatype for al fields?
        //KStream<String, Customer>[] branch = source
        
        
        KStream<String, Party>[] branch = source
        		 .branch((key, appearance) -> (!appearance.getFirstForename().equalsIgnoreCase("Aishwarya")),
                         (key, appearance) -> (appearance.getFirstForename().equalsIgnoreCase("Aishwarya")));
        
        
       // KStream<String, Customer>[] branched = branch[0].branch((key, appearance) -> (tble.filter((key1, appearance1) -> appearance1.getId().equals(appearance.getId())).));
logger.debug(branch.length +" branch entries ");
      
      // branch[1].to(exceptionTopic);  // can checnage and insert in compacted if needed .
      //  branch[1].to(exceptionTopic, Produced.with(Serdes.String(), customerSerde)); //serialize
        branch[1].to(exceptionTopic, Produced.with(Serdes.String(), partySerde)); //serialize
        logger.debug("Exception  -->"+branch[1]);
        logger.debug("Sending data to exception topic");
        
        
       // KStream<String, UpdatedCustomer> dest = branch[0].mapValues(v->transformEvents(v));
       
       
       final Joiner joiner = new Joiner();  // to join raw (kctesm) and compacted topic (ktable)
       logger.debug("Joining valid data and Ktable data");
    //   KStream<String, UpdatedCustomer> dest = branch[0].join(tble, joiner);
       KStream<String, Enriched> dest = branch[0].join(tble, joiner);
       
    
        dest.print(Printed.toSysOut());//print data
        logger.debug("Valid -->"+dest);
        logger.debug("Sending updated data to output topic");
       // dest.to(outputTopic); // do we need to uncomment for writing data to output tiopic?
       // dest.to(outputTopic, Produced.with(Serdes.String(), updatedCustomerSerde)); // updatedCustomerSerde serde of joined data
        dest.to(outputTopic, Produced.with(Serdes.String(), enrichedSerde)); 

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
        	logger.debug("Starting Consumer");
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
        
    }
      // Producer
        
        if(args[1].equals("P")) {
        	int delay =2;
        	int period = 2;
        	
        	if(null!=args[2])
        		delay = Integer.valueOf(args[2]);
        	if(null!=args[3])
        		period = Integer.valueOf(args[3]);
        		

        	logger.debug("Setting producer because args ->"+args[1]);
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
	       
	        //SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<Customer>();  //raw topic
	        SpecificAvroSerde<Party> partySerde = new SpecificAvroSerde<Party>();  //raw topic
	        
	    	int messagesCount=10;
	        HashMap<String, String > otherprop = new HashMap<>();
	        
	        if(!allConfig.isEmpty()) {
	        	logger.debug("Setting producer properties from prop file");
	        	if(allConfig.getProperty("app")!=null && !allConfig.getProperty("app").isBlank())
	        	properties.put(StreamsConfig.APPLICATION_ID_CONFIG, allConfig.getProperty("app")+"producer");
	        	
	        	if(allConfig.getProperty("bootstrap")!=null && !allConfig.getProperty("bootstrap").isBlank())
	        	properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, allConfig.getProperty("bootstrap")); // confluent Bootstrap Servers
	        	
	        	if(allConfig.getProperty("schemaregistry")!=null && !allConfig.getProperty("schemaregistry").isBlank()) {
	        		properties.put("schema.registry.url", allConfig.getProperty("schemaregistry"));// Schema Registry URL
	        		
	        		Map<String, String> serdeConfig=Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, allConfig.getProperty("schemaregistry"));
	            	//customerSerde.configure(serdeConfig, false);
	        		partySerde.configure(serdeConfig, false);
	        	}
	        	
	        	
	        	if(allConfig.getProperty("mechanism")!=null && !allConfig.getProperty("mechanism").isBlank())
	        	properties.put(SaslConfigs.SASL_MECHANISM, allConfig.getProperty("mechanism"));
	        	
	        	if(allConfig.getProperty("protocol")!=null && !allConfig.getProperty("protocol").isBlank())
	        	properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,allConfig.getProperty("protocol"));
	        	

	            if(allConfig.getProperty("jaasmodule")!=null &&!allConfig.getProperty("jaasmodule").isBlank() &&
	            		allConfig.getProperty("jaasuser")!=null && !allConfig.getProperty("jaasuser").isBlank() &&
	            		allConfig.getProperty("jaaspwd")!=null && !allConfig.getProperty("jaaspwd").isBlank())
	            			properties.put(SaslConfigs.SASL_JAAS_CONFIG, allConfig.getProperty("jaasmodule")+" required username=\""+allConfig.getProperty("jaasuser")+"\" password=\""+allConfig.getProperty("jaaspwd")+"\";");
	            
	            otherprop.put("outputTopic", allConfig.getProperty("inputtopic"));
	        	 
	        	 
	        	 if(allConfig.getProperty("key")!= null && !allConfig.getProperty("key").isBlank())
	        		 otherprop.put("key", allConfig.getProperty("key"));
	        		 
	        	 
	        	 if(allConfig.getProperty("count")!= null && !allConfig.getProperty("count").isBlank())
	        		 messagesCount = Integer.valueOf(allConfig.getProperty("count"));

	        }else {
	        	
	        	logger.debug("Setting default producer properties");
	        	properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-t0618producer");
	        	properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9071"); // confluent Bootstrap Servers
	        	properties.put("schema.registry.url", "http://schemaregistry:8081");// Schema Registry URL
	        	Map<String, String> serdeConfig=Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8081");
            	//customerSerde.configure(serdeConfig, false);
	        	partySerde.configure(serdeConfig, false);
	        	properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
	        	properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT");
	        	properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
	   	
	    otherprop.put("outputTopic", "customer");
        
    	 
	        }
	        
	        
	        
	        
	      //  Schema schema = new Schema.Parser().parse(new FileInputStream(args[1]));
	        
			/*
			 * GenericRecord Customer = new GenericData.Record(schema);
			 * 
			 * Customer.put("Name", "Eric"); Customer.put("Age", 65); Customer.put("City",
			 * "Mumbai");
			 */
	        logger.debug("Creating data for raw and compacted topic");
	        
	        
	        
	        //ArrayList<ProducerRecord<String,Customer>> producerRecord = new ArrayList< ProducerRecord<String,Customer>>();
	        ArrayList<ProducerRecord<String,Party>> producerRecord = new ArrayList< ProducerRecord<String,Party>>();
	     
	        allConfig.stringPropertyNames().parallelStream().filter(x->x.startsWith("data.raw")).forEach( y ->{
	        	String value = allConfig.getProperty(y);
	        	logger.debug("Building data and record for " + y);
	        	String att [] = value.split(",");
	        //	 Customer data = Customer.newBuilder().setId(Integer.valueOf(att[0])).setName(att[1]).setAge(Integer.valueOf(att[2])).setCity(att[3]).build();
	        	Party data = Party.newBuilder().setPartyID(Integer.valueOf(att[0])).setFirstForename(att[1]).setSecondForename(att[2]).setSurname(att[3]).setFirstInitial(att[4]).setSecondInitial(att[5]).setThirdInitial(att[6]).setPartyIndicator(att[7]).setBirthDate(att[8]).build();
	        	 String k = otherprop.get("key");
	        	 String out = otherprop.get("outputTopic");
	        	 if(null!=k) {
	        		// setting the key
	 	        	if(k.contains("name")) {
	 	        		logger.debug("Key to be provided for valid raw is -> "+k);
	 	        		producerRecord.add(new ProducerRecord<>(out,data.getFirstForename(), data));
	 	        	}
	 	        	else if(k.contains("id")) {
	 	        		logger.debug("Key to be provided for valid RAW is -> "+k);
	 	        		producerRecord.add(new ProducerRecord<>(out,String.valueOf(data.getPartyID()), data));
	 	        	}else {
	 	        		logger.debug("No key has been set for valid RAW as the value doesnt match name or id");
	 	        		producerRecord.add(new ProducerRecord<>(out, data));
	 	        	}
	 	        }else {
	 	        	logger.debug("No key set for valid RAW as key is null");
	 	        	producerRecord.add(new ProducerRecord<>(out, data));
	 	        }
	        });
	        
	        
	        
		
	     // construct kafka producer.
	        
	        logger.debug("Creating producers for raw topic");
	        
	        //KafkaProducer<String,Customer> producer = new KafkaProducer<String,Customer>(properties);
	        //KafkaProducer<String,InputCustomer> compactedProducer = new KafkaProducer<String,InputCustomer>(properties);
	        
	        //KafkaProducer<String,Customer> producer = new KafkaProducer<>(properties, Serdes.String().serializer(), customerSerde.serializer());//raw topic
	        //KafkaProducer<String,Party> producer = new KafkaProducer<>(properties, Serdes.String().serializer(), partySerde.serializer());//raw topic
	      
	       	        
	        try {
	        	schedulerToSendMessage.scheduleAtFixedRate(new Messager(otherprop.get("outputTopic"),producerRecord,null,properties,partySerde,null,messagesCount), delay, period, TimeUnit.MINUTES);
	        	
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
	        	

	
        }
        
// Compacted Producer
        
        if(args[1].equals("CP")) {
        	int delay =2;
        	int period = 2;
        	
        	if(null!=args[3])
        		delay = Integer.valueOf(args[3]);
        	if(null!=args[4])
        		period = Integer.valueOf(args[4]);
        		

        	logger.debug("Setting compacted producer because args ->"+args[1]);
        	if(args.length<3) {
        		logger.debug("Please pass 3rd argument as I or U . I for Insert and U for Update");
        		System.exit(1);
        	}
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
	       
	        //SpecificAvroSerde<InputCustomer> inputCustomerSerde = new SpecificAvroSerde<InputCustomer>(); //compacted topic
	        SpecificAvroSerde<Address> addressSerde = new SpecificAvroSerde<Address>(); //compacted topic
	        HashMap<String, String > otherprop = new HashMap<>();
	        
	        if(!allConfig.isEmpty()) {
	        	logger.debug("Setting producer properties from prop file");
	        	if(allConfig.getProperty("app")!=null && !allConfig.getProperty("app").isBlank())
	        	properties.put(StreamsConfig.APPLICATION_ID_CONFIG, allConfig.getProperty("app")+"producer");
	        	
	        	if(allConfig.getProperty("bootstrap")!=null && !allConfig.getProperty("bootstrap").isBlank())
	        	properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, allConfig.getProperty("bootstrap")); // confluent Bootstrap Servers
	        	
	        	if(allConfig.getProperty("schemaregistry")!=null && !allConfig.getProperty("schemaregistry").isBlank()) {
	        		properties.put("schema.registry.url", allConfig.getProperty("schemaregistry"));// Schema Registry URL
	        		
	        		Map<String, String> serdeConfig=Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, allConfig.getProperty("schemaregistry"));
	            	
	        		//inputCustomerSerde.configure(serdeConfig, false);
	        		addressSerde.configure(serdeConfig, false);
	        	}
	        	
	        	
	        	if(allConfig.getProperty("mechanism")!=null && !allConfig.getProperty("mechanism").isBlank())
	        	properties.put(SaslConfigs.SASL_MECHANISM, allConfig.getProperty("mechanism"));
	        	
	        	if(allConfig.getProperty("protocol")!=null && !allConfig.getProperty("protocol").isBlank())
	        	properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,allConfig.getProperty("protocol"));
	        	

	            if(allConfig.getProperty("jaasmodule")!=null &&!allConfig.getProperty("jaasmodule").isBlank() &&
	            		allConfig.getProperty("jaasuser")!=null && !allConfig.getProperty("jaasuser").isBlank() &&
	            		allConfig.getProperty("jaaspwd")!=null && !allConfig.getProperty("jaaspwd").isBlank())
	            			properties.put(SaslConfigs.SASL_JAAS_CONFIG, allConfig.getProperty("jaasmodule")+" required username=\""+allConfig.getProperty("jaasuser")+"\" password=\""+allConfig.getProperty("jaaspwd")+"\";");
	            
	            
	            otherprop.put("compactedTopic", allConfig.getProperty("compactedTopic"));
	        	 
	        	 
	        	 if(allConfig.getProperty("key")!= null && !allConfig.getProperty("key").isBlank())
	        		 otherprop.put("key", allConfig.getProperty("key"));
	        	
	        	 

	        }else {
	        	
	        	logger.debug("Setting default producer properties");
	        	properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-t0618producer");
	        	properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9071"); // confluent Bootstrap Servers
	        	properties.put("schema.registry.url", "http://schemaregistry:8081");// Schema Registry URL
	        	Map<String, String> serdeConfig=Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8081");
            	
            	//inputCustomerSerde.configure(serdeConfig, false);
	        	addressSerde.configure(serdeConfig, false);
	        	properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
	        	properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT");
	        	properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
	   	
	    
        otherprop.put("compactedTopic", "compactedTopic");
    	 
	        }
	        
	        
	        
	        
	      //  Schema schema = new Schema.Parser().parse(new FileInputStream(args[1]));
	        
			/*
			 * GenericRecord Customer = new GenericData.Record(schema);
			 * 
			 * Customer.put("Name", "Eric"); Customer.put("Age", 65); Customer.put("City",
			 * "Mumbai");
			 */
	        logger.debug("Creating data for  compacted topic");
	        
	        
	        
	    	///ArrayList<ProducerRecord<String,InputCustomer>> compactedProducerRecord = new ArrayList< ProducerRecord<String,InputCustomer>>();
	    	ArrayList<ProducerRecord<String,Address>> compactedProducerRecord = new ArrayList< ProducerRecord<String,Address>>();
	        String dataToSearch = args[2];
	        
	        logger.debug("Producer will send records starting with property -> "+"data.compacted."+dataToSearch.trim().toLowerCase());
	    
	        allConfig.stringPropertyNames().parallelStream().filter(x->x.startsWith("data.compacted."+dataToSearch.trim().toLowerCase())).forEach( y ->{
	        	String value = allConfig.getProperty(y);
	        	logger.debug("Building data and record for " + y);
	        	String att [] = value.split(",");
	        	// setting the InputCustomer record value Input customer is POJO for compaced topic
	        	//InputCustomer data = InputCustomer.newBuilder().setId(Integer.valueOf(att[0])).setFirstName(att[1]).setLastName(att[2]).setAddress(att[3]).setEmail(att[4]).setLevel(att[5]).build();
	        	Address data = Address.newBuilder().setPartyID(Integer.valueOf(att[0])).setStructuredAddressID(Integer.valueOf(att[1])).setUnstructuredAddressID(Integer.valueOf(att[2])).setPartyAddressType(att[3]).setPartyAddress(att[4]).setAddressCareOfName(att[5]).setEffectiveDate(att[6]).setExpiryDate(att[7]).build();
	        	
	        	String k = otherprop.get("key");
	        	String out = otherprop.get("compactedTopic");
	        	 if(null!=k) {
	        		// setting the InputCustomer record key
	        		 logger.debug("Key to be provided for compacted is -> "+k);
	 	        	if(k.contains("name")) {
	 	        		logger.debug("setting key as firstname");
	 	        		compactedProducerRecord.add(new ProducerRecord<>(out, data.getAddressCareOfName(),data));
	 			      }
	 	        	else if(k.contains("id")) {
	 	        		
	 	        		logger.debug("Key to be provided for compacted is -> "+k);
	 	        		compactedProducerRecord.add(new ProducerRecord<>(out, String.valueOf(data.getPartyID()),data));
	 			          
	 			      }
	 	        	
	 	        	else {
	 	        		
	 	        		logger.debug("No key has been set as the value doesnt match name or id for compacted");
	 	        		compactedProducerRecord.add(new ProducerRecord<>(out, data));
	 	        		
	 			
	 	        	}
	 	        }else {
	 	        	logger.debug("No key set for compacted as key is null");
	 	        	compactedProducerRecord.add(new ProducerRecord<>(out, data));
	        	}

	        });
	        
		
	     // construct kafka producer.
	        
	        logger.debug("Creating producers for  compacted topic");
	        
	        //KafkaProducer<String,Customer> producer = new KafkaProducer<String,Customer>(properties);
	        //KafkaProducer<String,InputCustomer> compactedProducer = new KafkaProducer<String,InputCustomer>(properties);
	     
	        //KafkaProducer<String,InputCustomer> compactedProducer = new KafkaProducer<>(properties, Serdes.String().serializer(), inputCustomerSerde.serializer()); //compacted topic
	        
	        //KafkaProducer<String,Address> compactedProducer = new KafkaProducer<>(properties, Serdes.String().serializer(), addressSerde.serializer()); //compacted topic
	       	        
	        try {
	        	
	        	
	        	schedulerToSendMessage.scheduleAtFixedRate(new Messager(otherprop.get("outputTopic"),null,compactedProducerRecord,null,null,addressSerde,0), delay, period, TimeUnit.MINUTES);
	        	
	        	} catch(SerializationException e) {
	        	  // may need to do something with it
	        		logger.debug("Exception Found in Producer "+e.getMessage());
	        		e.printStackTrace();
	        	}catch (Exception e1) {
	        		logger.debug("Exception Found in Producer "+e1.getMessage());
	        		e1.printStackTrace();
	        	}
	        	

	       


	
        }
    }
	/*
	 * //not used
	 * 
	 * public static UpdatedCustomer transformEvents(Customer customer){
	 * 
	 * 
	 * UpdatedCustomer updatedCustomer= UpdatedCustomer.newBuilder()
	 * .setFirstName(customer.get("Name").toString()) .setLastName("SomeName")
	 * .setAge((int) customer.get("Age")) .setCity(customer.get("City").toString())
	 * .build();
	 * 
	 * 
	 * return updatedCustomer;
	 * 
	 * }
	 * 
	 * 
	 * 
	 * private static SpecificAvroSerde<UpdatedCustomer> UpdatedCustomerAvroSerde()
	 * { SpecificAvroSerde<UpdatedCustomer> updatedAvroSerde = new
	 * SpecificAvroSerde<>();
	 * 
	 * final HashMap<String, String> serdeConfig = new HashMap<>();
	 * if(!allConfig.isEmpty() && allConfig.containsKey("schemaregistry"))
	 * serdeConfig.put("schema.registry.url",allConfig.getProperty("schemaregistry")
	 * ); else serdeConfig.put("schema.registry.url","http://localhost:8081");
	 * 
	 * updatedAvroSerde.configure(serdeConfig, false); return updatedAvroSerde; }
	 * 
	 * private static SpecificAvroSerde<InputCustomer> InputCustomerAvroSerde() {
	 * SpecificAvroSerde<InputCustomer> inputAvroSerde = new SpecificAvroSerde<>();
	 * 
	 * final HashMap<String, String> serdeConfig = new HashMap<>();
	 * if(!allConfig.isEmpty() && allConfig.containsKey("schemaregistry"))
	 * serdeConfig.put("schema.registry.url",allConfig.getProperty("schemaregistry")
	 * ); else serdeConfig.put("schema.registry.url","http://localhost:8081");
	 * 
	 * inputAvroSerde.configure(serdeConfig, false); return inputAvroSerde; }
	 */
    
    
    }
