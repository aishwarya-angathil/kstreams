package confluentkafka.kstreams;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
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
    	Properties props = new Properties();
    	
    	String inputTopic=null;
    	String outputTopic = null;
    	String exceptionTopic = null;
        
        if(args.length>0) {
        	InputStream inputConf = new FileInputStream(args[0]);
        	allConfig.load(inputConf);
        }
        
        if(!allConfig.isEmpty()) {
        	props.put(StreamsConfig.APPLICATION_ID_CONFIG, allConfig.getProperty("app"));
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, allConfig.getProperty("bootstrap")); // confluent Bootstrap Servers
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class); 
            props.put("schema.registry.url", allConfig.getProperty("schemaregistry"));// Schema Registry URL
            props.put(SaslConfigs.SASL_MECHANISM, allConfig.getProperty("mechanism"));
            props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,allConfig.getProperty("protocol"));
            props.put(SaslConfigs.SASL_JAAS_CONFIG, allConfig.getProperty("jaasmodule")+" required username=\""+allConfig.getProperty("jaasuser")+"\" password=\""+allConfig.getProperty("jaaspwd")+"\";");
             inputTopic=allConfig.getProperty("inputtopic");
        	 outputTopic = allConfig.getProperty("outputtopic");
        	 exceptionTopic = allConfig.getProperty("exceptiontopic");

        }else {
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
        }
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Customer> source = builder.stream(inputTopic);
        
        @SuppressWarnings("unchecked") // can we check type of datatype for al fields?
		KStream<String, Customer>[] branch = source
        		 .branch((key, appearance) -> (appearance.getName().equalsIgnoreCase("ABCD")),
                         (key, appearance) -> (!appearance.getName().equalsIgnoreCase("ABCD")));
        

       branch[1].to(exceptionTopic);
        
        
        
        KStream<String, UpdatedCustomer> dest = branch[0].mapValues(v->transformEvents(v));
        
        
    
        dest.print(Printed.toSysOut());
        
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
