package confluentkafka.kstreams;

import org.apache.avro.generic.GenericData.Array;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

//import com.training.Customer;
//import com.training.InputCustomer;
//import com.training.UpdatedCustomer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.utils.Json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.training.Address;
import com.training.Enriched;
import com.training.Enriched2;
import com.training.Enriched3;
import com.training.Enriched4;
import com.training.ExternalIdentifiers;
import com.training.Individual;
import com.training.JsonSchema;
import com.training.Party;

import confluentUtils.ExtrenalDetails;
import confluentUtils.IndividualDetails;
import confluentUtils.IndividualPartyDetails;
import confluentUtils.PartyIdentifierDetails;
import confluentUtils.PartyService;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class KStreamApp {

	static Logger logger = Logger.getLogger(KStreamApp.class);
	static Properties allConfig = new Properties();

	static JSONObject jsonObject = null;
	static String afterTag = null;
	static String eventType = null;
	static String beforeTag = null;
	static Integer partyId = null;
	static String timestampUTC = null;
	static ObjectMapper mapper = null;
	static String result = null;
	static IndividualDetails individualDetails = null;
	static List<ExtrenalDetails> externalIdentifiersList = new ArrayList<ExtrenalDetails>();

	public static void main(String[] args) throws Exception {
		final ScheduledExecutorService schedulerToSendMessage = Executors.newScheduledThreadPool(1);

		logger.debug("KStreamApp started main method");

		if (args.length > 0) {
			logger.debug("Args passed ->" + args.length);
			logger.debug("Loading property file  ->" + args[0]);
			InputStream inputConf = new FileInputStream(args[0]);
			allConfig.load(inputConf);
		}

		if (args[1].equalsIgnoreCase("C")) {
			logger.debug("Setting consumer because args ->" + args[1]);
			Properties props = new Properties();

			String inputTopic = null;
			String inputTopic1 = null;
			String outputTopic = null;
			String exceptionTopic = null;
			String compactedTopic = null;
			String compactedTopic1 = null;

			// props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
			// Serdes.String().getClass());
			// props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
			// SpecificAvroSerde.class); // magic byte problem

			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					org.apache.kafka.common.serialization.StringDeserializer.class.getName());
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());
			props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_json");

			// SpecificAvroSerde<Customer> customerSerde = new
			// SpecificAvroSerde<Customer>();
			// SpecificAvroSerde<InputCustomer> inputCustomerSerde = new
			// SpecificAvroSerde<InputCustomer>();

			// SpecificAvroSerde<UpdatedCustomer> updatedCustomerSerde = new
			// SpecificAvroSerde<UpdatedCustomer>();

			// SpecificAvroSerde<Party> partySerde = new SpecificAvroSerde<Party>();
			System.out.println("in properties of c " + props);
			SpecificAvroSerde<Individual> addressSerde = new SpecificAvroSerde<Individual>();
			SpecificAvroSerde<ExternalIdentifiers> externalSerde = new SpecificAvroSerde<ExternalIdentifiers>(); // compacted
																													// topic

			SpecificAvroSerde<JsonSchema> jsonSerde = new SpecificAvroSerde<JsonSchema>();
			// SpecificAvroSerde<jsonSchema> jsonSerde = new
			// SpecificAvroSerde<jsonSchema>();

			// SpecificAvroSerde<Enriched> enrichedSerde = new
			// SpecificAvroSerde<Enriched>();
			SpecificAvroSerde<Enriched4> enrichedSerde2 = new SpecificAvroSerde<Enriched4>();

			if (!allConfig.isEmpty()) {
				logger.debug("Setting consumer properties from prop file");
				if (allConfig.getProperty("app") != null && !allConfig.getProperty("app").isBlank())
					props.put(StreamsConfig.APPLICATION_ID_CONFIG, allConfig.getProperty("app"));

				if (allConfig.getProperty("bootstrap") != null && !allConfig.getProperty("bootstrap").isBlank())
					props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, allConfig.getProperty("bootstrap")); // confluent
																											// Bootstrap
																											// Servers

				if (allConfig.getProperty("schemaregistry") != null
						&& !allConfig.getProperty("schemaregistry").isBlank()) {
					props.put("schema.registry.url", allConfig.getProperty("schemaregistry"));// Schema Registry URL
					props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
							allConfig.getProperty("schemaregistry"));
					// map with 1 prop SR url need to be set in each serde.. hence it was giving SR
					// is null error .
					Map<String, String> serdeConfig = Collections.singletonMap(
							AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
							allConfig.getProperty("schemaregistry"));
					// customerSerde.configure(serdeConfig, false);
					// inputCustomerSerde.configure(serdeConfig, false);
					// updatedCustomerSerde.configure(serdeConfig, false);//join of raw and
					// compacted

					// partySerde.configure(serdeConfig, false);
					addressSerde.configure(serdeConfig, false);
					jsonSerde.configure(serdeConfig, false);
					externalSerde.configure(serdeConfig, false);// compacted topic created for externalIdentifiers
					enrichedSerde2.configure(serdeConfig, false);// join of raw and compacted

				}

				if (allConfig.getProperty("mechanism") != null && !allConfig.getProperty("mechanism").isBlank())
					props.put(SaslConfigs.SASL_MECHANISM, allConfig.getProperty("mechanism"));

				if (allConfig.getProperty("protocol") != null && !allConfig.getProperty("protocol").isBlank())
					props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, allConfig.getProperty("protocol"));

				if (allConfig.getProperty("jaasmodule") != null && !allConfig.getProperty("jaasmodule").isBlank()
						&& allConfig.getProperty("jaasuser") != null && !allConfig.getProperty("jaasuser").isBlank()
						&& allConfig.getProperty("jaaspwd") != null && !allConfig.getProperty("jaaspwd").isBlank())
					props.put(SaslConfigs.SASL_JAAS_CONFIG,
							allConfig.getProperty("jaasmodule") + " required username=\""
									+ allConfig.getProperty("jaasuser") + "\" password=\""
									+ allConfig.getProperty("jaaspwd") + "\";");

				inputTopic = allConfig.getProperty("inputtopic");
				outputTopic = allConfig.getProperty("outputtopic");
				exceptionTopic = allConfig.getProperty("exceptiontopic");
				compactedTopic = allConfig.getProperty("compactedTopic");
				compactedTopic1 = allConfig.getProperty("compactedTopic1");

			} else {
				logger.debug("Setting default consumer properties ");
				props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-t0618");
				props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9071"); // confluent Bootstrap Servers

				props.put("schema.registry.url", "http://schemaregistry:8081");// Schema Registry URL

				Map<String, String> serdeConfig = Collections.singletonMap(
						AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8081");
				// customerSerde.configure(serdeConfig, false);
				// inputCustomerSerde.configure(serdeConfig, false);
				// updatedCustomerSerde.configure(serdeConfig, false);

				jsonSerde.configure(serdeConfig, false);
				addressSerde.configure(serdeConfig, false);
				externalSerde.configure(serdeConfig, false);
				enrichedSerde2.configure(serdeConfig, false);
				props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
				props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
				props.put(SaslConfigs.SASL_JAAS_CONFIG,
						"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
				inputTopic = "customer";
				outputTopic = "outputcustomer";
				exceptionTopic = "exceptiontopic";
				compactedTopic = "compactedTopic";
				compactedTopic1 = "compactedTopic1";
				System.out.println("in properties of c " + props);
			}
			final StreamsBuilder builder = new StreamsBuilder();
			KStream<String, JsonSchema> source = builder.stream(inputTopic, Consumed.with(Serdes.String(), jsonSerde));
			// System.out.println("raw data " + source);// raw topic topic /key / value
			logger.debug("raw data " + source);
			// System.out.println("raw data "+source);
			KTable<String, Individual> tble = builder.table(compactedTopic,
					Consumed.with(Serdes.String(), addressSerde)); // compacted
			KTable<String, ExternalIdentifiers> externalTble = builder.table(compactedTopic1,
					Consumed.with(Serdes.String(), externalSerde));

			// topic
			System.out.println("compacted Data " + tble);
			System.out.println("compacted Data for externalIdentifiers " + externalTble);
			logger.debug("Building Kstream and Ktable");

			// @SuppressWarnings("unchecked") // can we check type of datatype for al

			/*
			 * KStream<String, jsonSchema>[] branch = jsonSchema.branch( (key, appearance)
			 * -> (!appearance.getPartyId().equalsIgnoreCase()), (key, appearance) ->
			 * (appearance.getPartyId().equalsIgnoreCase("12")));
			 */

			// branch[1].to(exceptionTopic); // can checnage and insert in compacted if
			// needed .
			// branch[1].to(exceptionTopic, Produced.with(Serdes.String(), customerSerde));
			// //serialize
			// branch[1].to(exceptionTopic, Produced.with(Serdes.String(), partySerde)); //
			// serialize
			// logger.debug("Exception -->" + branch[1]);
			// logger.debug("Sending data to exception topic");

			// KStream<String, UpdatedCustomer> dest =
			// branch[0].mapValues(v->transformEvents(v));

			/*
			 * PartyService pService=new PartyService(); Individual ind = new Individual();
			 * System.out.println("in Individual "+ind); pService.setIndividual(ind);
			 * List<ExternalIdentifiers> extList=new ArrayList<ExternalIdentifiers>();
			 * System.out.println("In external List "+extList);
			 * pService.setExternalIdentifiers(extList);
			 * System.out.println("In Party Service "+pService);
			 * result=mapper.writeValueAsString(pService);
			 * System.out.println("Result "+result);
			 * System.out.println("In codepoints "+result.codePoints());
			 */

			final Joiner2 joiner2 = new Joiner2(); // to join raw (kctesm) and compacted topic (ktable)
			logger.debug("joiner " + joiner2);
			System.out.println("in joiner " + joiner2);
			final Joiner joiner = new Joiner();
			final Joiner3 joiner3 = new Joiner3();

			logger.debug("Joining valid data and Ktable data");
			// KStream<String, UpdatedCustomer> dest = branch[0].join(tble, joiner);

			/*
			 * KStream<String, String> afterTagStream = (KStream<String, String>)
			 * afterTag.chars() .mapToObj(a -> String.valueOf((char) a));
			 */
			KStream<String, Enriched2> dest = source.join(tble, joiner2);//for joining raw with compacted1
			KStream<String, Enriched3> dest1 = source.join(externalTble, joiner);//for joining raw with compacted2
			/*
			 * ValueJoiner<Enriched2,Enriched3,Enriched5> valueJoiner=(a,b)->{return a+b;};
			 * dest.leftJoin(dest1, valueJoiner, JoinWindows.of(Duration.ofSeconds(10)));
			 */
			// KStream<String, Enriched5> finalOutput=dest.join(dest1,joiner3);
			// dest.merge(dest1);

			// KStream<String, String> dest = (KStream<String, String>) result.codePoints();

			/*
			 * System.out.println("hi we are in enriched output " + dest); mapper= new
			 * ObjectMapper(); PartyService pService=new PartyService();
			 * individualDetails=new IndividualDetails();
			 * individualDetails.setRawIndividual(dest);
			 * 
			 * KStream<String, Integer> outputStream = dest.mapValues(new
			 * ValueMapper<String, Integer> { Integer apply(String value) { return
			 * value.split(" ").length; }
			 * 
			 * pService.setIndividual(individualDetails);
			 * System.out.println("In individualDetails "+individualDetails.getRawIndividual
			 * ()); ExtrenalDetails externalDetails=new ExtrenalDetails();
			 * externalDetails.setRawExternal(dest1);
			 * externalIdentifiersList.add(externalDetails);
			 * System.out.println("In externalIdentifiersList "+externalIdentifiersList);
			 * pService.setExternalIdentifiers(externalIdentifiersList);
			 * System.out.println("In a partService class "+pService);
			 * result=mapper.writeValueAsString(pService);
			 * System.out.println("In a result "+result);
			 */
			// System.out.println("In codepoints
			// "+result.codePoints().mapToObj(c->String.valueOf((char)c)));
			// KStream<String,String> dest2=(KStream<String, String>)
			// result.codePoints().mapToObj(c->String.valueOf((char)c));

			dest.print(Printed.toSysOut());
			dest1.print(Printed.toSysOut());
			// dest2.print(Printed.toSysOut());

			// KStream<String, String> source1 = builder.stream(inputTopic1);
			// KStream<String,String>
			// foutput=result.flatMapValues(value->Arrays.asList(result.toStream()));
			KStream<String, Enriched4> finalOutput = dest.join(dest1, joiner3, JoinWindows.of(Duration.ofSeconds(0)));
			finalOutput.print(Printed.toSysOut());

			// pService.print(Printed.toSysOut());
			// print data logger.debug("Valid -->" + dest);
			logger.debug("Sending updated data to output topic");

			// dest.to(outputTopic); // do we need to uncomment for writing data to output
			// tiopic?
			// dest.to(outputTopic, Produced.with(Serdes.String(), updatedCustomerSerde));
			// // updatedCustomerSerde serde of joined data
			finalOutput.to(outputTopic, Produced.with(Serdes.String(), enrichedSerde2));
			// result

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

		if (args[1].equals("P")) {
			int delay = 2;
			int period = 2;

			if (null != args[2])
				delay = Integer.valueOf(args[2]);
			if (null != args[3])
				period = Integer.valueOf(args[3]);

			logger.debug("Setting producer because args ->" + args[1]);
			Properties properties = new Properties();
			// normal producer

			properties.put("acks", "all");
			properties.put("retries", "10");
			// avro part
			// properties.setProperty("key.serializer", StringSerializer.class.getName());
			// properties.setProperty("value.serializer",
			// KafkaAvroSerializer.class.getName());
			properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					org.apache.kafka.common.serialization.StringSerializer.class.getName());
			properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());

			// SpecificAvroSerde<Customer> customerSerde = new
			// SpecificAvroSerde<Customer>(); //raw topic
			// SpecificAvroSerde<Party> partySerde = new SpecificAvroSerde<Party>(); //raw
			// topic
			SpecificAvroSerde<JsonSchema> jsonSerde = new SpecificAvroSerde<JsonSchema>();

			int messagesCount = 10;
			HashMap<String, String> otherprop = new HashMap<>();

			if (!allConfig.isEmpty()) {
				logger.debug("Setting producer properties from prop file");
				if (allConfig.getProperty("app") != null && !allConfig.getProperty("app").isBlank())
					properties.put(StreamsConfig.APPLICATION_ID_CONFIG, allConfig.getProperty("app") + "producer");

				if (allConfig.getProperty("bootstrap") != null && !allConfig.getProperty("bootstrap").isBlank())
					properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, allConfig.getProperty("bootstrap")); // confluent
																												// Bootstrap
																												// Servers

				if (allConfig.getProperty("schemaregistry") != null
						&& !allConfig.getProperty("schemaregistry").isBlank()) {
					properties.put("schema.registry.url", allConfig.getProperty("schemaregistry"));// Schema Registry
																									// URL

					Map<String, String> serdeConfig = Collections.singletonMap(
							AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
							allConfig.getProperty("schemaregistry"));
					// customerSerde.configure(serdeConfig, false);
					jsonSerde.configure(serdeConfig, false);
				}

				if (allConfig.getProperty("mechanism") != null && !allConfig.getProperty("mechanism").isBlank())
					properties.put(SaslConfigs.SASL_MECHANISM, allConfig.getProperty("mechanism"));

				if (allConfig.getProperty("protocol") != null && !allConfig.getProperty("protocol").isBlank())
					properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, allConfig.getProperty("protocol"));

				if (allConfig.getProperty("jaasmodule") != null && !allConfig.getProperty("jaasmodule").isBlank()
						&& allConfig.getProperty("jaasuser") != null && !allConfig.getProperty("jaasuser").isBlank()
						&& allConfig.getProperty("jaaspwd") != null && !allConfig.getProperty("jaaspwd").isBlank())
					properties.put(SaslConfigs.SASL_JAAS_CONFIG,
							allConfig.getProperty("jaasmodule") + " required username=\""
									+ allConfig.getProperty("jaasuser") + "\" password=\""
									+ allConfig.getProperty("jaaspwd") + "\";");

				otherprop.put("outputTopic", allConfig.getProperty("inputtopic"));

				if (allConfig.getProperty("key") != null && !allConfig.getProperty("key").isBlank())
					otherprop.put("key", allConfig.getProperty("key"));

				if (allConfig.getProperty("count") != null && !allConfig.getProperty("count").isBlank())
					messagesCount = Integer.valueOf(allConfig.getProperty("count"));

			} else {

				logger.debug("Setting default producer properties");
				properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-t0618producer");
				properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9071"); // confluent Bootstrap Servers
				properties.put("schema.registry.url", "http://schemaregistry:8081");// Schema Registry URL
				Map<String, String> serdeConfig = Collections.singletonMap(
						AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8081");
				// customerSerde.configure(serdeConfig, false);
				jsonSerde.configure(serdeConfig, false);
				properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
				properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
				properties.put(SaslConfigs.SASL_JAAS_CONFIG,
						"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");

				otherprop.put("outputTopic", "customer");

			}

			// Schema schema = new Schema.Parser().parse(new FileInputStream(args[1]));

			/*
			 * GenericRecord Customer = new GenericData.Record(schema);
			 * 
			 * Customer.put("Name", "Eric"); Customer.put("Age", 65); Customer.put("City",
			 * "Mumbai");
			 */
			logger.debug("Creating data for raw and compacted topic");

			// ArrayList<ProducerRecord<String,Customer>> producerRecord = new ArrayList<
			// ProducerRecord<String,Customer>>();
			// ArrayList<ProducerRecord<String, Party>> producerRecord = new
			// ArrayList<ProducerRecord<String, Party>>();

			/*
			 * allConfig.stringPropertyNames().parallelStream().filter(x->x.startsWith(
			 * "data.raw")).forEach( y ->{ String value = allConfig.getProperty(y);
			 * logger.debug("Building data and record for " + y); String att [] =
			 * value.split(","); // Customer data =
			 * 
			 * Customer.newBuilder().setId(Integer.valueOf(att[0])).setName(att[1]).setAge(
			 * Integer.valueOf(att[2])).setCity(att[3]).build(); Party data =
			 * Party.newBuilder().setPartyID(Integer.valueOf(att[0])).setFirstForename(att[1
			 * ]).setSecondForename(att[2]).setSurname(att[3]).setFirstInitial(att[4]).
			 * setSecondInitial(att[5]).setThirdInitial(att[6]).setPartyIndicator(att[7]).
			 * setBirthDate(att[8]).build(); String k = otherprop.get("key"); String out =
			 * otherprop.get("outputTopic"); if(null!=k) { // setting the key
			 * if(k.contains("name")) {
			 * logger.debug("Key to be provided for valid raw is -> "+k);
			 * producerRecord.add(new ProducerRecord<>(out,data.getFirstForename(), data));
			 * } else if(k.contains("id")) {
			 * logger.debug("Key to be provided for valid RAW is -> "+k);
			 * producerRecord.add(new
			 * ProducerRecord<>(out,String.valueOf(data.getPartyID()), data)); }else {
			 * logger.
			 * debug("No key has been set for valid RAW as the value doesnt match name or id"
			 * ); producerRecord.add(new ProducerRecord<>(out, data)); } }else {
			 * logger.debug("No key set for valid RAW as key is null");
			 * producerRecord.add(new ProducerRecord<>(out, data)); } });
			 */
			/*
			 * JSONObject jsonObject = null; String afterTag = null; String eventType =
			 * null; String beforeTag = null; Integer partyId = null;
			 */
			ArrayList<ProducerRecord<String, JsonSchema>> jsonProducerRecord = new ArrayList<ProducerRecord<String, JsonSchema>>();
			allConfig.stringPropertyNames().parallelStream().filter(x -> x.startsWith("json.raw")).forEach(y -> {
				String value = allConfig.getProperty(y);
				logger.debug("Building data and record for " + y);
				System.out.println("hi..we are in json msg " + y);
				System.out.println(value);
				System.out.println(value.length());

				try {
					jsonObject = new JSONObject(value);
					eventType = jsonObject.getJSONObject("header").get("eventType").toString();
					timestampUTC = jsonObject.getJSONObject("header").getString("timestamp_UTC").toString();
					if (eventType.equalsIgnoreCase("INSERT")) {
						afterTag = ((JSONObject) jsonObject.get("after")).get("columns").toString();
						System.out.println("In after Tag " + afterTag);
						JSONArray jsonArrayAfter = new JSONArray(afterTag);
						System.out.println("In jsonAfterArray " + jsonArrayAfter);
						for (int i = 0; i < jsonArrayAfter.length(); i++) {
							boolean flag = false;

							jsonObject = jsonArrayAfter.getJSONObject(i);
							System.out.println("jsonObject " + jsonObject);

							if (jsonObject.get("name").toString() != null
									&& jsonObject.get("name").toString().equals("PTY_ID")) {
								partyId = (Integer) jsonObject.get("value"); // if name is = PTY_ID, then we are
																				// fetching the value in after Tag
																				// columns as PartyId
								System.out.println("fetching partyId  " + partyId);
								flag = true;

							}
							if (flag == true) {
								break;
							}

						}

					} else if (eventType.equalsIgnoreCase("UPDATE")) {
						afterTag = ((JSONObject) jsonObject.get("after")).get("columns").toString();
						beforeTag = ((JSONObject) jsonObject.get("before")).get("columns").toString();
						System.out.println(afterTag);
						System.out.println(beforeTag);

					} else if (eventType.equalsIgnoreCase("DELETE")) {
						beforeTag = ((JSONObject) jsonObject.get("before")).get("columns").toString();
						System.out.println(beforeTag);
					}
					String k = otherprop.get("key");
					String out = otherprop.get("outputTopic");
					// Array jsonArrayAfter = new Array(afterTag);
					/*
					 * String att[]=afterTag.split("columns"); System.out.println(att);
					 */

					/*
					 * columns col =
					 * columns.newBuilder().setName(String.valueOf(jsonObject.get("name")))
					 * .setDataType(String.valueOf(jsonObject.get("dataType")))
					 * .setPartyID(jsonObject.getInt("value")).build();
					 */

					JsonSchema data = JsonSchema.newBuilder().setPartyID(partyId).build();
					if (k != null) {
						if (k.contains("id")) {
							jsonProducerRecord.add(new ProducerRecord<>(out, String.valueOf(partyId), data));
							System.out.println("in jsonProRecord " + jsonProducerRecord);

						} else {
							logger.debug("no key was set as the key does not match with id");
							jsonProducerRecord.add(new ProducerRecord<>(out, data));
						}
					}
					/*
					 * logger.debug("no key was set as the key is null"); jsonProducerRecord.add(new
					 * ProducerRecord<>(out, data));
					 */
					// String[] eventTag= {eventType,afterTag,beforeTag,timestampUTC};

				} catch (JSONException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			});

			// construct kafka producer.

			logger.debug("Creating producers for raw topic");

			// KafkaProducer<String,Customer> producer = new
			// KafkaProducer<String,Customer>(properties);
			// KafkaProducer<String,InputCustomer> compactedProducer = new
			// KafkaProducer<String,InputCustomer>(properties);

			// KafkaProducer<String,Customer> producer = new KafkaProducer<>(properties,
			// Serdes.String().serializer(), customerSerde.serializer());//raw topic
			// KafkaProducer<String,Party> producer = new KafkaProducer<>(properties,
			// Serdes.String().serializer(), partySerde.serializer());//raw topic

			try {

				schedulerToSendMessage.scheduleAtFixedRate(new Messager(otherprop.get("outputTopic"),
						jsonProducerRecord, null, null, properties, jsonSerde, null, null, messagesCount), delay,
						period, TimeUnit.MINUTES);

				/*
				 * KafkaProducer<String,JsonSchema> producer = new KafkaProducer<>(properties,
				 * Serdes.String().serializer(),jsonSerde.serializer()); int i=0;int count=1;
				 * 
				 * 
				 * 
				 * while(i<count) {
				 * 
				 * jsonProducerRecord.forEach(x -> { logger.debug("Data raw to be sent -->"+x);
				 * producer.send(x);
				 * 
				 * }); i++; } producer.close();
				 */

			} catch (SerializationException e) { // may need to do something with it
				logger.debug("Exception Found in Producer " + e.getMessage());
				e.printStackTrace();
			} catch (Exception e1) {
				System.out.println("Exception Found in Producer " + e1.getMessage());
				e1.printStackTrace();
			}

			// When you're finished producing records, you can flush the producer to ensure
			// it has all been written to Kafka and
			// then close the producer to free its resources.

		}

// Compacted Producer

		if (args[1].equals("CP")) {
			int delay = 2;
			int period = 2;

			if (null != args[3])
				delay = Integer.valueOf(args[3]);
			if (null != args[4])
				period = Integer.valueOf(args[4]);

			logger.debug("Setting compacted producer because args ->" + args[1]);
			if (args.length < 3) {
				logger.debug("Please pass 3rd argument as I or U . I for Insert and U for Update");
				System.exit(1);
			}
			Properties properties = new Properties();
			// normal producer

			properties.put("acks", "all");
			properties.put("retries", "10");
			// avro part
			// properties.setProperty("key.serializer", StringSerializer.class.getName());
			// properties.setProperty("value.serializer",
			// KafkaAvroSerializer.class.getName());
			properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					org.apache.kafka.common.serialization.StringSerializer.class.getName());
			properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());

			// SpecificAvroSerde<InputCustomer> inputCustomerSerde = new
			// SpecificAvroSerde<InputCustomer>(); //compacted topic
			SpecificAvroSerde<Individual> compactedSerde = new SpecificAvroSerde<Individual>(); // compacted topic
			HashMap<String, String> otherprop = new HashMap<>();

			if (!allConfig.isEmpty()) {
				logger.debug("Setting producer properties from prop file");
				if (allConfig.getProperty("app") != null && !allConfig.getProperty("app").isBlank())
					properties.put(StreamsConfig.APPLICATION_ID_CONFIG,
							allConfig.getProperty("app") + "compactedproducer");

				if (allConfig.getProperty("bootstrap") != null && !allConfig.getProperty("bootstrap").isBlank())
					properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, allConfig.getProperty("bootstrap")); // confluent
																												// Bootstrap
																												// Servers

				if (allConfig.getProperty("schemaregistry") != null
						&& !allConfig.getProperty("schemaregistry").isBlank()) {
					properties.put("schema.registry.url", allConfig.getProperty("schemaregistry"));// Schema Registry
																									// URL

					Map<String, String> serdeConfig = Collections.singletonMap(
							AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
							allConfig.getProperty("schemaregistry"));

					// inputCustomerSerde.configure(serdeConfig, false);
					compactedSerde.configure(serdeConfig, false);
				}

				if (allConfig.getProperty("mechanism") != null && !allConfig.getProperty("mechanism").isBlank())
					properties.put(SaslConfigs.SASL_MECHANISM, allConfig.getProperty("mechanism"));

				if (allConfig.getProperty("protocol") != null && !allConfig.getProperty("protocol").isBlank())
					properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, allConfig.getProperty("protocol"));

				if (allConfig.getProperty("jaasmodule") != null && !allConfig.getProperty("jaasmodule").isBlank()
						&& allConfig.getProperty("jaasuser") != null && !allConfig.getProperty("jaasuser").isBlank()
						&& allConfig.getProperty("jaaspwd") != null && !allConfig.getProperty("jaaspwd").isBlank())
					properties.put(SaslConfigs.SASL_JAAS_CONFIG,
							allConfig.getProperty("jaasmodule") + " required username=\""
									+ allConfig.getProperty("jaasuser") + "\" password=\""
									+ allConfig.getProperty("jaaspwd") + "\";");

				otherprop.put("compactedTopic", allConfig.getProperty("compactedTopic"));

				if (allConfig.getProperty("key") != null && !allConfig.getProperty("key").isBlank())
					otherprop.put("key", allConfig.getProperty("key"));

			} else {

				logger.debug("Setting default producer properties");
				properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-t0618compactedproducer");
				properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9071"); // confluent Bootstrap Servers
				properties.put("schema.registry.url", "http://schemaregistry:8081");// Schema Registry URL
				Map<String, String> serdeConfig = Collections.singletonMap(
						AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8081");

				// inputCustomerSerde.configure(serdeConfig, false);
				compactedSerde.configure(serdeConfig, false);
				properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
				properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
				properties.put(SaslConfigs.SASL_JAAS_CONFIG,
						"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");

				otherprop.put("compactedTopic", "compactedTopic");

			}

			// Schema schema = new Schema.Parser().parse(new FileInputStream(args[1]));

			/*
			 * GenericRecord Customer = new GenericData.Record(schema);
			 * 
			 * Customer.put("Name", "Eric"); Customer.put("Age", 65); Customer.put("City",
			 * "Mumbai");
			 */
			logger.debug("Creating data for  compacted topic");

			/// ArrayList<ProducerRecord<String,InputCustomer>> compactedProducerRecord =
			/// new ArrayList< ProducerRecord<String,InputCustomer>>();
			ArrayList<ProducerRecord<String, Individual>> compactedProducerRecord = new ArrayList<ProducerRecord<String, Individual>>();
			String dataToSearch = args[2];

			logger.debug("Producer will send records starting with property -> " + "json.compacted."
					+ dataToSearch.trim().toLowerCase());

			allConfig.stringPropertyNames().parallelStream()
					.filter(x -> x.startsWith("json.compacted." + dataToSearch.trim().toLowerCase())).forEach(y -> {
						String value = allConfig.getProperty(y);
						logger.debug("Building data and record for " + y);
						try {
							jsonObject = new JSONObject(value);
							partyId = (Integer) jsonObject.getJSONObject("individual").get("partyID");
							String title = jsonObject.getJSONObject("individual").get("title").toString();
							List<String> foreNames = List
									.of(jsonObject.getJSONObject("individual").get("forenames").toString());
							List<String> initials = List
									.of(jsonObject.getJSONObject("individual").get("initials").toString());
							String lastname = jsonObject.getJSONObject("individual").get("lastname").toString();
							String generationSuffix = jsonObject.getJSONObject("individual").get("generationSuffix")
									.toString();
							String suffixTitle = jsonObject.getJSONObject("individual").get("suffixTitle").toString();
							String salutation = jsonObject.getJSONObject("individual").get("salutation").toString();
							String gender = jsonObject.getJSONObject("individual").get("gender").toString();
							String birthDate = jsonObject.getJSONObject("individual").get("birthDate").toString();
							String maritalStatus = jsonObject.getJSONObject("individual").get("maritalStatus")
									.toString();
							String deathDate = jsonObject.getJSONObject("individual").get("deathDate").toString();
							String deathNotificationDate = jsonObject.getJSONObject("individual")
									.get("deathNotificationDate").toString();
							Individual data = Individual.newBuilder().setPartyID(partyId).setTitle(title)
									.setForenames(foreNames).setInitials(initials).setLastname(lastname)
									.setGenerationSuffix(generationSuffix).setSuffixTitle(suffixTitle)
									.setSalutation(salutation).setGender(gender).setBirthDate(birthDate)
									.setMaritalStatus(maritalStatus).setDeathDate(deathDate)
									.setDeathNotificationDate(deathNotificationDate).build();
							// PartyIdentifierDetails party=PartyIdentifierDetails.
							String k = otherprop.get("key");
							String out = otherprop.get("compactedTopic");
							System.out.println("we are in compacted topic after creation of compacted topic " + out);
							if (null != k) {
								// setting the InputCustomer record key
								logger.debug("Key to be provided for compacted is -> " + k);
								if (k.contains("id")) {

									logger.debug("Key to be provided for compacted is -> " + k);
									compactedProducerRecord
											.add(new ProducerRecord<>(out, String.valueOf(data.getPartyID()), data));
									logger.debug("copmpacted Record Produced is " + compactedProducerRecord);
									// KafkaProducer<String,Address> compactedProducer = new
									// KafkaProducer<>(properties, Serdes.String().serializer(),
									// addressSerde.serializer()); //compacted topic

									/*
									 * try (KafkaProducer<String,Address> compactedProducer = new
									 * KafkaProducer<>(properties, Serdes.String().serializer(),
									 * addressSerde.serializer())) { compactedProducerRecord.forEach(x -> {
									 * logger.debug("Data compacted to be sent -->"+x); compactedProducer.send(x);
									 * 
									 * }); }
									 * 
									 */

								}

					else {

									logger.debug(
											"No key has been set as the value doesnt match name or id for compacted");
									compactedProducerRecord.add(new ProducerRecord<>(out, data));

								}
							} else {
								logger.debug("No key set for compacted as key is null");
								compactedProducerRecord.add(new ProducerRecord<>(out, data));
							}
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						// String att[] = value.split(",");
						// setting the InputCustomer record value Input customer is POJO for compaced
						// topic
						// InputCustomer data =
						// InputCustomer.newBuilder().setId(Integer.valueOf(att[0])).setFirstName(att[1]).setLastName(att[2]).setAddress(att[3]).setEmail(att[4]).setLevel(att[5]).build();

						// String att [] = value.split(":");

					});

			// construct kafka producer.

			logger.debug("Creating producers for  compacted topic");

			// KafkaProducer<String,Customer> producer = new
			// KafkaProducer<String,Customer>(properties);
			// KafkaProducer<String,InputCustomer> compactedProducer = new
			// KafkaProducer<String,InputCustomer>(properties);

			// KafkaProducer<String,InputCustomer> compactedProducer = new
			// KafkaProducer<>(properties, Serdes.String().serializer(),
			// inputCustomerSerde.serializer()); //compacted topic

			// KafkaProducer<String,Address> compactedProducer = new
			// KafkaProducer<>(properties, Serdes.String().serializer(),
			// addressSerde.serializer()); //compacted topic

			try {

				schedulerToSendMessage.scheduleAtFixedRate(new Messager(otherprop.get("compactedTopic"), null,
						compactedProducerRecord, null, properties, null, compactedSerde, null, 0), delay, period,
						TimeUnit.MINUTES);

			} catch (SerializationException e) { // may need to do something with it
				logger.debug("Exception Found in Producer " + e.getMessage());
				e.printStackTrace();
			} catch (Exception e1) {
				logger.debug("Exception Found in Producer " + e1.getMessage());
				e1.printStackTrace();
			}

		}
		if (args[1].equals("CP1")) {
			int delay = 2;
			int period = 2;

			if (null != args[3])
				delay = Integer.valueOf(args[3]);
			if (null != args[4])
				period = Integer.valueOf(args[4]);

			logger.debug("Setting compacted producer because args ->" + args[1]);
			if (args.length < 3) {
				logger.debug("Please pass 3rd argument as I or U . I for Insert and U for Update");
				System.exit(1);
			}
			Properties properties = new Properties();
			// normal producer

			properties.put("acks", "all");
			properties.put("retries", "10");
			// avro part
			// properties.setProperty("key.serializer", StringSerializer.class.getName());
			// properties.setProperty("value.serializer",
			// KafkaAvroSerializer.class.getName());
			properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					org.apache.kafka.common.serialization.StringSerializer.class.getName());
			properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());

			// SpecificAvroSerde<InputCustomer> inputCustomerSerde = new
			// SpecificAvroSerde<InputCustomer>(); //compacted topic
			SpecificAvroSerde<ExternalIdentifiers> addressSerde = new SpecificAvroSerde<ExternalIdentifiers>(); // compacted
																												// topic
			HashMap<String, String> otherprop = new HashMap<>();

			if (!allConfig.isEmpty()) {
				logger.debug("Setting producer properties from prop file");
				if (allConfig.getProperty("app") != null && !allConfig.getProperty("app").isBlank())
					properties.put(StreamsConfig.APPLICATION_ID_CONFIG,
							allConfig.getProperty("app") + "compactedproducer");

				if (allConfig.getProperty("bootstrap") != null && !allConfig.getProperty("bootstrap").isBlank())
					properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, allConfig.getProperty("bootstrap")); // confluent
																												// Bootstrap
																												// Servers

				if (allConfig.getProperty("schemaregistry") != null
						&& !allConfig.getProperty("schemaregistry").isBlank()) {
					properties.put("schema.registry.url", allConfig.getProperty("schemaregistry"));// Schema Registry
																									// URL

					Map<String, String> serdeConfig = Collections.singletonMap(
							AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
							allConfig.getProperty("schemaregistry"));

					// inputCustomerSerde.configure(serdeConfig, false);
					addressSerde.configure(serdeConfig, false);
				}

				if (allConfig.getProperty("mechanism") != null && !allConfig.getProperty("mechanism").isBlank())
					properties.put(SaslConfigs.SASL_MECHANISM, allConfig.getProperty("mechanism"));

				if (allConfig.getProperty("protocol") != null && !allConfig.getProperty("protocol").isBlank())
					properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, allConfig.getProperty("protocol"));

				if (allConfig.getProperty("jaasmodule") != null && !allConfig.getProperty("jaasmodule").isBlank()
						&& allConfig.getProperty("jaasuser") != null && !allConfig.getProperty("jaasuser").isBlank()
						&& allConfig.getProperty("jaaspwd") != null && !allConfig.getProperty("jaaspwd").isBlank())
					properties.put(SaslConfigs.SASL_JAAS_CONFIG,
							allConfig.getProperty("jaasmodule") + " required username=\""
									+ allConfig.getProperty("jaasuser") + "\" password=\""
									+ allConfig.getProperty("jaaspwd") + "\";");

				otherprop.put("compactedTopic1", allConfig.getProperty("compactedTopic1"));

				if (allConfig.getProperty("key") != null && !allConfig.getProperty("key").isBlank())
					otherprop.put("key", allConfig.getProperty("key"));

			} else {

				logger.debug("Setting default producer properties");
				properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-t0618compactedproducer");
				properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9071"); // confluent Bootstrap Servers
				properties.put("schema.registry.url", "http://schemaregistry:8081");// Schema Registry URL
				Map<String, String> serdeConfig = Collections.singletonMap(
						AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8081");

				// inputCustomerSerde.configure(serdeConfig, false);
				addressSerde.configure(serdeConfig, false);
				properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
				properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
				properties.put(SaslConfigs.SASL_JAAS_CONFIG,
						"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");

				otherprop.put("compactedTopic1", "compactedTopic1");

			}

			// Schema schema = new Schema.Parser().parse(new FileInputStream(args[1]));

			/*
			 * GenericRecord Customer = new GenericData.Record(schema);
			 * 
			 * Customer.put("Name", "Eric"); Customer.put("Age", 65); Customer.put("City",
			 * "Mumbai");
			 */
			logger.debug("Creating data for  compacted topic1");

			/// ArrayList<ProducerRecord<String,InputCustomer>> compactedProducerRecord =
			/// new ArrayList< ProducerRecord<String,InputCustomer>>();
			ArrayList<ProducerRecord<String, ExternalIdentifiers>> compactedProducerRecord = new ArrayList<ProducerRecord<String, ExternalIdentifiers>>();
			String dataToSearch = args[2];

			logger.debug("Producer will send records starting with property -> " + "json.compacted1."
					+ dataToSearch.trim().toLowerCase());

			allConfig.stringPropertyNames().parallelStream()
					.filter(x -> x.startsWith("json.compacted1." + dataToSearch.trim().toLowerCase())).forEach(y -> {
						String value = allConfig.getProperty(y);
						logger.debug("Building data and record for " + y);
						String extPartyId = null;
						Integer extSystemId = null;
						String createTimestamp = null;
						ExternalIdentifiers data = null;

						try {
							jsonObject = new JSONObject(value);
							String externalIdentifiers = jsonObject.get("externalIdentifiers").toString();
							JSONArray jsonArrayAfter = new JSONArray(externalIdentifiers);
							System.out.println("In jsonAfterArray " + jsonArrayAfter);
							for (int i = 0; i < jsonArrayAfter.length(); i++) {
								jsonObject = jsonArrayAfter.getJSONObject(i);
								extPartyId = jsonObject.get("externalPartyId").toString();
								extSystemId = (Integer) jsonObject.get("externalSystemId");
								createTimestamp = jsonObject.get("createTimestamp").toString();
								data = ExternalIdentifiers.newBuilder().setExternalPartyId(extPartyId)
										.setExternalSystemId(extSystemId).setCreateTimestamp(createTimestamp).build();
								break;
							}
								// String att [] = value.split(",");
								// setting the InputCustomer record value Input customer is POJO for compaced
								// topic
								// InputCustomer data =
								// InputCustomer.newBuilder().setId(Integer.valueOf(att[0])).setFirstName(att[1]).setLastName(att[2]).setAddress(att[3]).setEmail(att[4]).setLevel(att[5]).build();

								String k = otherprop.get("key");
								String out = otherprop.get("compactedTopic1");
								if (null != k) {
									// setting the InputCustomer record key
									logger.debug("Key to be provided for compacted is -> " + k);
									if (k.contains("name")) {
										logger.debug("setting key as firstname");
										compactedProducerRecord
												.add(new ProducerRecord<>(out, String.valueOf(data.getExternalPartyId()), data));
									} else if (k.contains("id")) {

										logger.debug("Key to be provided for compacted is -> " + k);
										compactedProducerRecord.add(new ProducerRecord<>(out,
												String.valueOf(data.getExternalPartyId()), data));

									}

					else {

										logger.debug(
												"No key has been set as the value doesnt match name or id for compacted");
										compactedProducerRecord.add(new ProducerRecord<>(out, data));

									}
								} else {
									logger.debug("No key set for compacted as key is null");
									compactedProducerRecord.add(new ProducerRecord<>(out, data));
								}
							
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					});

			// construct kafka producer.

			logger.debug("Creating producers for  compacted topic1");

			// KafkaProducer<String,Customer> producer = new
			// KafkaProducer<String,Customer>(properties);
			// KafkaProducer<String,InputCustomer> compactedProducer = new
			// KafkaProducer<String,InputCustomer>(properties);

			// KafkaProducer<String,InputCustomer> compactedProducer = new
			// KafkaProducer<>(properties, Serdes.String().serializer(),
			// inputCustomerSerde.serializer()); //compacted topic

			// KafkaProducer<String,Address> compactedProducer = new
			// KafkaProducer<>(properties, Serdes.String().serializer(),
			// addressSerde.serializer()); //compacted topic

			try {

				schedulerToSendMessage.scheduleAtFixedRate(new Messager(otherprop.get("compactedTopic1"), null, null,
						compactedProducerRecord, properties, null, null, addressSerde, 0), delay, period,
						TimeUnit.MINUTES);

			} catch (SerializationException e) {
				// may need to do something with it
				logger.debug("Exception Found in Producer " + e.getMessage());
				e.printStackTrace();
			} catch (Exception e1) {
				logger.debug("Exception Found in Producer " + e1.getMessage());
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

	/*
	 * private static <K extends IndividualPartyDetails,V extends
	 * ExternalIdentifiers> ValueJoiner<K,V,Enriched4> merge() { // TODO
	 * Auto-generated method stub return
	 * (a,b)->{a.setPartyId((Integer)(b.getExternalPartyId()); }
	 */
}
