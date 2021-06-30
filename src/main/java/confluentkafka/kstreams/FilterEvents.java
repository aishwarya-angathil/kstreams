/*package confluentkafka.kstreams;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;


public class FilterEvents {
  
  public static void main(final String[] args) throws Exception {
	    Properties config = new Properties();
	    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
	    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092");
	    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	    config.put("schema.registry.url", "http://localhost:8081");
	    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

	    StreamsBuilder builder = new StreamsBuilder();
	    KStream<String, String> textLines = builder.stream("TextLinesTopic");
	   
	    KafkaStreams streams = new KafkaStreams(builder, config);
	    streams.start();

	    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	  }
}
*/