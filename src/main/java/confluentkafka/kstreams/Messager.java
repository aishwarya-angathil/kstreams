package confluentkafka.kstreams;

import java.util.ArrayList;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.training.Address;
import com.training.Party;
public class Messager implements Runnable{
	private ArrayList<ProducerRecord<String,Party>> records;
	private KafkaProducer<String,Party> producer;
	private ArrayList<ProducerRecord<String,Address>> compactedrecords;
	private KafkaProducer<String,Address> compactedproducer;
	private int count ;
	static Logger logger = Logger.getLogger(Messager.class);

	public Messager(ArrayList<ProducerRecord<String,Party>> records, KafkaProducer<String,Party> producer,ArrayList<ProducerRecord<String,Address>> compactedrecords, KafkaProducer<String,Address> compactedproducer,int count ) {
		super();
		this.records = records;
		this.producer = producer;
		this.compactedrecords=compactedrecords;
		this.compactedproducer = compactedproducer;
		this.count=count;
	
	}


	@Override
	public void run() {
		// TODO Auto-generated method stub
		if(producer!=null && records!=null) {
		int i = 0;
    	while(i<this.count) {
    		logger.debug("Sending data to raw topic");
    		logger.debug("Total records to be sent to raw topic ->"+records.size());
    		this.records.forEach(x -> {
    			logger.debug("Data raw to be sent -->"+x);
    			this.producer.send(x);
    		
        	});
    	  i++;
    	}
		}
		
		if(compactedproducer!=null && compactedrecords!=null) {
			logger.debug("Sending data to compacted topic");
	    		logger.debug("Total records to be sent to compacted topic ->"+compactedrecords.size());
	    		this.compactedrecords.forEach(x -> {
	    			logger.debug("Data compacted to be sent -->"+x);
	    			this.compactedproducer.send(x);
	    		
	        	});
			}
		
	}

}
