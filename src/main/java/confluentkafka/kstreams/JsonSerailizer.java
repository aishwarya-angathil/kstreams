package confluentkafka.kstreams;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerailizer<T> implements Serializer<T> {
	private ObjectMapper om=new ObjectMapper();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] serialize(String topic, T data) {
		// TODO Auto-generated method stub
		byte[] retVal=null;
		
		try {
			System.out.println(data.getClass());
			retVal=om.writeValueAsString(data).getBytes();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retVal;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
