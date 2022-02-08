package confluentkafka.kstreams;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonDeserializer<T> implements Deserializer<T> {
	private ObjectMapper om=new ObjectMapper();
	private Class<T> type;

	public JsonDeserializer() {
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public T deserialize(String topic, byte[] bytes) {
		// TODO Auto-generated method stub
		T data=null;
		if(bytes==null || bytes.length==0) {
			return null;
		}
		try {
			System.out.println(getType());
			data=om.readValue(bytes, type);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	protected Class<T> getType() {
		// TODO Auto-generated method stub
		return type;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
