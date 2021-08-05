package confluentkafka.kstreams;

import com.training.Customer;
import com.training.InputCustomer;
import com.training.UpdatedCustomer;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class Joiner implements ValueJoiner<Customer, InputCustomer, UpdatedCustomer>{

	@Override
	public UpdatedCustomer apply(Customer value1, InputCustomer value2) {
		// TODO Auto-generated method stub
		return UpdatedCustomer.newBuilder()
	    		.setFirstName(value1.getName())
	            .setLastName(value2.getLastName())
	            .setAge(value1.getAge())
	            .setCity(value1.getCity())
	            .build();
	}

}
