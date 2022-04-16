package confluentkafka.kstreams;


import com.training.Address;
import com.training.Enriched;
import com.training.Enriched3;
import com.training.Enriched4;
import com.training.ExternalIdentifiers;
import com.training.JsonSchema;
import com.training.Party;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.log4j.Logger;

public class Joiner implements ValueJoiner<JsonSchema, ExternalIdentifiers, Enriched3>{
	 static Logger logger = Logger.getLogger(Joiner.class);
	public Enriched3 apply(JsonSchema value1, ExternalIdentifiers value2) {
		// TODO Auto-generated method stub
		logger.debug("Joining valid data and Ktable data -> "+value1.getPartyID()+" "+ value2.getCreateTimestamp()+" "+value2.getExternalSystemId()+" "+value2.getExternalPartyId());
		return Enriched3.newBuilder().setPartyID(null!=value1.getPartyID() ? value1.getPartyID():null).setExternalPartyId(null!=value2.getExternalPartyId()?value2.getExternalPartyId():null)
		  .setExternalSystemId(null!=value2.getExternalSystemId()?value2.getExternalSystemId():null).setCreateTimestamp(null!=value2.getCreateTimestamp()?value2.getCreateTimestamp():null)
		  .build();
	}

}
