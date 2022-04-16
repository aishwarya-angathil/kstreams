package confluentkafka.kstreams;


import com.training.Address;
import com.training.Enriched;
import com.training.Enriched2;
import com.training.Enriched3;
import com.training.Enriched4;
import com.training.JsonSchema;
import com.training.Party;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.log4j.Logger;

public class Joiner3 implements ValueJoiner<Enriched2, Enriched3, Enriched4>{
	 static Logger logger = Logger.getLogger(Joiner3.class);
	public Enriched4 apply(Enriched2 value1, Enriched3 value2) {
		// TODO Auto-generated method stub
		logger.debug("Joining valid data and Ktable data -> "+value1.getPartyID()+"  "+value1.getLastname()+" "+value1.getGender()+" "+ value2.getCreateTimestamp()+" "+value1.getSalutation()+" "+value2.getExternalSystemId());
		return Enriched4.newBuilder().setPartyID(null!=value1.getPartyID() ? value1.getPartyID():null).setTitle(null!=value1.getTitle()?value1.getTitle():null).setBirthDate(null!=value1.getBirthDate() ?value1.getBirthDate():null)
		  .setLastname(null!=value1.getLastname()?value1.getLastname():null).setGenerationSuffix(null!=value1.getGenerationSuffix()?value1.getGenerationSuffix():null).setForenames(null!=value1.getForenames()?value1.getForenames():null)
		  .setInitials(null!=value1.getInitials()?value1.getInitials():null).setGender(null!=value1.getGender()?value1.getGender():null).setMaritalStatus(null!=value1.getMaritalStatus()?value1.getMaritalStatus():null)
		  .setSalutation(null!=value1.getSalutation()?value1.getSalutation():null).setSuffixTitle(null!=value1.getSuffixTitle()?value1.getSuffixTitle():null).setDeathDate(null!=value1.getDeathDate()?value1.getDeathDate():null)
		  .setDeathNotificationDate(null!=value1.getDeathNotificationDate()?value1.getDeathNotificationDate():null).setExternalPartyId(null!=value2.getExternalPartyId()?value2.getExternalPartyId():null)
		  .setExternalSystemId(null!=value2.getExternalSystemId()?value2.getExternalSystemId():null).setCreateTimestamp(null!=value2.getCreateTimestamp()?value2.getCreateTimestamp():null)
		  .build();
	}

}
