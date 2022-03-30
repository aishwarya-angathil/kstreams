package confluentkafka.kstreams;


import com.training.Address;
import com.training.CompactedSchema;
import com.training.Enriched;
import com.training.Enriched2;
import com.training.Enriched3;
import com.training.individual;
import com.training.JsonSchema;
import com.training.Party;

import confluentUtils.PartyService;

import java.util.List;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.log4j.Logger;

public class Joiner2 implements ValueJoiner<JsonSchema, PartyService, Enriched3>{
	 static Logger logger = Logger.getLogger(Joiner2.class);
		/*
		 * public Enriched apply(Party value1, Address value2) { // TODO Auto-generated
		 * method stub
		 * logger.debug("Joining valid data and Ktable data -> "+value1.getFirstForename
		 * ()+"  "+value1.getSurname()+" "+value1.getBirthDate()+" "+
		 * value1.getPartyID()+" "+value2.getAddressCareOfName()+" "+value2.
		 * getPartyAddress()); return Enriched.newBuilder()
		 * .setForename((null!=value1.getFirstForename() ?
		 * value1.getFirstForename():""+" "+null != value1.getSecondForename()?
		 * value1.getSecondForename() : "").trim())
		 * .setSurname((null!=value1.getSurname()? value1.getSurname():"").trim())
		 * .setBirthDate((null!= value1.getBirthDate()?
		 * value1.getBirthDate():"").trim())
		 * .setInitials((null!=value1.getFirstInitial()? value1.getFirstInitial()+","
		 * :""+null!=value1.getSecondInitial()? value1.getSecondInitial()+",":""
		 * +null!=value1.getThirdInitial()? value1.getThirdInitial():"").trim())
		 * .setAddressCareOfName((null!=value2.getAddressCareOfName()?value2.
		 * getAddressCareOfName():"").trim())
		 * .setPartyAddress((null!=value2.getPartyAddress()?
		 * value2.getPartyAddress():"").trim())
		 * .setPartyID((null!=value1.getPartyID()?value1.getPartyID():null)) .build(); }
		 */
	
	  @Override 
	  public Enriched3 apply(JsonSchema value1,  PartyService value2) { 
		  // TODO Auto-generated method stub
			/*
			 * logger.debug("Joining valid data and Ktable data -> "+value1.getColumns().get
			 * (0)+" "+value1.getColumns().get(2)+" "+value2.getAddressCareOfName()+" "
			 * +value2.getPartyAddress());
			 */
	  logger.debug("Joining valid data and Ktable data -> "+value1.getPartyID() +" "+value2.getExternalIdentifiers()+" "+value2.getIndividual());
	  return Enriched3.newBuilder().setPartyID(null!=value1.getPartyID() ? value1.getPartyID():null).setExternalIdentifiers(null)
			  .setExternalIdentifiers(null!=value2.getExternalIdentifiers()? List.of(value2.get.getExternalIdentifiers()):null).

			  .build();
			   }
	 

}

