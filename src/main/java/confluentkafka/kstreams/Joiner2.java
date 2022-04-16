package confluentkafka.kstreams;


import com.training.Address;
import com.training.Enriched;
import com.training.Enriched2;
import com.training.Individual;
import com.training.JsonSchema;
import com.training.Party;



import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.log4j.Logger;

public class Joiner2 implements ValueJoiner<JsonSchema, Individual, Enriched2>{
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
	  public Enriched2 apply(JsonSchema value1, Individual value2) { 
		  // TODO Auto-generated method stub
			/*
			 * logger.debug("Joining valid data and Ktable data -> "+value1.getColumns().get
			 * (0)+" "+value1.getColumns().get(2)+" "+value2.getAddressCareOfName()+" "
			 * +value2.getPartyAddress());
			 */
	  logger.debug("Joining valid data and Ktable data -> "+value1.getPartyID() +" "+value2.getLastname()+" "+value2.getGenerationSuffix());
	  return Enriched2.newBuilder().setPartyID(null!=value1.getPartyID() ? value1.getPartyID():null).setTitle(null!=value2.getTitle()?value2.getTitle():null).setBirthDate(null!=value2.getBirthDate() ?value2.getBirthDate():null)
			  .setLastname(null!=value2.getLastname()?value2.getLastname():null).setGenerationSuffix(null!=value2.getGenerationSuffix()?value2.getGenerationSuffix():null).setForenames(null!=value2.getForenames()?value2.getForenames():null)
			  .setInitials(null!=value2.getInitials()?value2.getInitials():null).setGender(null!=value2.getGender()?value2.getGender():null).setMaritalStatus(null!=value2.getMaritalStatus()?value2.getMaritalStatus():null)
			  .setSalutation(null!=value2.getSalutation()?value2.getSalutation():null).setSuffixTitle(null!=value2.getSuffixTitle()?value2.getSuffixTitle():null).setDeathDate(null!=value2.getDeathDate()?value2.getDeathDate():null)
			  .setDeathNotificationDate(null!=value2.getDeathNotificationDate()?value2.getDeathNotificationDate():null)
			  .build();
			   }
	 

}

