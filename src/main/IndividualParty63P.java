package confluentUtils;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

public class IndividualParty63P {
	ObjectMapper mapper= new ObjectMapper();
	PartyService partyService=new PartyService();

	
	String result=null;
	Integer partyId=null;
	String partyIdentifier=null;	
	List<PartyIdentifierDetails> identifierList=new ArrayList<PartyIdentifierDetails>();
	IndividualPartyDetails indPartyDetails=new IndividualPartyDetails();
	
	//indPartyDetails
	/*
	 * partyService. 
	 * partyService.set
	 */
	/*
	 * if(partyIdentifier!=null) {
	 * partyService.setExternalIdentifiers(identifierList);
	 * partyService.setIndividual(indPartyDetails);
	 * 
	 * 
	 * }
	 */
}

