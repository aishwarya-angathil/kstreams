package confluentUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.training.Address;
import com.training.Individual;





public class CommonUtils { 
	public static IndividualPartyDetails getIndvDetails(Individual ind) {
		SimpleDateFormat  sdf=new SimpleDateFormat("yyyy-MM-dd");
		IndividualPartyDetails indvPartyDetails=new IndividualPartyDetails();
		indvPartyDetails.setPartyId(ind.getPartyID());
		indvPartyDetails.setPartyType("INDIVIDUAL");
		indvPartyDetails.setTitle(ind.getTitle());
		indvPartyDetails.setForeNames(ind.getForenames());
		indvPartyDetails.setIntials(ind.getInitials());
		indvPartyDetails.setLastName(ind.getLastname());
		indvPartyDetails.setGenerationSuffix(ind.getGenerationSuffix());
		indvPartyDetails.setSuffixTitle(ind.getSuffixTitle());
		indvPartyDetails.setSalutation(ind.getSalutation());
		indvPartyDetails.setGender(ind.getGender());
		indvPartyDetails.setMaritalStatus(ind.getMaritalStatus());		
		try {
			indvPartyDetails.setBirthDate(sdf.parse(ind.getBirthDate()));
			indvPartyDetails.setDeathDate(sdf.parse(ind.getDeathDate()));
			indvPartyDetails.setDeathNotificationDate(sdf.parse(ind.getDeathNotificationDate()));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return indvPartyDetails;		
	}	
	public static List<PartyIdentifierDetails> getIdentifierDetails(List<ExternalSystemPartyIdentifier> extPartyList){
		//PartyIdentifierDetails partyIdentifierDetails=null;
		List<PartyIdentifierDetails> identifierList=new ArrayList<PartyIdentifierDetails>();
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date date=new Date();
		for(ExternalSystemPartyIdentifier externalList:extPartyList) {
			PartyIdentifierDetails partyIdentifierDetails=new PartyIdentifierDetails();
			partyIdentifierDetails.setExternalPartyID(externalList.getExternalPartyId());
			partyIdentifierDetails.setExternalSystemId(externalList.getExternalSystemId());
			String createTimestamp=sdf.format(date);
			partyIdentifierDetails.setCreateTimestamp(createTimestamp);
			identifierList.add(partyIdentifierDetails);		
		}		
		return identifierList;	
		}
	}
