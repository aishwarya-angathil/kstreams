package confluentUtils;

import java.util.ArrayList;
import java.util.List;

public class PartyService {
	private IndividualPartyDetails individual=new IndividualPartyDetails();
	private List<PartyIdentifierDetails> externalIdentifiers=new ArrayList<PartyIdentifierDetails>();
	
	public IndividualPartyDetails getIndividual() {
		return individual;
	}
	public void setIndividual(IndividualPartyDetails individual) {
		this.individual = individual;
	}
	public List<PartyIdentifierDetails> getExternalIdentifiers() {
		return externalIdentifiers;
	}
	public void setExternalIdentifiers(List<PartyIdentifierDetails> externalIdentifiers) {
		this.externalIdentifiers = externalIdentifiers;
	}
	@Override
	public String toString() {
		return "PartyService [individual=" + individual + ", externalIdentifiers=" + externalIdentifiers + "]";
	}
	}
