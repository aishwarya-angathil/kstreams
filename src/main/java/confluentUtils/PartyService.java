package confluentUtils;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import com.training.Enriched2;
import com.training.Enriched3;
import com.training.ExternalIdentifiers;
import com.training.Individual;

public class PartyService {
	//private String PTY_Id =null;
	private IndividualDetails individual=new IndividualDetails();
	private List<ExtrenalDetails> externalIdentifiers=new ArrayList<ExtrenalDetails>();
	
	public IndividualDetails getIndividual() {
		return individual;
	}
	public void setIndividual(IndividualDetails individual) {
		this.individual = individual;
	}
	public List<ExtrenalDetails> getExternalIdentifiers() {
		return externalIdentifiers;
	}
	public void setExternalIdentifiers(List<ExtrenalDetails> externalIdentifiers) {
		this.externalIdentifiers = externalIdentifiers;
	}
	@Override
	public String toString() {
		return "PartyService [individual=" + individual + ", externalIdentifiers=" + externalIdentifiers + "]";
	}
	
	
		/*
	 * @Override public String toString() { return "PartyService [PTY_Id=" + PTY_Id
	 * + ", individual=" + individual + ", externalIdentifiers=" +
	 * externalIdentifiers + "]"; }
	 */
	
	}
