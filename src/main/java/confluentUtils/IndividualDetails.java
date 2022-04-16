package confluentUtils;

import org.apache.kafka.streams.kstream.KStream;

import com.training.Enriched2;

public class IndividualDetails { 
	private KStream<String, Enriched2> rawIndividual=null;

	public KStream<String, Enriched2> getRawIndividual() {
		return rawIndividual;
	}

	public void setRawIndividual(KStream<String, Enriched2> rawIndividual) {
		this.rawIndividual = rawIndividual;
	}

	@Override
	public String toString() {
		return "IndividualDetails {rawIndividual=" + rawIndividual + "}";
	}
	

	
	

}

