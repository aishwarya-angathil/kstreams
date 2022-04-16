package confluentUtils;

import org.apache.kafka.streams.kstream.KStream;

import com.training.Enriched3;

public class ExtrenalDetails {
	private KStream<String, Enriched3> rawExternal=null;

	public KStream<String, Enriched3> getRawExternal() {
		return rawExternal;
	}

	public void setRawExternal(KStream<String, Enriched3> rawExternal) {
		this.rawExternal = rawExternal;
	}

	@Override
	public String toString() {
		return "ExtrenalDetails {rawExternal=" + rawExternal + "}";
	}
	
	

}
