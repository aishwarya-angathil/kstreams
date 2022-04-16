package confluentUtils;

import java.io.Serializable;

public class ExternalSystemPartyIdentifier  implements Serializable {
	private String externalPartyId;
	private String externalSystemId;
	public String getExternalPartyId() {
		return externalPartyId;
	}
	public void setExternalPartyId(String externalPartyId) {
		this.externalPartyId = externalPartyId;
	}
	public String getExternalSystemId() {
		return externalSystemId;
	}
	public void setExternalSystemId(String externalSystemId) {
		this.externalSystemId = externalSystemId;
	}
	@Override
	public String toString() {
		return "ExternalSystemPartyIdentifier [externalPartyId=" + externalPartyId + ", externalSystemId="
				+ externalSystemId + "]";
	}
	}
