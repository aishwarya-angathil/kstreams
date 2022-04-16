package confluentUtils;

public class PartyIdentifierDetails {
	private String externalPartyID=null;
	private String externalSystemId=null;
	private String createTimestamp=null;
	public String getExternalPartyID() {
		return externalPartyID;
	}
	public void setExternalPartyID(String externalPartyID) {
		this.externalPartyID = externalPartyID;
	}
	public String getExternalSystemId() {
		return externalSystemId;
	}
	public void setExternalSystemId(String externalSystemId) {
		this.externalSystemId = externalSystemId;
	}
	public String getCreateTimestamp() {
		return createTimestamp;
	}
	public void setCreateTimestamp(String createTimestamp) {
		this.createTimestamp = createTimestamp;
	}
	@Override
	public String toString() {
		return "external Identifiers [externalPartyID=" + externalPartyID + ", externalSystemId=" + externalSystemId
				+ ", createTimestamp=" + createTimestamp + "]";
	}
	
	
	

}
