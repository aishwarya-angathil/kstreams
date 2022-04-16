package confluentUtils;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class IndividualPartyDetails implements Serializable {
	private Integer partyId=null;
	private String partyType=null;
	private String title=null;
	private List<String> foreNames;
	private List<String> intials;
	private String lastName=null;
	private String generationSuffix=null;
	private String suffixTitle=null;
	private String salutation=null;
	private String gender=null;
	private Date birthDate=null;
	private String maritalStatus=null;
	private Date deathDate=null;
	private Date deathNotificationDate=null;
	
	public Integer getPartyId() {
		return partyId;
	}
	public void setPartyId(Integer partyId) {
		this.partyId = partyId;
	}
	public String getPartyType() {
		return partyType;
	}
	public void setPartyType(String partyType) {
		this.partyType = partyType;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public List<String> getForeNames() {
		return foreNames;
	}
	public void setForeNames(List<String> foreNames) {
		this.foreNames = foreNames;
	}
	public List<String> getIntials() {
		return intials;
	}
	public void setIntials(List<String> intials) {
		this.intials = intials;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getGenerationSuffix() {
		return generationSuffix;
	}
	public void setGenerationSuffix(String generationSuffix) {
		this.generationSuffix = generationSuffix;
	}
	public String getSuffixTitle() {
		return suffixTitle;
	}
	public void setSuffixTitle(String suffixTitle) {
		this.suffixTitle = suffixTitle;
	}
	public String getSalutation() {
		return salutation;
	}
	public void setSalutation(String salutation) {
		this.salutation = salutation;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public Date getBirthDate() {
		return birthDate;
	}
	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate;
	}
	public String getMaritalStatus() {
		return maritalStatus;
	}
	public void setMaritalStatus(String maritalStatus) {
		this.maritalStatus = maritalStatus;
	}
	public Date getDeathDate() {
		return deathDate;
	}
	public void setDeathDate(Date deathDate) {
		this.deathDate = deathDate;
	}
	public Date getDeathNotificationDate() {
		return deathNotificationDate;
	}
	public void setDeathNotificationDate(Date deathNotificationDate) {
		this.deathNotificationDate = deathNotificationDate;
	}
	@Override
	public String toString() {
		return "IndividualPartyDetails {partyId=" + partyId + ", partyType=" + partyType + ", title=" + title
				+ ", foreNames=" + foreNames + ", intials=" + intials + ", lastName=" + lastName + ", generationSuffix="
				+ generationSuffix + ", suffixTitle=" + suffixTitle + ", salutation=" + salutation + ", gender="
				+ gender + ", birthDate=" + birthDate + ", maritalStatus=" + maritalStatus + ", deathDate=" + deathDate
				+ ", deathNotificationDate=" + deathNotificationDate + "}";
	}
	
	

	
	}
