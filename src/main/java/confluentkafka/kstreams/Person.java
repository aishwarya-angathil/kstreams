package confluentkafka.kstreams;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
@JsonRootName("person")
public class Person implements Serializable {
	private String name;
	private String personalId;
	private String country;
	private String occupatuion;
	
	public Person() {		
	}
@JsonCreator
	public Person(@JsonProperty("name")String name, @JsonProperty("personalId")String personalId,@JsonProperty("country") String country, 
			@JsonProperty("occupatuion") String occupatuion) {	
		this.name = name;
		this.personalId = personalId;
		this.country = country;
		this.occupatuion = occupatuion;
	}
public String getName() {
	return name;
}
public void setName(String name) {
	this.name = name;
}
public String getPersonalId() {
	return personalId;
}
public void setPersonalId(String personalId) {
	this.personalId = personalId;
}
public String getCountry() {
	return country;
}
public void setCountry(String country) {
	this.country = country;
}
public String getOccupatuion() {
	return occupatuion;
}
public void setOccupatuion(String occupatuion) {
	this.occupatuion = occupatuion;
}
 
	

}
