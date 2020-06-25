package model;

import java.io.Serializable;

public class PercentagePeoplePoverty implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String state;
	private String city;
	private Double povertyRate;

	public PercentagePeoplePoverty() {}
	
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public Double getPovertyRate() {
		return povertyRate;
	}
	public void setPovertyRate(Double povertyRate) {
		this.povertyRate = povertyRate;
	}
	
	@Override
	public String toString() {
		return "PercentagePeoplePoverty [state: " + state + ", city: " + city + ", povertyRate: " + povertyRate + "]";
	}
	
}
