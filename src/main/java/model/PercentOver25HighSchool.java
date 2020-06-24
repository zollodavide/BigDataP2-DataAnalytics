package model;

import java.io.Serializable;

public class PercentOver25HighSchool implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String state;
	private String city;
	private Double percentCompletedHS;
	
	public PercentOver25HighSchool() {}

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

	public Double getPercentCompletedHS() {
		return percentCompletedHS;
	}

	public void setPercentCompletedHS(Double percentCompletedHS) {
		this.percentCompletedHS = percentCompletedHS;
	}
	
	

}
