package model;

public class MedianHouseholdIncome {

	private String state;
	private String city;
	private Double medianIncome;
	
	public MedianHouseholdIncome(){}
	
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
	public Double getMedianIncome() {
		return medianIncome;
	}
	public void setMedianIncome(Double medianIncome) {
		this.medianIncome = medianIncome;
	}
	
	
}
