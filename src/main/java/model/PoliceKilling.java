package model;

import java.util.Date;

public class PoliceKilling {

	private String name;
	private Date date;
	private String mannerOfDeath;
	private String armed;
	private Integer age;
	private String gender;
	private String race;
	private String city;
	private String state;
	private Boolean signsOfMentalIllness;
	private String threatLevel;
	private String flee;
	private Boolean bodyCamera;
	
	public PoliceKilling() {}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getMannerOfDeath() {
		return mannerOfDeath;
	}

	public void setMannerOfDeath(String mannerOfDeath) {
		this.mannerOfDeath = mannerOfDeath;
	}

	public String getArmed() {
		return armed;
	}

	public void setArmed(String armed) {
		this.armed = armed;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getRace() {
		return race;
	}

	public void setRace(String race) {
		this.race = race;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public Boolean getSignsOfMentalIllness() {
		return signsOfMentalIllness;
	}

	public void setSignsOfMentalIllness(Boolean signsOfMentalIllness) {
		this.signsOfMentalIllness = signsOfMentalIllness;
	}

	public String getThreatLevel() {
		return threatLevel;
	}

	public void setThreatLevel(String threatLevel) {
		this.threatLevel = threatLevel;
	}

	public String getFlee() {
		return flee;
	}

	public void setFlee(String flee) {
		this.flee = flee;
	}

	public Boolean getBodyCamera() {
		return bodyCamera;
	}

	public void setBodyCamera(Boolean bodyCamera) {
		this.bodyCamera = bodyCamera;
	}

	
}
