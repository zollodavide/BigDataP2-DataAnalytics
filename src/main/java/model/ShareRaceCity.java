package model;

import java.io.Serializable;

public class ShareRaceCity implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String state;
	private String city;
	private Double shareWhite;
	private Double shareNativeAmerican;
	private Double shareBlack;
	private Double shareAsian;
	private Double shareHispanic;

	public ShareRaceCity() {}

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

	public Double getShareWhite() {
		return shareWhite;
	}

	public void setShareWhite(Double shareWhite) {
		this.shareWhite = shareWhite;
	}

	public Double getShareNativeAmerican() {
		return shareNativeAmerican;
	}

	public void setShareNativeAmerican(Double shareNativeAmerican) {
		this.shareNativeAmerican = shareNativeAmerican;
	}

	public Double getShareBlack() {
		return shareBlack;
	}

	public void setShareBlack(Double shareBlack) {
		this.shareBlack = shareBlack;
	}

	public Double getShareAsian() {
		return shareAsian;
	}

	public void setShareAsian(Double shareAsian) {
		this.shareAsian = shareAsian;
	}

	public Double getShareHispanic() {
		return shareHispanic;
	}

	public void setShareHispanic(Double shareHispanic) {
		this.shareHispanic = shareHispanic;
	}
	
	

}
