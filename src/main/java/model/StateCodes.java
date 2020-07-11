package model;

public class StateCodes {
	private String name;
	private String abbreviation;
	private String code;
	
	public StateCodes() {}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAbbreviation() {
		return abbreviation;
	}

	public void setAbbreviation(String abbreviation) {
		this.abbreviation = abbreviation;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	@Override
	public String toString() {
		return "StateCodes [name=" + name + ", abbreviation=" + abbreviation + ", code=" + code + "]";
	}
	
	
}
