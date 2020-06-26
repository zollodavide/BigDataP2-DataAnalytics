package model;

public class StatePopulation {

	private Integer year;
	private String state;
	private Integer population;
	private String age;
	private String genre;
	
	public StatePopulation() {}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public Integer getPopulation() {
		return population;
	}

	public void setPopulation(Integer population) {
		this.population = population;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	@Override
	public String toString() {
		return "StatePopulation [year: " + year + ", state: " + state + ", population: " + population + ", age: " + age
				+ ", genre: " + genre + "]";
	}
	
	
}
