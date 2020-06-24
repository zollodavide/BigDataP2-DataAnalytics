package utility;

import java.text.SimpleDateFormat;
import java.util.Date;

import model.*;

public class Parser {
	
	public static MedianHouseholdIncome parseHouseholdIncomeTable(String line) {
		String[] parts = line.split(",");
		MedianHouseholdIncome out = new MedianHouseholdIncome();
		try {
			out.setState(parts[0]);
			out.setCity(parts[1]);
			out.setMedianIncome(Double.parseDouble(parts[2]));
		} catch (Exception e) {
			return null;
		}
		return out;
	}
	
	public static PercentagePeoplePoverty parsePercentagePovertyTable(String line) {
		String[] parts = line.split(",");
		PercentagePeoplePoverty out = new PercentagePeoplePoverty();
		try {
			out.setState(parts[0]);
			out.setCity(parts[1]);
			out.setPovertyRate(Double.parseDouble(parts[2]));
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}
		return out;
	}
	
	public static ShareRaceCity parseShareRaceTable(String line) {
		String[] parts = line.split(",");
		ShareRaceCity out = new ShareRaceCity();
		try {
			out.setState(parts[0]);
			out.setCity(parts[1]);
			out.setShareWhite(Double.parseDouble(parts[2]));
			out.setShareBlack(Double.parseDouble(parts[3]));
			out.setShareNativeAmerican(Double.parseDouble(parts[4]));
			out.setShareAsian(Double.parseDouble(parts[5]));
			out.setShareHispanic(Double.parseDouble(parts[6]));
		} catch (Exception e) {
			return null;
		}
		return out;
	}
	
	public static PercentOver25HighSchool parsePercentCompletedHSTable(String line) {
		String[] parts = line.split(",");
		PercentOver25HighSchool out = new PercentOver25HighSchool();
		try {
			out.setState(parts[0]);
			out.setCity(parts[1]);
			out.setPercentCompletedHS(Double.parseDouble(parts[2]));
		} catch (Exception e) {
			return null;
		}
		return out;
	}
	
	public static PoliceKilling parsePoliceKillingTable(String line) {
		String[] parts = line.split(",");
		PoliceKilling out = new PoliceKilling();
		SimpleDateFormat format = new SimpleDateFormat("dd/MM/yy");
		Date date;
		try {
			date = format.parse(parts[2]);
			out.setName(parts[1]);
			out.setDate(date);
			out.setMannerOfDeath(parts[3]);
			out.setArmed(parts[4]);
			out.setAge(Integer.parseInt(parts[5]));
			out.setGender(parts[6].charAt(0));
			out.setRace(parts[7].charAt(0));
			out.setCity(parts[8]);
			out.setState(parts[9]);
			out.setSignsOfMentalIllness(Boolean.parseBoolean(parts[10]));
			out.setThreatLevel(parts[11]);
			out.setFlee(parts[12]);
			out.setBodyCamera(Boolean.parseBoolean(parts[13]));
		} catch (Exception e) {
			return null;
		}
		return out;
	}
	
	

}
