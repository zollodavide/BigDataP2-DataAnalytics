package service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import model.MedianHouseholdIncome;
import model.PercentOver25HighSchool;
import model.PercentagePeoplePoverty;
import model.PoliceKilling;
import model.ShareRaceCity;
import model.StatePopulation;
import utility.Parser;

public class DatasetService {
	
	private JavaRDD<MedianHouseholdIncome> medianIncome;
	private JavaRDD<PercentagePeoplePoverty> percentagePoverty;
	private JavaRDD<PercentOver25HighSchool> percentHighSchool;
	private JavaRDD<PoliceKilling> policeKilling;
	private JavaRDD<ShareRaceCity> shareRace;
	private JavaRDD<StatePopulation> statePopulation;
	private JavaSparkContext sparkContext;
	
	public DatasetService(String fileMI, String filePP, String filePHS, String filePK, String fileSR, String fileSP) {
		SparkConf conf = new SparkConf().setAppName("DataAnalytics");
		this.sparkContext = new JavaSparkContext(conf);
		
		this.medianIncome = getMedianIncomeRecords(sparkContext, fileMI);
		this.percentagePoverty = getPercentagePovertyRecords(sparkContext, filePP);
		this.percentHighSchool = getPercentHighSchoolRecords(sparkContext, filePHS);
		this.policeKilling = getPoliceKillingRecords(sparkContext, filePK);
		this.shareRace = getShareRaceRecords(sparkContext, fileSR);
		this.statePopulation = getStatePopulationRecords(sparkContext, fileSP);
	}

	private JavaRDD<ShareRaceCity> getShareRaceRecords(JavaSparkContext sc, String fileSR) {
		JavaRDD<ShareRaceCity> raw5= sc.textFile(fileSR).map(line -> Parser.parseShareRaceTable(line))
				.filter(stock -> stock!=null);
		return raw5;
	}
	
	private JavaRDD<PoliceKilling> getPoliceKillingRecords(JavaSparkContext sc, String filePK) {
		JavaRDD<PoliceKilling> raw4 = sc.textFile(filePK).map(line -> Parser.parsePoliceKillingTable(line))
				.filter(record -> record!=null);
		return raw4;
	}
	
	private JavaRDD<PercentOver25HighSchool> getPercentHighSchoolRecords(JavaSparkContext sc, String filePHS) {
		JavaRDD<PercentOver25HighSchool> raw3= sc.textFile(filePHS).map(line -> Parser.parsePercentCompletedHSTable(line))
				.filter(record -> record!=null);
		return raw3;
	}
	
	private JavaRDD<PercentagePeoplePoverty> getPercentagePovertyRecords(JavaSparkContext sc, String filePP) {
		JavaRDD<PercentagePeoplePoverty> raw2= sc.textFile(filePP).map(line -> Parser.parsePercentagePovertyTable(line))
				.filter(record -> record!=null);
		return raw2;
	}
	
	private JavaRDD<MedianHouseholdIncome> getMedianIncomeRecords(JavaSparkContext sc, String fileMI) {
		JavaRDD<MedianHouseholdIncome> raw1= sc.textFile(fileMI).map(line -> Parser.parseHouseholdIncomeTable(line))
				.filter(record-> record!=null);
		return raw1;
	}
	
	private JavaRDD<StatePopulation> getStatePopulationRecords(JavaSparkContext sc, String fileSP) {
		JavaRDD<StatePopulation> raw5= sc.textFile(fileSP).map(line -> Parser.parseStatePopulationTable(line))
				.filter(record -> record!=null)
				.filter(record -> record.getYear() == 2016)
				.filter(record -> record.getAge().equals("Total"))
				.filter(record -> record.getGenre().equals("Total"));

		return raw5;
	}
	
	public void closeSparkContext() {
		this.sparkContext.close();
	}

	public JavaRDD<MedianHouseholdIncome> getMedianIncome() {
		return medianIncome;
	}

	public JavaRDD<PercentagePeoplePoverty> getPercentagePoverty() {
		return percentagePoverty;
	}

	public JavaRDD<PercentOver25HighSchool> getPercentHighSchool() {
		return percentHighSchool;
	}

	public JavaRDD<PoliceKilling> getPoliceKilling() {
		return policeKilling;
	}

	public JavaRDD<ShareRaceCity> getShareRace() {
		return shareRace;
	}

	public JavaRDD<StatePopulation> getStatePopulation() {
		return statePopulation;
	}
	
	
	
	
}
