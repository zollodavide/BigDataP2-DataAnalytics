package service;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import model.MedianHouseholdIncome;
import model.PercentOver25HighSchool;
import model.PercentagePeoplePoverty;
import model.PoliceKilling;
import model.ShareRaceCity;
import model.StateCodes;
import model.StatePopulation;
import scala.Tuple2;
import utility.Parser;

public class DatasetService {
	
	private JavaRDD<MedianHouseholdIncome> medianIncome;
	private JavaRDD<PercentagePeoplePoverty> percentagePoverty;
	private JavaRDD<PercentOver25HighSchool> percentHighSchool;
	private JavaRDD<PoliceKilling> policeKilling;
	private JavaRDD<ShareRaceCity> shareRace;
	private JavaRDD<StatePopulation> statePopulation;
	private JavaRDD<StateCodes> stateCodes;
	private JavaSparkContext sparkContext;
	
	public DatasetService(String fileMI, String filePP, String filePHS, String filePK, String fileSR, String fileSP, String fileSC, String fileML) {
		SparkConf conf = new SparkConf().setAppName("DataAnalytics");
		
		this.sparkContext = new JavaSparkContext(conf);
		this.medianIncome = getMedianIncomeRecords(sparkContext, fileMI);
		this.percentagePoverty = getPercentagePovertyRecords(sparkContext, filePP);
		this.percentHighSchool = getPercentHighSchoolRecords(sparkContext, filePHS);
		this.policeKilling = getPoliceKillingRecords(sparkContext, filePK);
		this.shareRace = getShareRaceRecords(sparkContext, fileSR);
		this.statePopulation = getStatePopulationRecords(sparkContext, fileSP);
		this.stateCodes = getStateCodesRecords(sparkContext, fileSC);
		
		new ml.ClassificationModel(this.sparkContext, this, fileML);

	}
	

	private JavaRDD<ShareRaceCity> getShareRaceRecords(JavaSparkContext sc, String fileSR) {
		JavaRDD<ShareRaceCity> raw5= sc.textFile(fileSR).map(line -> Parser.parseShareRaceTable(line))
				.filter(stock -> stock!=null);
		return raw5;
	}
	//Qui prendo e metto nell'RDD i record del file PoliceKilling
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
	
	private JavaRDD<StateCodes> getStateCodesRecords(JavaSparkContext sc, String fileSC) {
		JavaRDD<StateCodes> raw1= sc.textFile(fileSC).map(line -> Parser.parseStateCodesTable(line))
				.filter(record-> record!=null);
		return raw1;
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


	public JavaRDD<StateCodes> getStateCodes() {
		return stateCodes;
	}

//
//	public JavaRDD<LabeledPoint> getPkLabeledPoints() {
//		return pkLabeledPoints;
//	}
	
	
	
	
}
