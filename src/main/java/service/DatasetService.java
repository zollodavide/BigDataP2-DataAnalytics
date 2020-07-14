package service;

import java.io.File;
import java.util.List;

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
	private JavaRDD<LabeledPoint> pkLabeledPoints;
	
	public DatasetService(String fileMI, String filePP, String filePHS, String filePK, String fileSR, String fileSP, String fileSC) {
		SparkConf conf = new SparkConf().setAppName("DataAnalytics");
		
		this.sparkContext = new JavaSparkContext(conf);
		
		this.medianIncome = getMedianIncomeRecords(sparkContext, fileMI);
		this.percentagePoverty = getPercentagePovertyRecords(sparkContext, filePP);
		this.percentHighSchool = getPercentHighSchoolRecords(sparkContext, filePHS);
		this.policeKilling = getPoliceKillingRecords(sparkContext, filePK);
		this.shareRace = getShareRaceRecords(sparkContext, fileSR);
		this.statePopulation = getStatePopulationRecords(sparkContext, fileSP);
		this.stateCodes = getStateCodesRecords(sparkContext, fileSC);
//		this.pkLabeledPoints = getLabeledPoints(sparkContext, filePK);
		getLabeledPoints(sparkContext, filePK);
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
	
	
	private void getLabeledPoints(JavaSparkContext sc, String filePK){
		
		JavaPairRDD<String, ShareRaceCity> shared = getShareRace().mapToPair(f-> new Tuple2<>(f.getCity() + " " + f.getState(), f));
		
		JavaPairRDD<String,PoliceKilling> rdd = getPoliceKilling()
				.filter(f -> f.getRace()!='?' && f.getRace()!='O')
				.filter(f -> f.getAge() >0)
				.mapToPair(f -> new Tuple2<>(f.getCity() + " " + f.getState(), f ));
		
		
		List<String> tb = rdd
			.join(shared)
			.map(f -> f._2()._1().getRace() + " " + 
				 " 1:" + f._2()._1().getAge() +
				 " 2:" + f._2()._2().getShareAsian() +
				 " 3:" + f._2()._2().getShareBlack() +
				 " 4:" + f._2()._2().getShareHispanic() + 
				 " 5:" + f._2()._2().getShareNativeAmerican() + 
				 " 6:" + f._2()._2().getShareWhite() )
			.collect();
//		
//		for(String s : tb)
//			System.out.println(s);

		SparkSession ss = new SparkSession(JavaSparkContext.toSparkContext(sc));
		
		Dataset<Row> data = ss.read().format("libsvm").load("input/MLdata.txt");
		System.out.println(data.count());
		
		 // generate the train/test split.
	    Dataset<Row>[] tmp = data.randomSplit(new double[]{0.8, 0.2});
	    Dataset<Row> train = tmp[0];
	    Dataset<Row> test = tmp[1];
	    
	 // configure the base classifier.
	    LogisticRegression classifier = new LogisticRegression()
	      .setMaxIter(10)
	      .setTol(1E-6)
	      .setFitIntercept(true);

	    // instantiate the One Vs Rest Classifier.
	    OneVsRest ovr = new OneVsRest().setClassifier(classifier);

	    // train the multiclass model.
	    OneVsRestModel ovrModel = ovr.fit(train);

	    // score the model on test data.
	    Dataset<Row> predictions = ovrModel.transform(test)
	      .select("prediction", "label");

	    // obtain evaluator.
	    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
	            .setMetricName("accuracy");

	    // compute the classification error on test data.
	    double accuracy = evaluator.evaluate(predictions);
	    System.out.println("Test Error = " + accuracy);
//	    evaluator.
	    
	    System.out.println("Test Error = " + (1 - accuracy));
	
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
