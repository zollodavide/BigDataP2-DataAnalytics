import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import model.*;
import utility.Parser;

public class Prova {
	String file1;
	String file2;
	String file3;
	String file4;
	String file5;

	public Prova(String file1, String file2, String file3, String file4, String file5) {
		this.file1 = file1;
		this.file2 = file2;
		this.file3 = file3;
		this.file4 = file4;
		this.file5 = file5;
	}
	
	public void run() {
		
		SparkConf conf = new SparkConf().setAppName("DataAnalytics");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<MedianHouseholdIncome> raw1 = getMedianIncomeRecords(sc);
		JavaRDD<PercentagePeoplePoverty> raw2 = getPercentagePovertyRecords(sc);
		JavaRDD<PercentOver25HighSchool> raw3 = getPercentHighSchoolRecords(sc);
		JavaRDD<PoliceKilling> raw4 = getPoliceKillingRecords(sc);
		JavaRDD<ShareRaceCity> raw5 = getShareRaceRecords(sc);
	
		printNumberOfParsedRecords(raw1, raw2, raw3, raw4, raw5);	
		sc.close();
	}


	private void printNumberOfParsedRecords(JavaRDD<MedianHouseholdIncome> raw1, JavaRDD<PercentagePeoplePoverty> raw2,
			JavaRDD<PercentOver25HighSchool> raw3, JavaRDD<PoliceKilling> raw4, JavaRDD<ShareRaceCity> raw5) {

		System.out.println("Parsed Records by File");
		System.out.println("MDI: " + raw1.collect().size());
		System.out.println("PPP: " + raw2.collect().size());
		System.out.println("PO25HS: " + raw3.collect().size());
		System.out.println("PK: " + raw4.collect().size());
		System.out.println("SRC: " + raw5.collect().size());
	}
	
	private JavaRDD<ShareRaceCity> getShareRaceRecords(JavaSparkContext sc) {
		JavaRDD<ShareRaceCity> raw5= sc.textFile(file5).map(line -> Parser.parseShareRaceTable(line))
				.filter(stock -> stock!=null);
		return raw5;
	}
	
	private JavaRDD<PoliceKilling> getPoliceKillingRecords(JavaSparkContext sc) {
		JavaRDD<PoliceKilling> raw4 = sc.textFile(file4).map(line -> Parser.parsePoliceKillingTable(line))
				.filter(stock -> stock!=null);
		return raw4;
	}
	
	private JavaRDD<PercentOver25HighSchool> getPercentHighSchoolRecords(JavaSparkContext sc) {
		JavaRDD<PercentOver25HighSchool> raw3= sc.textFile(file3).map(line -> Parser.parsePercentCompletedHSTable(line))
				.filter(stock -> stock!=null);
		return raw3;
	}
	
	private JavaRDD<PercentagePeoplePoverty> getPercentagePovertyRecords(JavaSparkContext sc) {
		JavaRDD<PercentagePeoplePoverty> raw2= sc.textFile(file2).map(line -> Parser.parsePercentagePovertyTable(line))
				.filter(stock -> stock!=null);
		return raw2;
	}
	
	private JavaRDD<MedianHouseholdIncome> getMedianIncomeRecords(JavaSparkContext sc) {
		JavaRDD<MedianHouseholdIncome> raw1= sc.textFile(file1).map(line -> Parser.parseHouseholdIncomeTable(line))
				.filter(stock -> stock!=null);
		return raw1;
	}
}
