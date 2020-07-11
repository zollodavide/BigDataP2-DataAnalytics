package utility;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import model.MedianHouseholdIncome;
import model.PercentOver25HighSchool;
import model.PercentagePeoplePoverty;
import model.PoliceKilling;
import model.ShareRaceCity;
import scala.Tuple2;

public class Printer {
	
	StringBuilder sb;
	
	public Printer() {
		sb = new StringBuilder();
	}
	
	public void printAllResults() {
		System.out.println(sb);
	}
	
	public void printVictimsByRace(JavaRDD<Tuple2<Character, Integer>> sorted, boolean print) {
		StringBuilder string = new StringBuilder();

		string.append("******** VICTIMS BY RACE ********\n");
		List<Tuple2<Character, Integer>> out = sorted.collect();
		for(Tuple2<Character, Integer> p : out)
			string.append(p._1() + ": " + p._2()+"\n");

		if (print)
			System.out.println(string);
		sb.append(string.toString()+"\n");
	}
	
	public void printVictime4state(JavaRDD<Tuple2<String, Integer>> sorted, boolean print) {
		StringBuilder string = new StringBuilder();
		
		string.append("******** KILLED PEOPLE FOR STATE ********\n");
		List<Tuple2<String,Integer>> out = sorted.collect();
		for(Tuple2<String, Integer>p :out)
			string.append(p._1() + ":" + p._2() +"\n");
		if(print)
			System.out.println(string);
		sb.append(string.toString()+"\n");
				
	}
	public void printMostCommonNames(JavaRDD<Tuple2<String, Integer>> sorted, boolean print) {
		StringBuilder string = new StringBuilder();

		string.append("******** VICTIMS MOST COMMON NAMES ********\n");
		List<Tuple2<String, Integer>> out = sorted.collect();
		for(Tuple2<String, Integer> p : out)
			string.append(p._1() + ": " + p._2()+"\n");

		if (print)
			System.out.println(string);
		sb.append(string.toString()+"\n");
	}
	
	public void printPoorestStates(JavaRDD<Tuple2<String, Double>> sorted, boolean print) {
		StringBuilder string = new StringBuilder();

		string.append("******** MEAN STATE POVERTY PERCENTAGE ********\n");
		List<Tuple2<String, Double>> out = sorted.collect();
		for(Tuple2<String, Double> p : out)
			string.append(p._1() + ": " + p._2()+"\n");

		if (print)
			System.out.println(string);
		sb.append(string.toString()+"\n");
	}
	
	public void printEducationVSPoverty(JavaPairRDD<String, Tuple2<Double, Double>> rdd, boolean print) {
		StringBuilder string = new StringBuilder();

		string.append("******** MEAN STATE EDUCATION VS POVERTY PERCENTAGE ********\n");
		Map<String,Tuple2<Double, Double>> out = rdd.collectAsMap();
		for(String p : out.keySet())
			string.append(p + ": Education = " + out.get(p)._1() + ", Poverty = "+ out.get(p)._2() + "\n");

		if (print)
			System.out.println(string);
		sb.append(string.toString()+"\n");
	}
	
	
	public void printMostCommonMannerOfDeath(JavaRDD<Tuple2<String, Integer>> sorted, boolean print) {
		StringBuilder string = new StringBuilder();
		
		string.append("******** MOST COMMON MANNER OF DEATH ********\n");
		List<Tuple2<String, Integer>> out = sorted.collect();
		for(Tuple2<String, Integer>  p : out)
			string.append(p._1() + ":" + p._2()+"\n");
		
		if (print)
			System.out.println(string);
		sb.append(string.toString()+"\n");
	}
	
	public void printDangerousCities (JavaRDD<Tuple2<String, Integer>> sorted, boolean print) {
		StringBuilder string = new StringBuilder();
		
		string.append("******** MOST DANGEROUS CITIES********\n");
		List<Tuple2<String, Integer>> out = sorted.collect();
		for(Tuple2<String, Integer> p : out)
			string.append(p._1() + ":" + p._2()+"\n");
		
		if(print)
			System.out.println(string);
		sb.append(string.toString()+"\n");
	}
	

	public void printCountBodyCamera (JavaRDD<Tuple2<Boolean, Integer>> sorted, boolean print) {
		StringBuilder string = new StringBuilder();
		
		string.append("******** COUNT IF IS PRESENT BODY CAMERA********\n");
		List<Tuple2<Boolean, Integer>> out = sorted.collect();
		for(Tuple2<Boolean, Integer> p : out)
			string.append(p._1() + ":" + p._2()+"\n");
		
		if(print)
			System.out.println(string);
		sb.append(string.toString()+"\n");
	}
	
	public void printCommonWeapon (JavaRDD<Tuple2<String, Integer>> sorted, boolean print) {
		StringBuilder string = new StringBuilder();
		
		string.append("******** COMMON WEAPON USE********\n");
		List<Tuple2<String, Integer>> out = sorted.collect();
		for(Tuple2<String, Integer> p : out)
			string.append(p._1() + ":" + p._2()+"\n");
		
		if(print)
			System.out.println(string);
		sb.append(string.toString()+"\n");
	}
	
	public void printVictimGender(JavaRDD<Tuple2<Character, Integer>> sorted, boolean print) {
		StringBuilder string = new StringBuilder();

		string.append("******** GENDER OF VICTIM ********\n");
		List<Tuple2<Character, Integer>> out = sorted.collect();
		for(Tuple2<Character, Integer> p : out)
			string.append(p._1() + ": " + p._2()+"\n");

		if (print)
			System.out.println(string);
		sb.append(string.toString()+"\n");
	}
	
	public void printMeanAge4Race(JavaRDD<Tuple2<Character, Double>> sorted, boolean print) {
		StringBuilder string = new StringBuilder();

		string.append("******** MEAN AGE KILLED FOR RACE ********\n");
		List<Tuple2<Character,Double>> out = sorted.collect();
		for(Tuple2<Character, Double> p : out)
			string.append(p._1() + ":" + p._2() + "\n");

		if (print)
			System.out.println(string);
		sb.append(string.toString()+"\n");
	}
	
//	public void printMentaIllnessVSThreatLevel(JavaRDD<Tuple2<Boolean, String>> sorted, boolean print) {
//		StringBuilder string = new StringBuilder();
//
//		string.append("******** MENTAL ILLNESS VS THREAT LEVEL ********\n");
//		List<Tuple2<Boolean,String>> out = sorted.collect();
//		for(Tuple2<Boolean, String> p : out)
//			string.append(p._1() + ":" + p._2() + "\n");
//
//		if (print)
//			System.out.println(string);
//		sb.append(string.toString()+"\n");
//	}
	
	
	//Momentaneo da sistemare
	
//	public void printMeanAge4race (JavaRDD<Tuple2<Character, Integer>> calculateMeanAgeWithRace, boolean print) {
//		StringBuilder string = new StringBuilder();
//		
//		string.append("******** COMMON WEAPON USE********\n");
//		List<Tuple2<Character, Integer>> out = calculateMeanAgeWithRace.collect();
//		for(Tuple2<Character, Integer> p : out)
//			string.append(p._1() + ":" + p._2()+"\n");
//		
//		if(print)
//			System.out.println(string);
//		sb.append(string.toString()+"\n");
//	}
	
	
	
//	public void printNumberOfParsedRecords(JavaRDD<MedianHouseholdIncome> raw1, JavaRDD<PercentagePeoplePoverty> raw2,
//			JavaRDD<PercentOver25HighSchool> raw3, JavaRDD<PoliceKilling> raw4, JavaRDD<ShareRaceCity> raw5) {
//
//		System.out.println("Parsed Records by File");
//		System.out.println("MDI: " + raw1.collect().size());
//		System.out.println("PPP: " + raw2.collect().size());
//		System.out.println("PO25HS: " + raw3.collect().size());
//		System.out.println("PK: " + raw4.collect().size());
//		System.out.println("SRC: " + raw5.collect().size());
//	}
}
