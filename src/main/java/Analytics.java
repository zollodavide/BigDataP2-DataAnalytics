import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import model.*;
import scala.Tuple2;
import service.DatasetService;

public class Analytics {

	private DatasetService datasetService; //SINGLETON -> NON INSTANZIARE DUE VOLTE
	
	public Analytics(String fileMI, String filePP, String filePHS, String filePK, String fileSR) {
		this.datasetService = new DatasetService(fileMI, filePP, filePHS, filePK, fileSR);
	}
	
	public void run() {
		
		JavaRDD<Tuple2<String, Double>> sortedPoorestStates = 
				calculatePoorestStates(this.datasetService.getPercentagePoverty());
		JavaRDD<Tuple2<String, Integer>> sortedCommonVictimNames = 
				calculateMostCommonVictimNames(this.datasetService.getPoliceKilling());
		JavaRDD<Tuple2<Character,Integer>> sortedRaceVictims = calculateKilledPeopleByRace(this.datasetService.getPoliceKilling());
		
		printVictimsByRace(sortedRaceVictims);
//		printMostCommonNames(sortedCommonVictimNames);
//		printPoorestStates(sortedPoorestStates);
		
		this.datasetService.closeSparkContext(); //CHIUSURA SPARK CONTEXT - DEV'ESSERE L'ULTIMA RIGA ESEGUITA
		
	}

	private JavaRDD<Tuple2<String, Double>> calculatePoorestStates(JavaRDD<PercentagePeoplePoverty> rdd) {
		
		JavaPairRDD<String, Double> state2cityPoverty= rdd
				.mapToPair(pov -> new Tuple2<>(pov.getState(), pov.getPovertyRate()))
				.reduceByKey((s1,s2) -> s1+s2);
		
		JavaPairRDD<String, Integer> state2cityNum= rdd
				.mapToPair(pov -> new Tuple2<>(pov.getState(), 1))
				.reduceByKey((s1,s2) -> s1+s2);
		
		JavaPairRDD<String, Double> state2meanPov = state2cityPoverty
				.join(state2cityNum)
				.mapToPair(tup -> new Tuple2<>(tup._1(), tup._2()._1()/tup._2()._2()));
		
		JavaRDD<Tuple2<String, Double>> sorted = state2meanPov
				.map(tup -> new Tuple2<>(tup._1(), tup._2()))
				.sortBy(tup ->tup._2(), false, 1);
		
		return sorted;
	}

	
	private JavaRDD<Tuple2<String, Integer>> calculateMostCommonVictimNames(JavaRDD<PoliceKilling> rdd) {
		
		JavaPairRDD<String, Integer> name2count = rdd
				.mapToPair(pk -> new Tuple2<>(pk.getName().split(" ")[0], 1))
				.reduceByKey((s1,s2) -> s1+s2);
		
		JavaRDD<Tuple2<String, Integer>> sorted = name2count
				.map(tup -> tup)
				.sortBy(tup -> tup._2(), false, 1);
		
		return sorted;
	}
	
	
	private JavaRDD<Tuple2<Character,Integer>> calculateKilledPeopleByRace(JavaRDD<PoliceKilling> rdd) {
		
		JavaPairRDD<Character, Integer> race2count = rdd
				.mapToPair(pk -> new Tuple2<>(pk.getRace(), 1))
				.reduceByKey((s1,s2) -> s1+s2);
		
		JavaRDD<Tuple2<Character, Integer>> sorted = race2count
				.map(tup -> tup)
				.sortBy(tup -> tup._2(), false, 1);
		
		return sorted;
	}
	
	private void printVictimsByRace(JavaRDD<Tuple2<Character, Integer>> sorted) {
		List<Tuple2<Character, Integer>> out = sorted.collect();
		for(Tuple2<Character, Integer> p : out)
			System.out.println(p._1() + ": " + p._2());
	}
	
	private void printMostCommonNames(JavaRDD<Tuple2<String, Integer>> sorted) {
		List<Tuple2<String, Integer>> out = sorted.collect();
		for(Tuple2<String, Integer> p : out)
			System.out.println(p._1() + ": " + p._2());
	}
	
	private void printPoorestStates(JavaRDD<Tuple2<String, Double>> sorted) {
		List<Tuple2<String, Double>> out = sorted.collect();
		for(Tuple2<String, Double> p : out)
			System.out.println(p._1() + ": " + p._2());
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
	
}
