import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import model.*;
import scala.Tuple2;
import service.DatasetService;

public class Analytics {

	private DatasetService datasetService;
	
	public Analytics(String fileMI, String filePP, String filePHS, String filePK, String fileSR) {
		this.datasetService = new DatasetService(fileMI, filePP, filePHS, filePK, fileSR);
	}
	
	public void run() {
		
		JavaRDD<Tuple2<String, Double>> sortedPoorestStates = 
				calculatePoorestStates(this.datasetService.getPercentagePoverty());
		printPoorestStates(sortedPoorestStates);
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

	private void printPoorestStates(JavaRDD<Tuple2<String, Double>> sorted) {
		List<Tuple2<String, Double>> out = sorted.collect();
		for(Tuple2<String, Double> p : out)
			System.out.println(p._1() + ": " + p._2());
	}

	
	@SuppressWarnings("unused")
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
