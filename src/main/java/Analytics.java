import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import model.*;
import scala.Tuple2;
import service.DatasetService;
import utility.Printer;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;

public class Analytics {

	private DatasetService datasetService; //SINGLETON -> NON INSTANZIARE DUE VOLTE
	private Printer printer;
	
	
	public Analytics(String fileMI, String filePP, String filePHS, String filePK, String fileSR, String fileSP) {
		this.datasetService = new DatasetService(fileMI, filePP, filePHS, filePK, fileSR, fileSP);
		this.printer = new Printer();
	}
	

	
	public void run() {
		
		
		JavaRDD<Tuple2<String, Double>> sortedPoorestStates = 
				calculatePoorestStates(this.datasetService.getPercentagePoverty());
		
		JavaRDD<Tuple2<String, Integer>> sortedCommonVictimNames = 
				calculateMostCommonVictimNames(this.datasetService.getPoliceKilling());
		JavaRDD<Tuple2<String, Integer>> sortedCommonMannerOfDeath = 
				calculateMostCommonMannerOfDeath(this.datasetService.getPoliceKilling());
		JavaRDD<Tuple2<String,Integer>> sortedDangerousCities = 
				dangerousCities(this.datasetService.getPoliceKilling());
		JavaRDD<Tuple2<Boolean,Integer>> sortedBodyCamera =
				bodyCameraCheck(this.datasetService.getPoliceKilling());
		JavaRDD<Tuple2<Character, Integer>> sortedVictimGender =
				genderOfVictim(this.datasetService.getPoliceKilling());
		JavaRDD<Tuple2<String,Integer>> sortedCommonWeapon =
				commonWeaponUse(this.datasetService.getPoliceKilling());
		JavaRDD<Tuple2<Character,Integer>> sortedRaceVictims = 
				calculateKilledPeopleByRace(this.datasetService.getPoliceKilling());
		JavaPairRDD<String, Tuple2<Double, Double>> educationVSpoverty = 
				calculateEducationVsPoverty(this.datasetService.getPercentHighSchool(), this.datasetService.getPercentagePoverty());
		
		JavaRDD<Tuple2<Character, Double>> sortedAgeRace =
				calculateMeanAgeWithRace(this.datasetService.getPoliceKilling());
	 
		JavaRDD<Tuple2<String, Integer>> sortedVictimByState= 
				calculateKilledPeopleByState(this.datasetService.getPoliceKilling());
		
		JavaRDD<Tuple2<String, Double>> sortedState= 
				calculateVictimsToPopulationProportion(this.datasetService.getPoliceKilling(), this.datasetService.getStatePopulation());
		
		
		List<Tuple2<String,Integer>>sd = sortedVictimByState.collect();
		List<Tuple2<String,Double>>sdd = sortedState.collect();
		
		for(Tuple2<String,Double> a: sdd)
			System.out.println(a._1() + ": " + a._2());
//		printer.printVictimsByRace(sortedRaceVictims,false);
//		printer.printMostCommonNames(sortedCommonVictimNames,false);
//		printer.printPoorestStates(sortedPoorestStates,false);
//		printer.printEducationVSPoverty(educationVSpoverty, true);
//		printer.printAllResults();
//		printer.printMostCommonMannerOfDeath(sortedCommonMannerOfDeath, false);
//		printer.printDangerousCities(sortedDangerousCities, false);
//		printer.printCountBodyCamera(sortedBodyCamera, false);
//		printer.printCommonWeapon(sortedCommonWeapon, false);
//		printer.printVictime4state(sortedVictimByState, false);
//		printer.printVictimGender(sortedVictimGender, false);
		printer.printMeanAge4Race(sortedAgeRace, true);
		this.datasetService.closeSparkContext(); //CHIUSURA SPARK CONTEXT - DEV'ESSERE L'ULTIMA RIGA ESEGUITA
		
	}
	
	private JavaRDD<Tuple2<String, Double>> calculateVictimsToPopulationProportion(JavaRDD<PoliceKilling> rddPK, JavaRDD<StatePopulation> rddSP) {
		
		JavaPairRDD<String, Integer> state2victimCount = calculateKilledPeopleByState(rddPK)
				.mapToPair(tup -> tup);
		
		JavaPairRDD<String, Integer> state2population = rddSP
				.mapToPair(sp -> new Tuple2<>(sp.getState(), sp.getPopulation()));
		
		JavaPairRDD<String, Double> state2prop = state2victimCount
				.join(state2population)
				.mapToPair(tup -> new Tuple2<>(tup._1(), ((double)tup._2()._1() / (double)tup._2()._2())));
		
		JavaRDD<Tuple2<String, Double>> sorted = state2prop
				.map(tup -> new Tuple2<>(tup._1(), tup._2()))
				.sortBy(tup ->tup._2(), false, 1);
		
		return sorted;
		
	}	
	
	private JavaRDD<Tuple2<String, Integer>> calculateKilledPeopleByState(JavaRDD<PoliceKilling> rdd) {
		
		JavaPairRDD<String, Integer> state2count = rdd
				.mapToPair(pk -> new Tuple2<>(pk.getState(), 1))
				.reduceByKey((s1,s2) -> s1+s2);
		
		JavaRDD<Tuple2<String, Integer>> sorted = state2count
				.map(tup -> new Tuple2<>(tup._1(), tup._2()))
				.sortBy(tup ->tup._2(), false, 1);
		
		
		return sorted;
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
	
	private JavaPairRDD<String, Tuple2<Double, Double>> calculateEducationVsPoverty(JavaRDD<PercentOver25HighSchool> rddHS, JavaRDD<PercentagePeoplePoverty> rddPP) {
		
		JavaPairRDD<String, Double> state2meanHS = calculateStateMeanEducation(rddHS);
		JavaPairRDD<String, Double> state2poorness = calculatePoorestStates(rddPP)
				.mapToPair(tup -> tup);
		
		JavaPairRDD<String, Tuple2<Double,Double>> state2educ2poor = state2meanHS
				.join(state2poorness);
				
		return state2educ2poor;
	}

	private JavaPairRDD<String, Double> calculateStateMeanEducation(JavaRDD<PercentOver25HighSchool> rddHS) {
		
		JavaPairRDD<String,Double> state2cityPerc = rddHS.
				mapToPair(hs -> new Tuple2<>(hs.getState(), hs.getPercentCompletedHS()));
	
		JavaPairRDD<String,Integer> state2count = state2cityPerc
				.mapToPair(tup -> new Tuple2<>(tup._1(),1))
				.reduceByKey((s1,s2) -> s1+s2);
		
		JavaPairRDD<String,Double> state2meanHS = state2cityPerc
				.reduceByKey((s1,s2) -> s1+s2)
				.join(state2count)
				.mapToPair(tup -> new Tuple2<>(tup._1(), tup._2()._1() / tup._2()._2()));
		
		return state2meanHS;
	}
	
	//Calcolare età media uccisa per razza, quindi per ogni razza ho la media dell'eta
	//L'output deve essere : A: 23,5-----H:25.8 etc.. devo contare quante persone ci sono per la razza e fare la somma di tutte l'età
	// per ciascuna razze e poi fare l'RDD nel quale stampo la razza e faccio la media dei due valori calcolati in precedenza
	private JavaRDD<Tuple2<Character, Double>> calculateMeanAgeWithRace(JavaRDD<PoliceKilling> rdd){
		//In teoria dovrebbe esssere double, devo capire come fare il cast
		
		JavaPairRDD<Character, Integer> age4race = rdd
				.filter(f -> f.getAge() > -1)

				.mapToPair(pk -> new Tuple2<>(pk.getRace(),1))
				.reduceByKey((s1,s2) -> s1+s2);
		
		JavaPairRDD<Character, Integer> numVictim4race = rdd
				.filter(f -> f.getAge() > -1)
				.mapToPair(pk -> new Tuple2<>(pk.getRace(), pk.getAge() ))
				.reduceByKey((s1,s2) -> s1+s2);

		
		JavaPairRDD<Character, Double> meanAge4race = age4race
				.join(numVictim4race)
				.mapToPair(tup -> new Tuple2<>(tup._1(),Double.valueOf(( tup._2()._2() / tup._2()._1()))));
		
		
		
		JavaRDD<Tuple2<Character, Double>> sorted = meanAge4race
				.map(tup->tup)
				.sortBy(tup -> tup._2(), false, 1);
		
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
	
	private JavaRDD<Tuple2<String, Integer>> calculateMostCommonMannerOfDeath(JavaRDD<PoliceKilling> rdd){
		
		JavaPairRDD<String, Integer> typeOfDeath2count = rdd
				.mapToPair(pk -> new Tuple2<>(pk.getMannerOfDeath(), 1))
				.reduceByKey((s1,s2) -> s1+s2);
		
		JavaRDD<Tuple2<String,Integer>> sorted = typeOfDeath2count
				.map(tup -> tup)
				.sortBy(tup -> tup._2(), false, 1);
		
		return sorted;
	}
	
	private JavaRDD<Tuple2<String, Integer>> dangerousCities(JavaRDD<PoliceKilling> rdd){
		
		JavaPairRDD<String, Integer> nameOfCities = rdd
				.mapToPair(pk -> new Tuple2<>((pk.getCity()), 1))
				.reduceByKey((s1,s2) -> s1+s2);
		
		JavaRDD<Tuple2<String,Integer>> sorted = nameOfCities
				.map(tup -> tup)
				.sortBy(tup -> tup._2(), false, 1);
		
		return sorted;
		
	}
	//Devo creare RDD per analizzare se si aveva la body camera o no, dove il campo è costituito da un booleano true o false, posso fare un count sui true e uno sui false
	private JavaRDD<Tuple2<Boolean, Integer>> bodyCameraCheck(JavaRDD<PoliceKilling> rdd){
		
		JavaPairRDD<Boolean, Integer> countBodyCamera = rdd
				.mapToPair(pk -> new Tuple2<>(pk.getBodyCamera(), 1))
				.reduceByKey((s1,s2)->s1+s2);
		JavaRDD<Tuple2<Boolean,Integer>> sorted = countBodyCamera
				.map(tup -> tup)
				.sortBy(tup -> tup._2(), false, 1);
		
		return sorted;
		
	}
	
	private JavaRDD<Tuple2<String, Integer>> commonWeaponUse (JavaRDD<PoliceKilling> rdd){
		
		JavaPairRDD<String, Integer> commonWeapon = rdd
				.mapToPair(pk -> new Tuple2<>(pk.getArmed(), 1))
				.reduceByKey((s1,s2) -> s1+s2);
		
		JavaRDD<Tuple2<String, Integer>> sorted = commonWeapon
				.map(tup -> tup)
				.sortBy(tup -> tup._2(),false, 1);
		
		return sorted;
	}
	
	private JavaRDD<Tuple2<Character, Integer>> genderOfVictim(JavaRDD<PoliceKilling> rdd){
		
	JavaPairRDD<Character, Integer> gender = rdd
			.mapToPair(pk -> new Tuple2<>(pk.getGender(), 1))
			.reduceByKey((s1,s2) -> s1+2);
	
	JavaRDD<Tuple2<Character, Integer>> sorted = gender
			.map(tup->tup)
			.sortBy(tup -> tup._2(), false, 1);
	
	return sorted;
			
	

	
	}

	
}
