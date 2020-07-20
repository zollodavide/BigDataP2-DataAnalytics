import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import model.*;
import scala.Tuple2;
import service.DatasetService;
import utility.Printer;

public class Analytics {

	private DatasetService datasetService; //SINGLETON -> NON INSTANZIARE DUE VOLTE
	private Printer printer;

	public Analytics(String fileMI, String filePP, String filePHS, String filePK, String fileSR, String fileSP, String fileSC, String fileML) {
		this.datasetService = new DatasetService(fileMI, filePP, filePHS, filePK, fileSR, fileSP, fileSC, fileML);
		this.printer = new Printer();


	}


	public void run() {


		JavaRDD<Tuple2<String, Double>> sortedPoorestStates = 
				calculatePoorestStates(this.datasetService.getPercentagePoverty());
		JavaPairRDD<String, Double> sortedMeanEducation =
				calculateStateMeanEducation(this.datasetService.getPercentHighSchool());
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
		JavaRDD<Tuple2<Integer, Integer>> sortedMonth =
				killedByMonth(this.datasetService.getPoliceKilling());

		JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Tuple2<Double,Double>, Double>, Double>, Double>> sortedRace4state =
				unionRace4State(this.datasetService.getShareRace());

		//		

		List<Tuple2<String,Integer>>sd = sortedVictimByState.collect();
		List<Tuple2<String,Double>>sdd = sortedState.collect();
		//		
		//		for(Tuple2<String,Double> a: sdd)
		//			System.out.println(a._1() + ": " + a._2());
		//		printer.printVictimsByRace(sortedRaceVictims,true);
		//		printer.printMostCommonNames(sortedCommonVictimNames,false);
		//		printer.printPoorestStates(sortedPoorestStates,true);
		//		printer.printEducationVSPoverty(educationVSpoverty, true);
		//		printer.printAllResults();
		//		printer.printMostCommonMannerOfDeath(sortedCommonMannerOfDeath, true);
		//		printer.printDangerousCities(sortedDangerousCities, true);
		//		printer.printCountBodyCamera(sortedBodyCamera, true);
		//		printer.printCommonWeapon(sortedCommonWeapon, true);
		//		printer.printVictime4state(sortedVictimByState, false);
		//		printer.printVictimGender(sortedVictimGender, true);
		//		printer.printMeanAge4Race(sortedAgeRace, true);
		//		printer.printKilledByMOnth(sortedMonth, true);
		//		printer.printAllRace4state(sortedRace4state, true);
		//		printer.printVictime4state(sortedVictimByState, true);
		//		printer.printStateMeanEducation(sortedMeanEducation, true);

//		List<Tuple2<Character, Double>> asd = killedByRaceProportion(this.datasetService.getPoliceKilling()).collect();
//
//		for(Tuple2<Character, Double> s : asd)
//			System.out.println(s._1() + " " + s._2());

		
		List<Tuple2<String, Double>> asd = calculateVictimsToPopulationProportion(this.datasetService.getPoliceKilling(),this.datasetService.getStatePopulation()).collect();

		for(Tuple2<String, Double> s : asd)
			System.out.println(s._1() + " " + s._2());

		this.datasetService.closeSparkContext(); //CHIUSURA SPARK CONTEXT - DEV'ESSERE L'ULTIMA RIGA ESEGUITA

	}


	private JavaPairRDD<String, Double> calculateMedianIncomeByState(JavaRDD<MedianHouseholdIncome> rddMH) {

		JavaPairRDD<String,Double> state2Income = rddMH
				.mapToPair(f -> new Tuple2<>(f.getState(), f.getMedianIncome()))
				.reduceByKey((s1,s2) -> s1+s2);

		JavaPairRDD<String,Integer> state2count = rddMH
				.mapToPair(f -> new Tuple2<>(f.getState(), 1))
				.reduceByKey((s1,s2) -> s1+s2);

		JavaPairRDD<String,Double> state2MedianIncome = state2Income
				.join(state2count)
				.mapToPair(f -> new Tuple2<>(f._1(), f._2()._1() /f._2()._2()));

		return state2MedianIncome;
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

	//Per ogni razza devo identificare l'arma pi utilizzata (ex: A: Gun)
	//	private JavaPairRDD<Character,Tuple2<String, Integer>> calculateMostArmedUse4Race(JavaRDD<PoliceKilling> rdd){
	//		
	//		JavaPairRDD<Character, String> raceWithArm = rdd
	//				.mapToPair(pk -> new Tuple2<>(pk.getRace(), pk.getArmed()));
	//
	//		
	//		JavaPairRDD<String, Integer> countWeapon = raceWithArm
	//				.mapToPair(tup -> new Tuple2<>(tup._1() + " " + tup._2(), 1))
	//				.reduceByKey((s1,s2) -> s1+s2);
	//		
	//		//Per ogni razza prendere l' arma più utilizzata
	//		JavaPairRDD<Character, Integer> maxWeaponUse = countWeapon
	//				.mapToPair(tup -> new Tuple2<>(tup._1(), max(tup._2())));
	//		
	//		
	//		
	////	
	////		JavaPairRDD<Character, Tuple2<String, Integer>> mostUseArm = countArm
	////				.join(raceWithArm)
	////      			.mapToPair(tup -> new Tuple2<>(tup._1(), tup._2()._1()));
	//	
	//
	//		return mostUseArm;
	//		
	//	}

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
	
	
	private JavaRDD<Tuple2<Character, Double>> killedByRaceProportion(JavaRDD<PoliceKilling> rddPK) {

		JavaPairRDD<Character, Integer> rdd = calculateKilledPeopleByRace(rddPK).mapToPair(f -> new Tuple2<>(f._1(), f._2()));

		JavaPairRDD<Character, Double> asd = rdd.mapToPair(f ->{

			if(f._1().equals('B'))
				return new Tuple2<>(f._1(), f._2()/38929319.);

			else if (f._1().equals('W'))
				return new Tuple2<>(f._1(), f._2()/223553265.);


			else if (f._1().equals('H'))
				return new Tuple2<>(f._1(), f._2()/50477594.);


			else if (f._1().equals('A'))
				return new Tuple2<>(f._1(), f._2()/14674252.);


			else if (f._1().equals('N'))
				return new Tuple2<>(f._1(), f._2()/2932248.);
			
			else return null;
		});
		
		JavaRDD<Tuple2<Character, Double>> sorted = asd.filter(f -> f!=null)
				.map(tup -> new Tuple2<>(tup._1(), tup._2()))
				.sortBy(tup ->tup._2(), false, 1);


		return sorted;
	}


	private JavaRDD<Tuple2<Character, Double>> calculateMeanAgeWithRace(JavaRDD<PoliceKilling> rdd){

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

	private JavaRDD<Tuple2<Integer, Integer>> killedByMonth(JavaRDD<PoliceKilling> rdd){

		JavaPairRDD<Integer, Integer> monthWithMostKill = rdd
				.mapToPair(pk -> new Tuple2<>(pk.getDate().getMonth(), 1))
				.reduceByKey((s1,s2) -> s1+s2);


		JavaRDD<Tuple2<Integer, Integer>> sorted = monthWithMostKill
				.map(tup -> tup)
				.sortBy(tup -> tup._1(), true, 1);

		return sorted;
	}

	//Calcolato la media della distribuzione per ogni stato della razza Asiatica
	private JavaPairRDD<String, Double> shareRaceAsianByState(JavaRDD<ShareRaceCity> rddSR){

		JavaPairRDD<String,Integer> countAsian = rddSR
				.mapToPair(sr -> new Tuple2<>(sr.getState(), 1))
				.reduceByKey((s1,s2) -> s1+s2);
		JavaPairRDD<String, Double> stateAsian = rddSR
				.mapToPair(sr -> new Tuple2<>(sr.getState(), sr.getShareAsian()))
				.reduceByKey((s1,s2) -> s1+s2);		
		JavaPairRDD<String,Double> meanRaceAsian4state = stateAsian
				.reduceByKey((s1,s2) -> s1+s2)
				.join(countAsian)
				.mapToPair(tup -> new Tuple2<>(tup._1(), Double.valueOf(tup._2()._1() / tup._2()._2())));


		return meanRaceAsian4state;

	}

	private JavaPairRDD<String, Double> shareRaceWhiteByState(JavaRDD<ShareRaceCity> rddSR){

		JavaPairRDD<String,Integer> countWhite = rddSR
				.mapToPair(sr -> new Tuple2<>(sr.getState(), 1))
				.reduceByKey((s1,s2) -> s1+s2);
		JavaPairRDD<String, Double> stateWhite = rddSR
				.mapToPair(sr -> new Tuple2<>(sr.getState(), sr.getShareWhite()))
				.reduceByKey((s1,s2) -> s1+s2);		
		JavaPairRDD<String,Double> meanRaceWhite4state = stateWhite
				.reduceByKey((s1,s2) -> s1+s2)
				.join(countWhite)
				.mapToPair(tup -> new Tuple2<>(tup._1(), Double.valueOf(tup._2()._1() / tup._2()._2())));




		return meanRaceWhite4state;


	}

	private JavaPairRDD<String, Double> shareRaceBlackByState(JavaRDD<ShareRaceCity> rddSR){

		JavaPairRDD<String,Integer> countBlack = rddSR
				.mapToPair(sr -> new Tuple2<>(sr.getState(), 1))
				.reduceByKey((s1,s2) -> s1+s2);
		JavaPairRDD<String, Double> stateBlack = rddSR
				.mapToPair(sr -> new Tuple2<>(sr.getState(), sr.getShareBlack()))
				.reduceByKey((s1,s2) -> s1+s2);		
		JavaPairRDD<String,Double> meanRaceBlack4state = stateBlack
				.reduceByKey((s1,s2) -> s1+s2)
				.join(countBlack)
				.mapToPair(tup -> new Tuple2<>(tup._1(), Double.valueOf(tup._2()._1() / tup._2()._2())));




		return meanRaceBlack4state;


	}

	private JavaPairRDD<String, Double> shareRaceNativeAmericanByState(JavaRDD<ShareRaceCity> rddSR){

		JavaPairRDD<String,Integer> countNativeAmerican = rddSR
				.mapToPair(sr -> new Tuple2<>(sr.getState(), 1))
				.reduceByKey((s1,s2) -> s1+s2);
		JavaPairRDD<String, Double> stateNativeAmerican = rddSR
				.mapToPair(sr -> new Tuple2<>(sr.getState(), sr.getShareNativeAmerican()))
				.reduceByKey((s1,s2) -> s1+s2);		
		JavaPairRDD<String,Double> meanRaceNativeAmerican4state = stateNativeAmerican
				.reduceByKey((s1,s2) -> s1+s2)
				.join(countNativeAmerican)
				.mapToPair(tup -> new Tuple2<>(tup._1(), Double.valueOf(tup._2()._1() / tup._2()._2())));




		return meanRaceNativeAmerican4state;


	}

	private JavaPairRDD<String, Double> shareRaceHispanicByState(JavaRDD<ShareRaceCity> rddSR){

		JavaPairRDD<String,Integer> countHispanic = rddSR
				.mapToPair(sr -> new Tuple2<>(sr.getState(), 1))
				.reduceByKey((s1,s2) -> s1+s2);
		JavaPairRDD<String, Double> stateHispanic = rddSR
				.mapToPair(sr -> new Tuple2<>(sr.getState(), sr.getShareHispanic()))
				.reduceByKey((s1,s2) -> s1+s2);		
		JavaPairRDD<String,Double> meanRaceHispanic4state = stateHispanic
				.reduceByKey((s1,s2) -> s1+s2)
				.join(countHispanic)
				.mapToPair(tup -> new Tuple2<>(tup._1(), Double.valueOf(tup._2()._1() / tup._2()._2())));




		return meanRaceHispanic4state;


	}


	private JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Tuple2<Double,Double>, Double>, Double>, Double>> unionRace4State(JavaRDD<ShareRaceCity> rddSR){

		JavaPairRDD<String, Double> asian4state = shareRaceAsianByState(rddSR);
		JavaPairRDD<String, Double> white4state = shareRaceWhiteByState(rddSR);
		JavaPairRDD<String, Double> black4state = shareRaceBlackByState(rddSR);
		JavaPairRDD<String, Double> nativeAmerican4state = shareRaceNativeAmericanByState(rddSR);
		JavaPairRDD<String, Double> hispanic4state = shareRaceHispanicByState(rddSR);


		JavaPairRDD<String, Tuple2<Double, Double>> joinAsianWhite = asian4state
				.join(white4state);

		JavaPairRDD<String, Tuple2<Tuple2<Double, Double>,Double>> joinAsWhBlack = joinAsianWhite
				.join(black4state);

		JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Double,Double>,Double>,Double>> joinAsWhBlNativeAmerican = joinAsWhBlack
				.join(nativeAmerican4state);

		JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Tuple2<Double,Double>, Double>, Double>, Double>> joinAll = joinAsWhBlNativeAmerican
				.join(hispanic4state);

		return joinAll;


	}





}
