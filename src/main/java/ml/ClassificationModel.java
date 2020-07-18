package ml;

import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import model.PercentOver25HighSchool;
import model.PercentagePeoplePoverty;
import model.PoliceKilling;
import model.ShareRaceCity;
import scala.Tuple2;
import service.DatasetService;

public class ClassificationModel {

	private DatasetService datasetService;

	public ClassificationModel(JavaSparkContext sc, DatasetService datasetService, String fileML){
		this.datasetService = datasetService;
		startClassificationModel(sc, fileML);
	}

	private void startClassificationModel(JavaSparkContext sc, String filePK){

		JavaPairRDD<String, ShareRaceCity> shared = this.datasetService.getShareRace()
				.mapToPair(f-> new Tuple2<>(f.getCity() + " " + f.getState(), f));

		JavaPairRDD<String,PoliceKilling> rdd = this.datasetService.getPoliceKilling()
				.filter(f -> f.getRace()!='?' && f.getRace()!='O')
				.filter(f -> f.getAge() >0)
				.filter(f -> !f.getArmed().isEmpty())
				.mapToPair(f -> new Tuple2<>(f.getCity() + " " + f.getState(), f ));

		JavaPairRDD<String, PercentOver25HighSchool> hs = this.datasetService.getPercentHighSchool()
				.mapToPair(f -> new Tuple2<>(f.getCity() + " " + f.getState(), f));

		//		JavaPairRDD<String, MedianHouseholdIncome> mhi = getMedianIncome()
		//				.mapToPair(f -> new Tuple2<>(f.getCity(), f));

		JavaPairRDD<String, PercentagePeoplePoverty> pov = this.datasetService.getPercentagePoverty()
				.mapToPair(f -> new Tuple2<>(f.getCity() + " " + f.getState(), f));


		JavaPairRDD<String, Tuple2<Tuple2<Tuple2<PoliceKilling, ShareRaceCity>, PercentOver25HighSchool>, PercentagePeoplePoverty>> join = rdd
				.join(shared)
				.join(hs)
				.join(pov);

		String arr[] = makeDictionary(join.map(f -> f._2()._1()._1()._1()));
		List<String> tb = join
				.map(f ->  {

					Character race = ' ';

					switch (f._2()._1()._1()._1().getRace()) {
					case 'W':
						race = '0';
						break;
					case 'B':
						race = '1';
						break;
					case 'H':
						race = '2';
						break;
					case 'A':
						race = '3';
						break;
					case 'N':
						race = '4';
						break;
					}

					String s = race + " " + 
							" 1:" + f._2()._1()._1()._1().getAge() +
							" 2:" + f._2()._1()._1()._2().getShareAsian() +
							" 3:" + f._2()._1()._1()._2().getShareBlack() +
							" 4:" + f._2()._1()._1()._2().getShareHispanic() + 
							" 5:" + f._2()._1()._1()._2().getShareNativeAmerican() + 
							" 6:" + f._2()._1()._1()._2().getShareWhite() + 
							" 7:" + f._2()._1()._2().getPercentCompletedHS() +
							" 8:" + f._2()._2().getPovertyRate();

					String ss = "";
					for(int i= 9; i<arr.length-1; i++) {

						if(arr[i].equals(f._2()._1()._1()._1().getArmed()))
							ss = ss+ " "+ i+":" +"1";
						else
							ss = ss+ " "+ i+":" +"0";
					}
					s = s+ss;
					return s;
				})
				.collect();

		System.out.println(tb.size());
		//		
		//		for(String s : tb)
		//			System.out.println(s);

		SparkSession ss = new SparkSession(JavaSparkContext.toSparkContext(sc));

		Dataset<Row> data = ss.read().format("libsvm").load(filePK);
		System.out.println(data.count());

		// generate the train/test split.
		Dataset<Row>[] tmp = data.randomSplit(new double[]{0.7, 0.3});
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
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");

		System.out.println("Precision Score = " + accuracy);
		System.out.println("Test Error = " + (1 - accuracy));

		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");

	}



	private String[] makeDictionary(JavaRDD<PoliceKilling> rdd) {

		Set<String> dic = (rdd
				.mapToPair(f -> new Tuple2<>(f.getArmed(), 1))
				.reduceByKey((s1,s2) -> s1+s2)
				.collectAsMap()
				.keySet()) ;


		// Creating a hash set of strings 

		int n = dic.size(); 
		String arr[] = new String[n]; 

		// Copying contents of s to arr[] 
		System.arraycopy(dic.toArray(), 0, arr, 0, n); 

		return arr;
	}
}
