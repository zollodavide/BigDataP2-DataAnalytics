import org.apache.spark.SparkConf;

public class DataAnalytics {

	public static void main(String[] args) {

		
		Analytics analytics = new Analytics(args[0], args[1], args[2], args[3], args[4], args[5]);
		analytics.run();
		
	}
}
