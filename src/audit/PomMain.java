package audit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class PomMain {
	public static void main(String[] args) {
		
		//SparkConf conf = new SparkConf().setAppName("PompuApp").set("spark.executor.memory", "16g");
		
		SparkConf conf = new SparkConf().setAppName("PompuApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
       
		AdrouterData adr=new AdrouterData();
		
		DataContext D1=new DataContext(adr);
		D1.setDataConfig(sc,"logFile_location_adr");
		D1.executeDataStrategy(sc, "stg_adr");
		
	
//############ FeedBack ##################################################################################################
	FeedbackData feed=new FeedbackData();
		
		DataContext D2=new DataContext(feed);
		D2.setDataConfig(sc, "logFile_location_feed");
		D2.executeDataStrategy(sc,"stg_feedback");
		
		System.out.println("............DONE DONE DONE.................");
	}

}
