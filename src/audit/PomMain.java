package audit;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaSparkContext;

//#########################################To RUN ####################################################################################
//    Generate a JAR file and copy it over to the ENV Box
//     
//
//#############################################################################################################################

public class PomMain {
	public static void main(String[] args) {
		
//  SparkConf is common to the whole program - so there is only one conf
		
		//SparkConf conf = new SparkConf().setAppName("PompuApp").set("spark.executor.memory", "16g"); // need to deComment this one for Cluster Mode
		
		SparkConf conf = new SparkConf().setAppName("PompuApp").setMaster("local"); // this is for local mode when running from eclipse
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        
 //############ Adrouter ##################################################################################################	  
		AdrouterData adr=new AdrouterData();
			
		DataContext D1=new DataContext(adr);
		D1.setDataConfig(sc,"logFile_location_adr");
		D1.executeDataStrategy(sc, "stg_adr");
		
	
//############ FeedBack ###################################################################################################
	FeedbackData feed=new FeedbackData();
		
		DataContext D2=new DataContext(feed);
		D2.setDataConfig(sc, "logFile_location_feed");
		D2.executeDataStrategy(sc,"stg_feedback");
		
		System.out.println("............DONE DONE DONE......HAVE A GOOD DAY......DONE DONE DONE.....");
	}

}
