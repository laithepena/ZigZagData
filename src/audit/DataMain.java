package audit;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;

import config.Config;
import conn.DataHubContext;

//#########################################To RUN ####################################################################################
//    Generate a JAR file and copy it over to the ENV Box
//     
//
//#############################################################################################################################

public class DataMain {

	static Config dbProperties = new Config();

	static final String Profile = dbProperties.getProperty("PROFILE");
	static final String batch_id = dbProperties.getProperty("batch_id");
	final static String logFile_location_adr = "hdfs://qa-us1-dhub20.blackarrow-corp.com:8020//user/datahub/profiles/"
			+ Profile + "/in/audit_files/processed/" + batch_id + "/*_*_*_ba_audit_adrouter.log.gz";
	final static String logFile_location_feed = "hdfs://qa-us1-dhub20.blackarrow-corp.com:8020//user/datahub/profiles/"
			+ Profile + "/in/audit_files/processed/" + batch_id + "/*_*_*_ba_audit_feedback*.log.gz";

	private static final String jdbcUrl = "jdbc:mysql://qa-us1-dhub20.blackarrow-corp.com:3306/blackarrow_dh_" + Profile
			+ "?user=" + Profile + "&password=" + Profile;

	public static void main(String[] args) {

		System.out.println("POMPU 786");

		DataContext dc = new DataContext();

		// ############ Adrouter
		// ##################################################################################################

		DataHubContext dhc = DataHubContext.getDataHubInstance("local");

		//dhc = DataHubContext.getDataHubInstance("local");

		//DataFrame df_adr = dc.getDataFrame(dhc, "typeAdrouter", logFile_location_adr);
		//df_adr.show(1000, false);
		
		

		//df_adr.write().mode(SaveMode.Overwrite).jdbc(jdbcUrl, "pom_adr", dbProperties.getDbProp());

		DataFrame df_feed = dc.getDataFrame(dhc, "typeFeedback", logFile_location_feed);
		df_feed.show(1000, false);

		df_feed.write().mode(SaveMode.Overwrite).jdbc(jdbcUrl, "pom_feed", dbProperties.getDbProp());

		System.out.println("............DONE DONE DONE......HAVE A GOOD DAY......DONE DONE DONE.....");
	}

}
