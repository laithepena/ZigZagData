package audit;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import config.Config;
import conn.DataHubContext;
import scala.Tuple2;

public class DataContext {
	// private String filename;
	private DataStrategyInterface dataStrategy;
	private JavaRDD<?> rdd;
	private JavaPairRDD<String, String> pairRDD;
	private List<Tuple2<String, String>> listPairRDD;
	private Properties colProp;
	private DataFrame DF1;
	private SQLContext sqlContext;
	private String jdbcUrl;
	private FileInputStream input;
	private Config dbProperties;
	String AdrAuditType;
	String FeedAuditType;

	public DataContext() {
		// this.dataStrategy=dataStrategy;

		/*
		 * this.AdrAuditType=AdrAuditType;
		 * 
		 * this.FeedAuditType=FeedAuditType;
		 */
		//dbProperties = new Config();

		colProp = new Properties();
		input = null;
		try {
			input = new FileInputStream("resources/columnConfig.properties");
			colProp.load(input);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// ############## For Adrouter/FeedBack DataConfig Starts
	// ##########################/////////////////////////////////////

	// ############## For Adrouter/FeedBack DataConfig ENDS
	// ##########################/////////////////////////////////////

	// ############## For Adrouter/FeedBack Strategy Starts
	// ##########################/////////////////////////////////////
	public void executeDataStrategy(DataHubContext dc1, String logType, String logLocation) {}

	public DataFrame getDataFrame(DataHubContext dc1, String logType, String logLocation) {

		/*if (logLocation.equals("logFile_location_adr")) {
			this.AdrAuditType = logLocation;
			this.dataStrategy = new AdrouterData();

		} else if (logLocation.equals("logFile_location_feed")) {
			this.FeedAuditType = logLocation;
			this.dataStrategy = new FeedbackData();
		}*/

		

		if (logType.equals("typeAdrouter")) {
			
		//	this.AdrAuditType = logLocation;
			this.dataStrategy = new AdrouterData(); ///  Changed to New

			rdd = dc1.getSc().textFile(logLocation, 10);

			pairRDD = dc1.getSc().wholeTextFiles(logLocation);

			listPairRDD = pairRDD.collect();

		} else if (logType.equals("typeFeedback")) {
			
		//	this.FeedAuditType = logLocation;
			this.dataStrategy = new FeedbackData();
			rdd = dc1.getSc().textFile(logLocation, 10);

			pairRDD = dc1.getSc().wholeTextFiles(logLocation);
			
			//pairRDD=null;

		}

		// sqlContext = new SQLContext(sc); //-------------------------

		return DF1 = dataStrategy.funcAuditParsing(rdd, listPairRDD, dc1.getSqlContext(), colProp);

	}

	// ############## For Adrouter/FeedBack Strategy ENDS
	// ##########################/////////////////////////////////////

	public DataStrategyInterface getDataStrategy() {
		return dataStrategy;
	}

	public void setDataStrategy(DataStrategyInterface dataStrategy) {
		this.dataStrategy = dataStrategy;
	}

}
