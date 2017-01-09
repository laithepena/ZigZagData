package conn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class DataHubContext {
	
	static SparkConf conf;
	public  SparkConf getConf() {
		return conf;
	}


	public static void setConf(SparkConf conf) {
		DataHubContext.conf = conf;
	}


	public  JavaSparkContext getSc() {
		return sc;
	}


	public  void setSc(JavaSparkContext sc) {
		DataHubContext.sc = sc;
	}


	public  SQLContext getSqlContext() {
		return sqlContext;
	}


	public static void setSqlContext(SQLContext sqlContext) {
		DataHubContext.sqlContext = sqlContext;
	}

	static JavaSparkContext sc;
	static SQLContext sqlContext;
	
	static String str;
	
private	 DataHubContext(String s){ // Private Constructor Singleton Implementation
	this.str=s;
		conf = new SparkConf().setAppName("PompuApp").setMaster(this.str); // this is for local mode when running from eclipse
         sc = new JavaSparkContext(conf);
        
        sqlContext = new SQLContext(sc);
      //  return sqlContext;
	}


public static DataHubContext getDataHubInstance(String clusterMode){
	
	DataHubContext d1=new DataHubContext("local");
	//d1.str=clusterMode;
	return d1;
	//return null;
	
}

public void disconnect() {
	sc.stop();
	
}

}
