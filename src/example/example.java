package example;
import dbTests.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.testng.Assert;

import config.Config;
import conn.DataHubContext;

public class example {
	
	static Config dbProperties = new Config(); // this is main properties object used eveywhere 

	static final String Profile = dbProperties.getProperty("PROFILE");
	static final String batch_id = dbProperties.getProperty("batch_id");
	// Properties dbProperties;
	static final String name_node = dbProperties.getProperty("name_node");

	private static final String jdbcUrl = "jdbc:mysql://" + name_node + ":3306/blackarrow_dh_" + Profile + "?user="
			+ Profile + "&password=" + Profile;

	final static String logFile_location_adr = "hdfs://" + name_node + ":8020//user/datahub/profiles/" + Profile
			+ "/in/audit_files/processed/" + batch_id + "/*_*_*_ba_audit_adrouter.log.gz";
	final static String logFile_location_feed = "hdfs://" + name_node + ":8020//user/datahub/profiles/" + Profile
			+ "/in/audit_files/processed/" + batch_id + "/*_*_*_ba_audit_feedback*.log.gz";
	final static String hdfsLocation = "hdfs://" + name_node + ":8020//user/datahub/pdbms/" + Profile
			+ "_dw.db/stg_event/batch_id=" + dbProperties.getProperty("batch_id");

	final static String datahubStgEventQuery1 = "select local_date_id as local_date_id,sum(actual_impression) as imps FROM stg_event where local_date_id is not null group by local_date_id  order by local_date_id ";

	final static String datahubStgEventQuery2 = "select local_date_id,sum(ad_decision) FROM stg_event where local_date_id is not null  group by local_date_id having sum(ad_decision)>0 order by local_date_id ";

	final static String datahubLogQueryIMPS = "select f.PSN_EVENTTIME_EST as local_date_id,count(a.ASSETID_Cr) as imps FROM TABLE_ADR a  INNER JOIN TABLE_FEED f ON a.ASSETID_Cr=f.PSN_ASSETID_Cr and f.PSN_EVENT='STARTED' group by f.PSN_EVENTTIME_EST order by f.PSN_EVENTTIME_EST";

	final static String datahubLogQueryDECS = "select a.EVENTTIME_EST as local_date_id,count(a.ADCOUNT) as decs FROM TABLE_ADR a where a.ADCOUNT=1 AND a.ASSETID_Cr is not null   group by a.EVENTTIME_EST order by a.EVENTTIME_EST";

	private static DataHubContext dhc;
	private static DBOperations db1;
	
	public static void main(String[] args) throws SQLException {
		
		dhc = DataHubContext.getDataHubInstance("local"); 
		
	
		List<Home> homes=new ArrayList<>();
		
		homes.add(new Home(600,"ramasish")); //homes.add(new Home(200,"antilope")); 
		
		homes.add(new Home(301,"galaxy")); homes.add(new Home(501,"beverly"));
		
		JavaRDD<Home> homeRDD =  dhc.getSc().parallelize(homes);
		
		
		
		
		 JavaRDD<Home> r2 = homeRDD.sortBy(  i -> 
		 {return i.getPrice(); }, 
		 true,
		 1 );
		
		// homeRDD.so
		
		// Assert( homeRDD.subtract(r2));
		//(actual, expected);
		 Assert.assertEquals(homeRDD.collect(), r2.collect(),"not pass");
		 
		List<Home> hh=r2.collect();
		
		for(Home h:hh){
			System.out.println(h.getPrice()+" "+h.getName());
		}
		
db1 = new DBOperations();
		
		//-----------------------------------
		
		DataFrame df_dh = db1.generateDataHubDataFrame(dhc.getSqlContext(), hdfsLocation, datahubStgEventQuery1,"stg_event");
		df_dh.show(1000, false);
		List<datahubRow> ll = new ArrayList<>();
		
		df_dh.printSchema();
		
		JavaRDD<Row> jd=df_dh.toJavaRDD();
		
		JavaRDD<datahubRow> l2=jd.map(s->{
		
			datahubRow d1=new datahubRow(); 
			d1.setLocal_date_id(s.get(0).toString());
			d1.setMet(s.getLong(1));
			return d1;
		});
		
		//l2.sortBy(1, 2, 10);
		
		DataFrame fileDF = dhc.getSqlContext().createDataFrame(l2, datahubRow.class);
		fileDF.show(1000, false);
		
		
	}

}
