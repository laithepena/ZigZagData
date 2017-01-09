package dbTests;



import config.Config;
import conn.DataHubContext;

import conn.MySqlConnect;
import conn.ReportingVerticaConnect;

import java.io.FileInputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import akka.util.Collections;
import audit.DataContext;
import audit.FeedbackData;

public class Test1 {
	//static final String Profile="qa17";
	
	static Config dbProperties=new Config();
	
	
	static final String Profile=dbProperties.getProperty("PROFILE");
	
	//Properties dbProperties;
	
	
	
	private static final String MySqlQuery = "SELECT file_batch_id,file_name,status FROM file_load_control";
	private static final String MySqlQuery1 = "SELECT file_batch_id,file_name,status FROM file_load_control order by file_batch_id desc";	
	
	
	private static final String ReportingHubQuery = "SELECT file_batch_id,file_name,status FROM blackarrow_rh_"+Profile+".file_load_control";
		
	final static String rhQuery1="select local_date_id, sum(decisions) ,sum(base_impressions)  from blackarrow_rh_"+Profile+".ft_campaign_line_ad_unit_asset_program_daily group by local_date_id  order by local_date_id ";
	final static String rhQuery2="select local_date_id, sum(decisions) ,sum(base_impressions)  from blackarrow_rh_"+Profile+".ft_central_program_daily group by local_date_id  order by local_date_id";
	final static String rhQuery3="select local_date_id, sum(decisions) ,sum(base_impressions)  from blackarrow_rh_"+Profile+".ft_campaign_line_ad_unit_asset_time where local_date_id>19700101 group by local_date_id  order by local_date_id";
	final static String rhQuery4="select local_date_id, sum(decisions) ,sum(base_impressions)  from blackarrow_rh_"+Profile+".ft_central_account_program_daily  group by local_date_id  order by local_date_id";
	final static String rhQuery5="select local_date_id, sum(decisions) ,sum(base_impressions)  from blackarrow_rh_"+Profile+".ft_campaign_line_ad_unit_asset_adm_daily  group by local_date_id  order by local_date_id";
	final static String rhQuery6="select local_date_id, sum(decisions) ,sum(base_impressions)  from blackarrow_rh_"+Profile+".ft_central_ad_unit_time  group by local_date_id  order by local_date_id";

	
	
	final static String hdfsLocation="hdfs://qa-us1-dhub20.blackarrow-corp.com:8020//user/datahub/pdbms/"+Profile+"_dw.db/stg_event/*";
	final static String datahubQuery1="select local_date_id,sum(ad_decision),sum(actual_impression) FROM stg_event  group by local_date_id order by local_date_id";
	
	final static String datahubQueryIMPS="select a.EVENTTIME_EST as local_date_id,count(a.ASSETID_Cr) as imps FROM adr a  INNER JOIN feed f ON a.ASSETID_Cr=f.PSN_ASSETID_Cr group by a.EVENTTIME_EST";
	
	final static String datahubQueryDECS="select a.EVENTTIME_EST as local_date_id,count(a.ADCOUNT) as decs FROM adr a  group by a.EVENTTIME_EST";
	
	private static MySqlConnect m1;
	private static ReportingVerticaConnect v1;

	//Properties dbProperties;
	static DataHubContext dh;

	@BeforeClass
	public void beforeClass() throws SQLException {
		m1 = new MySqlConnect();
		
		v1=new ReportingVerticaConnect();
		
	   // dh=DataHubContext.getDataHubInstance("local");
		
		
	}

	/*@Test
	void Verify_MySql_MySql() throws SQLException {

		ResultSet rs;
		ResultSet rs11;

		DBOperations db1 = new DBOperations();
		rs = db1.generateMySqlResultset(m1, MySqlQuery);

		rs11 = db1.generateMySqlResultset(m1, MySqlQuery);

		
		
		Assert.assertTrue(db1.compareDB(rs, rs11),"Test Verify_MySql_MySql");
	}
	
	
	@Test
	void Verify_DH_with_RH_rhQuery1() throws SQLException {
		List<Row> lol;
		DBOperations db1 = new DBOperations();
		lol=db1.generateDataHubDataFrame(dh.getSqlContext(),hdfsLocation,datahubQuery1);
		
		ResultSet rs = db1.generateVerticaResultset(v1, rhQuery1);
		
		
		
		Assert.assertTrue(db1.compareDhRh(lol, rs),"Test Verify_DH_with_RH");
		
	
	}

	*/
	@Test
	void Verify_DH_AuditLogs_with_DH(){
		
		
		System.out.println("POMPU 786");
				
		DataContext dc=new DataContext();
		
		DataHubContext dhc=DataHubContext.getDataHubInstance("local");

			//	dc.executeDataStrategy(dhc,"stg_adr","logFile_location_adr");
				
		//############ FeedBack ###################################################################################################
			
			//	dc.executeDataStrategy(dhc,"stg_feed","logFile_location_feed");
				
				
				DataFrame df_adr=dc.getDataFrame(dhc,"typeAdrouter","logFile_location_adr");
				df_adr.show(1000, false);
		
				DataFrame df_feed=dc.getDataFrame(dhc,"typeFeedback","logFile_location_feed");
				df_feed.show(1000, false);
				
				df_adr.registerTempTable("adr");  df_feed.registerTempTable("feed");
		          
				//List<Row> l1_adr
				DataFrame dd1=dhc.getSqlContext().sql(datahubQueryIMPS).toDF();
				
				dd1.show(1000, false);
				
               // DataFrame dd2=dhc.getSqlContext().sql(datahubQueryDECS).toDF();
				
				//dd2.show(1000, false);
				//List<Row> l1_adr=df_adr.collectAsList();
				
				/*for(Row r:l1_adr){
					System.out.println("inside ROW");
					System.out.print(r.get(0)+" ");
					System.out.print(r.get(1)+" ");
					System.out.println(r.get(2)+" ");
				}*/
		 
		 //#######################################################################
		 
	/*List<Row> l1_adr =dh.getSqlContext().sql(datahubQuery1).collectAsList();
	
	List<Row> l1_feed =dh.getSqlContext().sql(datahubQuery1).collectAsList();
	
	System.out.println(l1_adr.size()+"  ALOHA  "+l1_feed.size());
		 
		// JavaRDD<Row> l1=df.toJavaRDD();
		 
	for(Row r:l1_adr){
		System.out.print(r.get(0)+" ");
		System.out.print(r.get(1)+" ");
		System.out.println(r.get(2)+" ");
	}
		 */
		
		
	}
	
	@AfterClass
	public void afterClass() {
		m1.disconnect();
		
		v1.disconnect();
		
	   // dh.disconnect();

	}

}
