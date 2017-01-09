package dbTests;

import java.sql.SQLException;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import audit.DataContext;
import config.Config;
import conn.DataHubContext;

//##########################################################################################################################

//   Feature Explained - This for to Compare Decisions and impressions between ActualAudit logs and stg_event in hdfs
// 1. Parses the audit Logs and generates DataFrames - registers as two Tables TABLE_ADR and TABLE_FEED
// 2. gets a dataframe df_join  from the above two tables  - one for impressions and one for decisions
// 3. Directly queries the PDBMS for the Profile and gets a DataFrame
// 4. Now compares the DataFrames at step 2 and step 3 , the comparing technique is by converting  to List<Row>  and comparing them
// 5. To Run it from eclipse get the packages - Just two config parameters - we need to enter Profile and batch_id on db-properties.flat
//   
//
//                      Author: Shivadeep Borgohain                                     
//###########################################################################################################################

public class Test_Logs_With_DH {

	static Config dbProperties = new Config(); // this is main properties object
												// used eveywhere

	static final String Profile = dbProperties.getProperty("PROFILE");
	static final String batch_id = dbProperties.getProperty("batch_id");
	
	static final String name_node = dbProperties.getProperty("name_node");
	
	final static String jdbcUrl="jdbc:mysql://qa-us1-dhub20.blackarrow-corp.com:3306/blackarrow_dh_"+Profile+"?user="+Profile+"&password="+Profile;

	final static String logFile_location_adr = "hdfs://" + name_node + ":8020//user/datahub/profiles/" + Profile
			+ "/in/audit_files/processed/" + batch_id + "/*_*_*_ba_audit_adrouter.log.gz";
	final static String logFile_location_feed = "hdfs://" + name_node + ":8020//user/datahub/profiles/" + Profile
			+ "/in/audit_files/processed/" + batch_id + "/*_*_*_ba_audit_feedback*.log.gz";

	final static String hdfsLocation_stg_event = "hdfs://" + name_node + ":8020//user/datahub/pdbms/" + Profile
			+ "_dw.db/stg_event/batch_id=" + dbProperties.getProperty("batch_id");

	/// hdfs://qa-us1-dhub20.blackarrow-corp.com:8020//user/datahub/pdbms/qa_us1_app21_dw.db/stg_adrouter/batch_id=20161221195100987

	final static String hdfsLocation_stg_adrouter = "hdfs://" + name_node + ":8020//user/datahub/pdbms/" + Profile
			+ "_dw.db/stg_adrouter/" + "batch_id=" + dbProperties.getProperty("batch_id");

	final static String hdfsLocation_stg_feedback = "hdfs://" + name_node + ":8020//user/datahub/pdbms/" + Profile
			+ "_dw.db/stg_feedback/batch_id=" + dbProperties.getProperty("batch_id");

	// --------------------------------------------Queries
	// ------------------------------------------------------//
	
	
	final static String datahubStgEventQuery1 = "select local_date_id as local_date_id,sum(actual_impression) as imps FROM stg_event where local_date_id is not null group by local_date_id having sum(actual_impression)>0 order by local_date_id ";

	final static String datahubStgEventQuery2 = "select local_date_id,sum(ad_decision) FROM stg_event where local_date_id is not null   group by local_date_id having sum(ad_decision)>0 order by local_date_id ";

	final static String datahubStgEventQuery3 = "select batch_id as total FROM stg_event where event_type like 'adrouter'   ";

	final static String datahubStgEventQuery4 = "select batch_id as total FROM stg_event where event_type like '%feedback%'  ";

	final static String datahubStgAdrouterQuery5 = "select batch_id as total FROM stg_adrouter ";

	final static String datahubStgFeedBackQuery6 = "select batch_id as total FROM stg_feedback";

	final static String datahubLogQueryIMPS = "select f.PSN_EVENTTIME_EST as local_date_id,count(a.ASSETID_Cr) as imps FROM TABLE_ADR a  INNER JOIN TABLE_FEED f ON a.ASSETID_Cr=f.PSN_ASSETID_Cr and f.PSN_EVENT='STARTED' group by f.PSN_EVENTTIME_EST order by f.PSN_EVENTTIME_EST";

	final static String datahubLogQueryDECS = "select a.EVENTTIME_EST as local_date_id,count(a.ASSETID_Cr) as decs FROM TABLE_ADR a where  a.ASSETID_Cr is not null  group by a.EVENTTIME_EST order by a.EVENTTIME_EST";

	final static String datahubLogQueryADR = "select LOGTIME_PST  FROM TABLE_ADR ";
	final static String datahubLogQueryFEED = "select PSN_LOGTIME_PST  FROM TABLE_FEED ";

	private static DataHubContext dhc;
	private static DBOperations db1;

	@BeforeClass
	public void beforeClass() throws SQLException {

		System.out.println("POMPU 786");

		DataContext dc = new DataContext(); // this is data Context for the
											// specific Audit Log processing.
											// Use this to read raw audit Logs

		dhc = DataHubContext.getDataHubInstance("local"); // first step to get -
															// Singleton Class .
															// Use this to read
															// any Parquet
															// Files/any files
															// in HDFS

		DataFrame df_adr = dc.getDataFrame(dhc, "typeAdrouter", logFile_location_adr); // generation
																						// of
																						// df_adr
																						// from
																						// adr
																						// audit
																						// Logs
		DataFrame df_feed = dc.getDataFrame(dhc, "typeFeedback", logFile_location_feed); // generation
																							// of
																							// df_feed
																							// from
																							// feedback
																							// audit
																							// Logs

		df_adr.show(1000, false);

		df_feed.show(1000, false);
		
		df_adr.registerTempTable("TABLE_ADR"); // registering DataFrame
		// df_adr
		// as Tables
         df_feed.registerTempTable("TABLE_FEED"); // registering DataFrame
			// df_feed as Tables

		if (dbProperties.getProperty("SaveAuditLogTable").equals("YES")) {
			
	//	jdbcUrl=jdbc:mysql://qa-us1-dhub20.blackarrow-corp.com:3306/blackarrow_dh_qa_us1_app21?user=qa_us1_app21&password=qa_us1_app21

			
			df_adr.write().mode(SaveMode.Overwrite).jdbc(jdbcUrl, "TABLE_ADR",dbProperties.getDbProp()); // saving of df_adr to MySql jus for refrence - this is not used in the comparison- we use the temp tables
			
			System.out.println("ZZZZZZ");
			df_feed.printSchema();
			df_feed.show(1000, false);
			df_feed.write().mode(SaveMode.Overwrite).jdbc(jdbcUrl, "TABLE_FEED",dbProperties.getDbProp());
			
			
		}
		/*
		 * 
		 * df_adr.show(1000, false);
		 * 
		 * df_adr.write().mode(SaveMode.Overwrite).jdbc(jdbcUrl, "TABLE_ADR",
		 * dbProperties.getDbProp()); // saving of df_adr to MySql jus for
		 * refrence - this is not used in the comparison- we use the temp tables
		 * 
		 * df_feed.show(1000, false);
		 * 
		 * df_feed.write().mode(SaveMode.Overwrite).jdbc(jdbcUrl, "TABLE_FEED",dbProperties.getDbProp()); // saving of df_adr to MySql jus for
		 * refrence - this is not used in the comparison- we use the temp tables
		 * 
		 * 
		 * 
		 */
		db1 = new DBOperations();

	}

	// --------------------------------------------Test Methods starts
	// ----------------------------------------------------//
	
	 @Test void Verify_ActualImpressions_between_AuditLogs_and_stg_event()
	 throws SQLException {
	 
	  Assert.assertTrue(
	 db1.compareTwoListsOfRow(dhc.getSqlContext().sql(datahubLogQueryIMPS).
	  toDF().collectAsList(), db1.generateDataHubDataFrame(dhc.getSqlContext(),
	 hdfsLocation_stg_event, datahubStgEventQuery1,
	 "stg_event").collectAsList()),
	  " --- Verify_ActualImpressions_between_AuditLogs_and_stg_event for Profile="
	  + Profile + " and batch_id=" + batch_id + " ");
	  
	  }
	  
	 @Test void Verify_Decisions_between_AuditLogs_and_stg_event() throws
	  SQLException {
	  
	  Assert.assertTrue(
	  db1.compareTwoListsOfRow(dhc.getSqlContext().sql(datahubLogQueryDECS).
	  toDF().collectAsList(), db1.generateDataHubDataFrame(dhc.getSqlContext(),
	  hdfsLocation_stg_event, datahubStgEventQuery2,
	  "stg_event").collectAsList()),
	  " --- Verify_Decisions_between_AuditLogs_and_stg_event for Profile=" +
	  Profile + " and batch_id=" + batch_id + " ");
	  
	  }
	 

	@Test
	void Verify_EventTypeCount_between_stg_adrouter_and_stg_event() throws SQLException {

		Long r1 = db1.generateDataHubDataFrame(dhc.getSqlContext(), hdfsLocation_stg_adrouter, datahubStgAdrouterQuery5,
				"stg_adrouter").count();

		Long r2 = db1.generateDataHubDataFrame(dhc.getSqlContext(), hdfsLocation_stg_event, datahubStgEventQuery3,
				"stg_event").count();

		Assert.assertEquals(r1, r2, "-- Verify_EventTypeCount_between_stg_adrouter_and_stg_event -- Profile=" + Profile
				+ " and batch_id=" + batch_id + " ");
	}

	@Test
	void Verify_EventTypeCount_between_stg_feedback_and_stg_event() throws SQLException {

		Long r1 = db1.generateDataHubDataFrame(dhc.getSqlContext(), hdfsLocation_stg_feedback, datahubStgFeedBackQuery6,
				"stg_feedback").count();

		Long r2 = db1.generateDataHubDataFrame(dhc.getSqlContext(), hdfsLocation_stg_event, datahubStgEventQuery4,
				"stg_event").count();

		Assert.assertEquals(r1, r2, "-- Verify_EventTypeCount_between_stg_feedback_and_stg_event -- Profile=" + Profile
				+ " and batch_id=" + batch_id + " ");
	}

	@Test
	void Verify_EventTypeCount_between_TABLE_ADR_and_stg_event() throws SQLException {

		Long r1 = dhc.getSqlContext().sql(datahubLogQueryADR).toDF().count();
		Long r2 = db1.generateDataHubDataFrame(dhc.getSqlContext(), hdfsLocation_stg_event, datahubStgEventQuery3,
				"stg_event").count();

		Assert.assertEquals(r1, r2, "--Verify_EventTypeCount_between_TABLE_ADR_and_stg_event -- Profile=" + Profile
				+ " and batch_id=" + batch_id + " ");
	}

	@Test
	void Verify_EventTypeCount_between_TABLE_FEED_and_stg_event() throws SQLException {

		Long r1 = dhc.getSqlContext().sql(datahubLogQueryFEED).toDF().count();
		Long r2 = db1.generateDataHubDataFrame(dhc.getSqlContext(), hdfsLocation_stg_event, datahubStgEventQuery4,
				"stg_event").count();

		Assert.assertEquals(r1, r2, "--Verify_EventTypeCount_between_TABLE_FEED_and_stg_event -- Profile=" + Profile
				+ " and batch_id=" + batch_id + " ");
	}

	@AfterClass

	public void afterClass() {

		dhc.disconnect();

	}
}
