package dbTests;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import conn.MySqlConnect;
import conn.ReportingVerticaConnect;

public class DBOperations {
	
	
	/*rdd = sc.textFile(dbProperties.getProperty(AuditType), 10);
	
	 pairRDD=sc.wholeTextFiles(dbProperties.getProperty(AuditType));*/
	
	/*void getRDDFromTextFile(SparkConf sc,String loc){
		sc.textFile(dbProperties.getProperty(AuditType), 10);
	}
	 
	void getPairRDDFromTextFile(){
		
	}
	*/
	
	 
	 

	ResultSet generateMySqlResultset(MySqlConnect m1, String query) throws SQLException {
		ResultSet rs;
		java.sql.Statement stmt = m1.connect().createStatement();
		rs = stmt.executeQuery(query);

		return rs;

	}

	ResultSet generateVerticaResultset(ReportingVerticaConnect v1, String query) throws SQLException {
		ResultSet rs;
		java.sql.Statement stmt = v1.connect().createStatement();
		rs = stmt.executeQuery(query);

		return rs;

	}

	// DataFrame DF1=d1.read().parquet("hdfsLocation");

	public DataFrame generateDataHubDataFrame(SQLContext d1, String location, String query,String tableName) throws SQLException {
		// DataFrame df;

		DataFrame df = d1.read().parquet(location);

		List<Row> ListOfRowDataHUB1;

		df.registerTempTable(tableName);
		
	

		DataFrame df2 = d1.sql(query).toDF();  //collectAsList();
		 
		 
		return df2;

	}

	boolean compareDB(ResultSet rs, ResultSet rs11) throws SQLException {

		int[] col = { 1, 2, 3 };
		boolean f = true;
		while (rs.next() && rs11.next()) {

			if (!rs.getObject(col[0]).equals(rs11.getObject(col[0]))) {
				f = false;
				break;
			}

			if (!rs.getObject(col[1]).equals(rs11.getObject(col[1]))) {
				f = false;
				break;
			}

			if (!rs.getObject(col[2]).equals(rs11.getObject(col[2]))) {
				f = false;
				break;
			}

			// , "not matched 1");

			/*
			 * Assert.assertEquals(rs.getObject(col[1]), rs11.getObject(col[1]),
			 * "not matched 2");
			 * 
			 * Assert.assertEquals(rs.getObject(col[2]), rs11.getObject(col[2]),
			 * "not matched 3");
			 */

		}

		return f;

	}

	boolean compareDhRh(List<Row> lol, ResultSet rs) throws SQLException {

		int[] col = { 1, 2, 3 };
		boolean f = true;

		for (Row r : lol) {
			System.out.println("-----" + r.get(0) + " " + r.get(1) + " " + r.get(2));
		}

		if (lol.size() != rs.getRow()) {
			return false;
		}

		for (Row r : lol) {

			if (!rs.getObject(col[0]).equals(lol.get(col[0] - 1))) {

				f = false;
				break;
			}
			if (!rs.getObject(col[1]).equals(lol.get(col[1] - 1))) {
				f = false;
				break;
			}

			if (!rs.getObject(col[2]).equals(lol.get(col[2] - 1))) {
				f = false;
				break;
			}

		}
		return f;

	}
	
	boolean compareTwoListsOfRow(List<Row> r1, List<Row> r2) throws SQLException {
		
		boolean f = true;

		
		if (r1.size() != r2.size()) {
			return false;
		}

	
		for(int i=0; i<r1.size();i++){
			
			
			  for (int j = 0; j < r1.get(i).length(); j++) {
				  System.out.println(r1.get(i).get(j)  +"   ----  "+ r2.get(i).get(j));
				  if(!r1.get(i).get(j).equals(r2.get(i).get(j))){
					  f = false;
						break;
				  }
				  
			  }
			
			/*if(r1.get(i).get(0)==null||r1.get(i).get(1)==null||r2.get(i).get(0)==null||r2.get(i).get(1)==null){
				System.out.println("Some value is NULL NULL NULL");
				f = false;
				break;
				
			}
			
			System.out.println(r1.get(i).get(0)+" NNN "+r1.get(i).get(1));
			System.out.println(r2.get(i).get(0)+" NNN "+r2.get(i).get(1));
			
			if(!r1.get(i).get(0).equals(r2.get(i).get(0))){
				f = false;
				break;
			}
			if(!r1.get(i).get(1).equals(r2.get(i).get(1))){
				f = false;
				break;
			}*/
		}
		return f;

	}

}
