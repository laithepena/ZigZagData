package audit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FeedbackData extends Utilities implements DataStrategyInterface {

	@Override
	public DataFrame funcAuditParsing(JavaRDD<?> feeds,SQLContext sqlContext, Properties colProp) {
		// TODO Auto-generated method stub
		 System.out.println("............. getRowsFromFeedBack...........");
		 
		 JavaRDD l3 = feeds.flatMap(s -> {
	            Object[] rowArray = new Object[7];
	            Row aRow = RowFactory.create((Object[])rowArray);
	            String[] arr = ((String) s).split(",");  // First Split of line 
			 ArrayList<Row> lRow = new ArrayList<Row>();
			 
			 HashMap<String, String> rowMap2 = new HashMap<String, String>();
	            String[] pair = arr[0].split(":");
	            Utilities u = new Utilities();
	            String ret = u.getLocalDateID(pair[1], "America/New_York");
	            rowMap2.put("PSN_LOGTIME_PST", ret);
	            rowArray[0] = rowMap2.get(colProp.getProperty("feed_col1"));
	            int k = 1;
	            while (k < arr.length) {
	                rowMap2.put("PSN_ASSETID_Cr", arr[1]);
	                rowMap2.put("PSN_DURATION", arr[6]);
	                rowMap2.put("PSN_EVENT", arr[2]);
	                String ret1 = u.getLocalDateID(arr[3], "America/New_York");
	                rowMap2.put("PSN_EVENTTIME_EST", ret1);
	                rowMap2.put("PSN_NPT", arr[4]);
	                rowMap2.put("PSN_SCALE", arr[5]);
	                rowArray[1] = rowMap2.get(colProp.getProperty("feed_col2"));
	                rowArray[2] = rowMap2.get(colProp.getProperty("feed_col3"));
	                rowArray[3] = rowMap2.get(colProp.getProperty("feed_col4"));
	                rowArray[4] = rowMap2.get(colProp.getProperty("feed_col5"));
	                rowArray[5] = rowMap2.get(colProp.getProperty("feed_col6"));
	                rowArray[6] = rowMap2.get(colProp.getProperty("feed_col7"));
	                ++k;
	            }
	            lRow.add(aRow);
	            System.out.println("pom pom " + lRow);
	            		 
			 
			 
	            
			 return lRow; 
	        }
	        );
		 
		 //#################### Flat Map Ends ####################################################################//
		 l3.collect();
		 JavaRDD<Row> jj=l3;
		 
		 System.out.println(".......inside.....getDataFrameFromFeedbackRows");
	        ArrayList fields = new ArrayList();
	        ArrayList<StructField> fields2 = new ArrayList<StructField>();
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col1"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col2"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col3"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col4"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col5"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col6"), (DataType)DataTypes.StringType, (boolean)true));
	        fields2.add(DataTypes.createStructField((String)colProp.getProperty("feed_col7"), (DataType)DataTypes.StringType, (boolean)true));
	        StructType schema = DataTypes.createStructType(fields2);
	        DataFrame df1 = sqlContext.createDataFrame(jj, schema);
	        return df1;
		 
		 
		 
	}

	/*@Override
	public DataFrame funcAuditTable(JavaRDD<Row> rowRDD, SQLContext sqlContext, Properties colProp) {
		// TODO Auto-generated method stub
		return null;
	}*/
	
	

}
