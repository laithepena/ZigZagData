package audit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

import example.datahubRow;
import scala.Tuple2;

public class FeedbackData extends Utilities implements DataStrategyInterface {

	@Override
	
//	JavaRDD<Adrcols> l2 = lines.flatMap(s->ToListOfRowsNew.AdrToListOfRowsNew(s, listPairRDD,colProp));
	
	
	public DataFrame funcAuditParsing(JavaRDD<?> feeds,List<Tuple2<String, String>> listPairRDD,SQLContext sqlContext, Properties colProp) {
		// TODO Auto-generated method stub
		 System.out.println("............. getRowsFromFeedBack...........");
		 final int rows=8; // ################### no of rows
		 
		 
		 JavaRDD<FeedCols> l3 = feeds.flatMap(s->ToListOfRows.FeedToListOfRows(s,listPairRDD,colProp));
		
		 l3.collect();
		 
		/* JavaRDD<FeedCols> l3 =feeds.map(s->{
				
			 FeedCols d1=new FeedCols(); 
				d1.setLocal_date_id(s.get(0).toString());
				d1.setMet(s.getLong(1));
			 
			 d1.setLast_Updated_pst("");
				return d1;
			});*/
		 
		 
		 
		 JavaRDD<FeedCols> jj=l3;
		 
	        DataFrame df1 = sqlContext.createDataFrame(jj, FeedCols.class);
	        df1.show(1000, false);
	        return df1;
		 
		 
		 
	}
	

}
