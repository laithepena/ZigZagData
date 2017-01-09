package audit;


	
	import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

	public class AdrouterData extends Utilities implements DataStrategyInterface  {
		
		

		@Override
		public DataFrame funcAuditParsing(JavaRDD<?> lines,  List<Tuple2<String, String>> listPairRDD ,SQLContext sqlContext, Properties colProp) {
	        System.out.println("............. getRowsFromAdrAudits...........");
	        
	        final int rows=33; // ################### no of rows
	        
	      //  JavaRDD<Row> l2 = lines.flatMap(s->ToListOfRows.AdrToListOfRows(s, listPairRDD,colProp));
	        
	        JavaRDD<AdrCols> l2 = lines.flatMap(s->ToListOfRows.AdrToListOfRows(s, listPairRDD,colProp));
	    //############################## FLAT MAP ENDS############################################################################################   
	        
	        System.out.println("L2  "+l2);
	        l2.collect();
	        System.out.println("L2 -- COUNT --- "+l2.count());
	        
	        JavaRDD<AdrCols> jj=l2;
	        
	      
	        
	        System.out.println(".........inside....getDataFrameFromAdrouterRows.......");
	    
	        DataFrame df1 = sqlContext.createDataFrame(jj, AdrCols.class); // DATAFRAME Generated ###################################
	    
		     
		 return df1;
	    }

		
		

	}