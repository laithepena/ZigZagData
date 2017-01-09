package audit;

import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public interface DataStrategyInterface {
	
    public  DataFrame funcAuditParsing(JavaRDD<?> rdd,List<Tuple2<String,String>> listPairRDD, SQLContext sqlContext, Properties colProp);
	
    //public DataFrame funcAuditTable(JavaRDD<Row> rowRDD,SQLContext sqlContext,Properties colProp );
		
}
