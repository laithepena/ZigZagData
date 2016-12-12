package audit;

import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class FeedbackData extends Utilities implements DataStrategyInterface {

	@Override
	public DataFrame funcAuditParsing(JavaRDD<?> rdd,SQLContext sqlContext, Properties colProp) {
		// TODO Auto-generated method stub
		return null;
	}

	/*@Override
	public DataFrame funcAuditTable(JavaRDD<Row> rowRDD, SQLContext sqlContext, Properties colProp) {
		// TODO Auto-generated method stub
		return null;
	}*/
	
	

}
