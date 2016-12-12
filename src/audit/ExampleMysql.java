package audit;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class ExampleMysql {
	
	
public static void main(String[] args) {
	
	
	Properties dbProperties = new Properties();
	try {
		dbProperties.load(new FileInputStream(new File("db-properties.flat")));
	}
	catch (FileNotFoundException e) {
		e.printStackTrace();
	}
	catch (IOException e) {
		e.printStackTrace();
	}
	
	
	int[] arr={10,20,30,40,50};
	
	List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
	SparkConf conf = new SparkConf().setAppName("PompuApp").setMaster("local");
	
	  JavaSparkContext sc = new JavaSparkContext(conf);
	
	JavaRDD<Integer> distData = sc.parallelize(data);
	
	
	//dbProperties
	
	
}
}
