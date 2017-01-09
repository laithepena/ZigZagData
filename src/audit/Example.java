package audit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class Example {

	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("PompuApp").setMaster("local"); // this is for local mode when running from eclipse
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaPairRDD<String, String> rdd=  sc.wholeTextFiles("hdfs://qa-us1-dhub20.blackarrow-corp.com:8020//user/datahub/profiles/qa17/in/audit_files/processed/20161121220420390/20001_*_*_ba_audit_*.log.gz");
       System.out.println(rdd.keys().count());
       
      JavaRDD<String> rdd2= sc.textFile("hdfs://qa-us1-dhub20.blackarrow-corp.com:8020//user/datahub/profiles/qa17/in/audit_files/processed/20161121220420390/20001_*_*_ba_audit_*.log.gz");
       
      //for(int i=0;i<rdd.keys().count();i++){
    	  
    	  System.out.println(rdd.keys()+  "  POMPU  " + rdd.values());
    	  
    	 // rdd.fi
    	  
    	  List<String> l1=rdd2.collect();
    	  
       	  List<Tuple2<String, String>> pairs = rdd.collect();
       	  
       
       	  
       	JavaRDD<Row> l2 = rdd.flatMap(f->{
       		
       		
       		  f._2.split(",");
       		 ArrayList<Row> lRow = new ArrayList<Row>();
       		 
       		//RowFactory.create(values)
       		 
       		 return lRow;
       	  });
    	  
    	  System.out.println(pairs.size());
    	  
    	//  System.out.println
    	  for(int i=0;i<pairs.size();i++){
    	  if(pairs.get(i)._2.contains("014Pdd3HYkTjGzyEp-AFyP"))
    	  {
    		  System.out.println(pairs.get(i)._1.split("/"));
    		  String arr[]=pairs.get(i)._1.split("/");
    		  
    		  System.out.println(" ANSWER  "+arr[11]);
    
    	  }
    	  //System.out.println(pairs.get(1)._2.contains("014Pdd3HYkTjGzyEp-AFyP"));
    	  
       
    	 // List<List<String, String>> pairs = rdd.collect();
        
	}
}}
