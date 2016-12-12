package audit;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class DataContext {
	//private String filename;
	private DataStrategyInterface  dataStrategy;
	private JavaRDD rdd; 
	private Properties colProp; 
	private JavaRDD<Row> R1;
	private DataFrame DF1;
	private SQLContext sqlContext;
	private String jdbcUrl;
	private FileInputStream input;
	private Properties dbProperties;
	
	private String dbUser;
	private String dbPassword;
	//SQLContext sqlContext;

	DataContext(DataStrategyInterface  dataStrategy){
		this.dataStrategy=dataStrategy;
	}

	void setDataConfig(JavaSparkContext sc, String AuditType){

		colProp = new Properties();
		input = null;
		try {
			input = new FileInputStream("columnConfig.properties");
			colProp.load(input);
		}
		catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(colProp.getProperty("adr_col1"));
		System.out.println(colProp.getProperty("adr_col12"));
		 dbProperties = new Properties();
		try {
			dbProperties.load(new FileInputStream(new File("db-properties.flat")));
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		

		rdd = sc.textFile(dbProperties.getProperty(AuditType), 10);
		
		jdbcUrl = dbProperties.getProperty("jdbcUrl");
		/*dbUser= dbProperties.getProperty(dbUser);
		dbPassword=dbProperties.getProperty(dbPassword);*/
		
        System.out.println(dbProperties.getProperty(jdbcUrl));
        System.out.println(dbProperties.size());
		
	}

	void executeDataStrategy(JavaSparkContext sc,String tableName){
		
	    sqlContext = new SQLContext(sc);
		DF1=dataStrategy.funcAuditParsing(rdd,sqlContext,colProp );
		
		System.out.println("POMPU --- XYZ ");
				
		DF1.show(1000, false);
	
		
	
		//System.out.println("DF1  COUNT ---- "+DF1.count());
		DF1.write().mode(SaveMode.Overwrite).jdbc(jdbcUrl, tableName,dbProperties);
		// jdbc(jdbcUrl, "stg_twc_adr", dbProperties);
		//DF1.write().mode(SaveMode.Overwrite).
		
		DF1.count();
		
	}

	public DataStrategyInterface getDataStrategy() {
		return dataStrategy;
	}

	public void setDataStrategy(DataStrategyInterface dataStrategy) {
		this.dataStrategy = dataStrategy;
	}

}
