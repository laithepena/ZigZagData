package config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Config {
	  private   Properties dbProp;
	  private FileInputStream input;
	  

	  
	  public Properties getDbProp() {
		return dbProp;
	}

	public void setDbProp(Properties dbProp) {
		this.dbProp = dbProp;
	}

	public Config(){
		  dbProp=new Properties();
		  input=null;
		  try {
				input = new FileInputStream("resources/db-properties.flat");
				dbProp.load(input);
			}
			catch (FileNotFoundException e1) {
				e1.printStackTrace();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		 
	  }
	   
	  public  String getProperty(String key) {
	    return dbProp.getProperty(key);
	  }
	  
	  public void setProperty(String key,String value){
		  dbProp.setProperty(key, value);
		  
	  }
	}