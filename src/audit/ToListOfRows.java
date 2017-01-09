package audit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.Tuple2;

public class ToListOfRows {
	

	public static  List<FeedCols> FeedToListOfRows(Object s,List<Tuple2<String, String>> listPairRDD,Properties colProp) {
		
		//JavaRDD<Row> l3 = feeds.flatMap(s -> {/*
		
		List<FeedCols> ListOfFeedCols=new ArrayList<>();
		
       // Object[] rowArray = new Object[rows];
      //  Row aRow = RowFactory.create((Object[])rowArray);
        String[] arr = ((String) s).split(",");  // First Split of line 
	 ArrayList<Row> lRow = new ArrayList<Row>();
	 
	 HashMap<String, String> rowMap2 = new HashMap<String, String>();
	 Utilities u = new Utilities();
      //  rowArray[0] = rowMap2.get(colProp.getProperty("feed_col1"));
	 
	 FeedCols fc1=new FeedCols();
        int k = 1;
        while (k < arr.length) {
        	
        	
        	
        	 String[] pair = arr[0].split(":");
            
             String ret = u.getLocalDateID(pair[1], "America/New_York");
             rowMap2.put("PSN_LOGTIME_PST", ret);
        	
            rowMap2.put("PSN_ASSETID_Cr", arr[1]);
            rowMap2.put("PSN_DURATION", arr[6]);
            
            
            rowMap2.put("PSN_EVENT", arr[2]);
            
            String ret1 = u.getLocalDateID(arr[3], "America/New_York");
            rowMap2.put("PSN_EVENTTIME_EST", ret1);
            rowMap2.put("PSN_NPT", arr[4]);
            rowMap2.put("PSN_SCALE", arr[5]);
            /*rowArray[1] = rowMap2.get(colProp.getProperty("feed_col2"));
            rowArray[2] = rowMap2.get(colProp.getProperty("feed_col3"));
            rowArray[3] = rowMap2.get(colProp.getProperty("feed_col4"));
            rowArray[4] = rowMap2.get(colProp.getProperty("feed_col5"));
            rowArray[5] = rowMap2.get(colProp.getProperty("feed_col6"));
            rowArray[6] = rowMap2.get(colProp.getProperty("feed_col7"));*/
            
            //  a1.setRTASSETID(rowMap2.get(colProp.getProperty("adr_col1")));
            
            fc1.setPSN_LOGTIME_PST(rowMap2.get(colProp.getProperty("feed_col1")));
            fc1.setPSN_ASSETID_Cr(rowMap2.get(colProp.getProperty("feed_col2")));
            fc1.setPSN_DURATION(rowMap2.get(colProp.getProperty("feed_col3")));
            fc1.setPSN_EVENT(rowMap2.get(colProp.getProperty("feed_col4")));
            fc1.setPSN_EVENTTIME_EST(rowMap2.get(colProp.getProperty("feed_col5")));
            
           // fc1.setPSN_NPT(rowMap2.get(colProp.getProperty("feed_col6")));
          //  fc1.setPSN_SCALE(rowMap2.get(colProp.getProperty("feed_col7")));
            
            fc1.setPSN_NPT(Double.parseDouble(rowMap2.get(colProp.getProperty("feed_col6"))));
            fc1.setPSN_SCALE(Double.parseDouble(rowMap2.get(colProp.getProperty("feed_col7"))));
            
           // fc1.setLast_Updated_pst(colProp.getProperty("feed_col8"));
            
            long l = System.currentTimeMillis();
            String ss1 = Long.toString(l);
            System.out.println("POMPU 007 " + ss1);
            Utilities u2 = new Utilities();
            String dateUpdated_pst = u2.getLocalDateID_All(ss1, "America/Los_Angeles");
            //rowArray[7] = dateUpdated_pst;
            fc1.setLast_Updated_pst(dateUpdated_pst);
            k++;
            
          
        }
       // lRow.add(aRow);
        System.out.println("pom pom " + lRow);
        		 
        ListOfFeedCols.add(fc1);
	 return ListOfFeedCols;
        
	 //return lRow;
		//return null;
		
	}
	
	
	//---------------------------------------Adrouter from Below---------------------------------------------//
	   
	//final static int rows=33;
	public static  List<AdrCols> AdrToListOfRows(Object s,List<Tuple2<String, String>> listPairRDD, Properties colProp) {

        HashMap<Integer, String> OneRowMap = new HashMap<Integer, String>();
        HashMap<Integer, String> hMap = new HashMap<Integer, String>();
        HashMap<Integer, String> rowMap = new HashMap<Integer, String>();
       ArrayList<Row> lRow = new ArrayList<Row>();
       
       List<AdrCols> ListOfAdrCols=new ArrayList<>();
        
        String[] arr1 = ((String) s).split("\\^");
        int count = 0;
        while (count < arr1.length) {
            hMap.put(count, arr1[count]);
            System.out.println(" after ^  " + (String)hMap.get(count));
            ++count;
        }
        System.out.println(" SIZE ... ... " + hMap.size());
        if (hMap.size() == 1) {
            System.out.println();
            hMap.put(1, "#ACCOUNT DUMMY_VALUE#ADCOUNT DUMMY_VALUE");
        }
        int com = 0;
        while (com < hMap.size() - 1) {
            rowMap.put(com, String.valueOf((String)hMap.get(0)) + "#" + (String)hMap.get(com + 1));
            ++com;
        }
        int one = 0;
        int rr = 0;
        while (rr < rowMap.size()) {
            String[] temp = ((String)rowMap.get(rr)).split("#");
            int inner = 0;
            while (inner < temp.length) {
                if (temp[inner].equals(null) || temp[inner].equals("") || temp[inner].equals("*") || temp[inner].isEmpty()) {
                    temp[inner] = "DUMMY VALUE";
                }
                ++inner;
            }
            int check = 0;
            while (check < temp.length) {
                System.out.println(" this is temp  ----  " + temp[check]);
                ++check;
            }
            String result = "";
            String res1 = "";
            int pr = 0;
            while (pr < temp.length) {
                if (!temp[pr].matches("^ADCOUNT.*")) {
                    result = String.valueOf(result) + "," + temp[pr];
                } else {
                    res1 = String.valueOf(result) + "," + temp[pr];
                }
                if (!res1.isEmpty()) {
                    OneRowMap.put(one, res1);
                    ++one;
                }
                ++pr;
            }
            ++rr;
        }
        OneRowMap.forEach((k, v) -> {
            System.out.println(" CommonPart+ADCOUNTS together -- OneRowMap   " + k + " " + v);
        }
        );
        String[] rpair = null;
        int lcount = 0;
        while (lcount < OneRowMap.size()) {
          
        	AdrCols a1 =new AdrCols();
           
            
            
            HashMap<String, String> rowMap2 = new HashMap<String, String>();
            System.out.println(" ====================  CATCH ========rpair =====  " + OneRowMap);
            rpair = ((String)OneRowMap.get(lcount)).split(",");
            String[] val = new String[]{"one", "two"};
            System.out.println(" ====================  CATCH ========rpair =====  " + rpair);
            int last = 2;
            while (last < rpair.length) {
                String ret;
                Utilities u;
                System.out.println(" ====================  CATCH ========rpair[last] =====  " + rpair[last]);
                val = rpair[last].split(" ");
                if (val.length == 1) {
                    val[0] = String.valueOf(val[0]) + " DUMMYVALUE";
                    val = val[0].split(" ");
                }
                rowMap2.put(val[0], val[1]);
                String[] arr11 = null;
                if (val[0].equals("EVENTTIME")) {
                    u = new Utilities();
                    ret = u.getLocalDateID(val[1], "America/New_York");
                    rowMap2.put("EVENTTIME_EST", ret);
                }
                if (val[0].equals("LOGTIME")) {
                    u = new Utilities();
                    ret = u.getLocalDateID(val[1], "America/Los_Angeles");
                    rowMap2.put("LOGTIME_PST", ret);
                }
                if (val[0].equals("ASSETINFO")) {
                    arr11 = val[1].split(":");
                    int j1 = 0;
                    while (j1 < arr11.length) {
                        if (arr11[j1].equals("ASSETID")) {
                            rowMap2.put("ASSETID_Cr", arr11[j1 + 1]);
                        }
                        if (arr11[j1].equals("DATABASEID")) {
                            rowMap2.put("DATABASEID_Cr", arr11[j1 + 1]);
                        }
                        if (arr11[j1].equals("NAME")) {
                            rowMap2.put("NAME_Cr", arr11[j1 + 1]);
                        }
                        ++j1;
                    }
                }
                
                
                if(val[0].equals("PROGRAMPOSITION")){
                	if(val[1].equals("37")){
                		rowMap2.put("PROGRAMPOSITION", "Pre");
                	}
                	else if(val[1].equals("38")){
                		rowMap2.put("PROGRAMPOSITION", "Mid");
                	}
                	else if(val[1].equals("39")){
                		rowMap2.put("PROGRAMPOSITION", "Post");
                	}
                	
                }
                
                if(val[0].equals("PRMSGREF")){
                for(int i=0;i<listPairRDD.size();i++){
              	  if(listPairRDD.get(i)._2.contains(val[1]))
              	  {
              		  //System.out.println(listPairRDD.get(i)._1);
              		  
              		//listPairRDD.get(i)._1;
              		String arr[]=listPairRDD.get(i)._1.split("/");
              		rowMap2.put("LOG_FILE_NAME",arr[11]);
              	  }
              	  }
                }
                
                a1.setRTASSETID(rowMap2.get(colProp.getProperty("adr_col1")));
                a1.setSUBSCRIBERREF(rowMap2.get(colProp.getProperty("adr_col2")));
                a1.setMSGTRCKID(rowMap2.get(colProp.getProperty("adr_col3")));
                a1.setEVENTTIME_EST(rowMap2.get(colProp.getProperty("adr_col4")));
                a1.setLOGTIME_PST(rowMap2.get(colProp.getProperty("adr_col5")));
                a1.setPOISAVAILTRACKING(rowMap2.get(colProp.getProperty("adr_col6")));
                a1.setNETWORK(rowMap2.get(colProp.getProperty("adr_col7")));
                a1.setDISTRIBUTOR(rowMap2.get(colProp.getProperty("adr_col8")));
                a1.setCLID(rowMap2.get(colProp.getProperty("adr_col9")));
                
                a1.setASSETID_Cr(rowMap2.get(colProp.getProperty("adr_col10")));
                a1.setDATABASEID_Cr(rowMap2.get(colProp.getProperty("adr_col11")));
                a1.setNAME_Cr(rowMap2.get(colProp.getProperty("adr_col12")));
                a1.setBALIBID(rowMap2.get(colProp.getProperty("adr_col13")));
                a1.setACCOUNTID(rowMap2.get(colProp.getProperty("adr_col14")));
                a1.setACCOUNT(rowMap2.get(colProp.getProperty("adr_col15")));
                a1.setINVENTORYSRCACCOUNT(rowMap2.get(colProp.getProperty("adr_col16")));
                a1.setINVENTORYSRCENABLED(rowMap2.get(colProp.getProperty("adr_col17")));
                a1.setADCOUNT(rowMap2.get(colProp.getProperty("adr_col18")));
                a1.setADMAPID(rowMap2.get(colProp.getProperty("adr_col19")));
                a1.setPOISMSGTRACKING(rowMap2.get(colProp.getProperty("adr_col20")));
                a1.setPRMSGREF(rowMap2.get(colProp.getProperty("adr_col21")));
                a1.setTYPE(rowMap2.get(colProp.getProperty("adr_col22")));
                
                a1.setPLACEMENTID(rowMap2.get(colProp.getProperty("adr_col23")));
                a1.setPROGRAMPOSITION(rowMap2.get(colProp.getProperty("adr_col24")));
                a1.setADPOSITION(rowMap2.get(colProp.getProperty("adr_col25")));
                a1.setADMAPADUNITID(rowMap2.get(colProp.getProperty("adr_col26")));
                a1.setADUNITID(rowMap2.get(colProp.getProperty("adr_col27")));
                a1.setADSIDENTITY(rowMap2.get(colProp.getProperty("adr_col28")));
                a1.setACTION(rowMap2.get(colProp.getProperty("adr_col29")));
                
                a1.setRTPROVIDERID(rowMap2.get(colProp.getProperty("adr_col30")));
                a1.setADMID(rowMap2.get(colProp.getProperty("adr_col31")));
                a1.setLOG_FILE_NAME(rowMap2.get(colProp.getProperty("adr_col32")));
               
               
                
                long l = System.currentTimeMillis();
                String ss = Long.toString(l);
                System.out.println("POMPU 007 " + ss);
                Utilities u1 = new Utilities();
                String dateUpdated_pst = u1.getLocalDateID_All(ss, "America/Los_Angeles");
                //rowArray[32] = dateUpdated_pst;
                a1.setLast_Updated_pst(dateUpdated_pst);
                last++;
            } 
            // while loop ends
           
            
            ListOfAdrCols.add(a1);
        
            lcount++;
        }
        
      
        return ListOfAdrCols; 
        
        
    
	}
	


}
