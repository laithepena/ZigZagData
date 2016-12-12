package audit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.apache.derby.impl.sql.catalog.SYSROUTINEPERMSRowFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class AdrouterData extends Utilities implements DataStrategyInterface  {

	@Override
	public DataFrame funcAuditParsing(JavaRDD<?> lines,SQLContext sqlContext, Properties colProp) {
        System.out.println("............. getRowsFromAdrAudits...........");
        
        
        
        JavaRDD l2 = lines.flatMap(s -> {
            HashMap<Integer, String> OneRowMap = new HashMap<Integer, String>();
            HashMap<Integer, String> hMap = new HashMap<Integer, String>();
            HashMap<Integer, String> rowMap = new HashMap<Integer, String>();
            ArrayList<Row> lRow = new ArrayList<Row>();
            
            ArrayList<DataFrame> lFrame = new ArrayList<DataFrame>();
            
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
                Object[] rowArray = new Object[23];
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
                    HashMap aCreat = new HashMap();
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
                    rowArray[0] = rowMap2.get(colProp.getProperty("adr_col1"));
                    rowArray[1] = rowMap2.get(colProp.getProperty("adr_col2"));
                    rowArray[2] = rowMap2.get(colProp.getProperty("adr_col3"));
                    rowArray[3] = rowMap2.get(colProp.getProperty("adr_col4"));
                    rowArray[4] = rowMap2.get(colProp.getProperty("adr_col5"));
                    rowArray[5] = rowMap2.get(colProp.getProperty("adr_col6"));
                    rowArray[6] = rowMap2.get(colProp.getProperty("adr_col7"));
                    rowArray[7] = rowMap2.get(colProp.getProperty("adr_col8"));
                    rowArray[8] = rowMap2.get(colProp.getProperty("adr_col9"));
                    rowArray[9] = rowMap2.get(colProp.getProperty("adr_col10"));
                    rowArray[10] = rowMap2.get(colProp.getProperty("adr_col11"));
                    rowArray[11] = rowMap2.get(colProp.getProperty("adr_col12"));
                    rowArray[12] = rowMap2.get(colProp.getProperty("adr_col13"));
                    rowArray[13] = rowMap2.get(colProp.getProperty("adr_col14"));
                    rowArray[14] = rowMap2.get(colProp.getProperty("adr_col15"));
                    rowArray[15] = rowMap2.get(colProp.getProperty("adr_col16"));
                    rowArray[16] = rowMap2.get(colProp.getProperty("adr_col17"));
                    rowArray[17] = rowMap2.get(colProp.getProperty("adr_col18"));
                    rowArray[18] = rowMap2.get(colProp.getProperty("adr_col19"));
                    rowArray[19] = rowMap2.get(colProp.getProperty("adr_col20"));
                    rowArray[20] = rowMap2.get(colProp.getProperty("adr_col21"));
                    rowArray[21] = rowMap2.get(colProp.getProperty("adr_col22"));
                    rowArray[22] = rowMap2.get(colProp.getProperty("adr_col23"));
                    long l = System.currentTimeMillis();
                    String ss = Long.toString(l);
                    System.out.println("POMPU 007 " + ss);
                    Utilities u1 = new Utilities();
                    String dateUpdated_pst = u1.getLocalDateID_All(ss, "America/Los_Angeles");
                    rowArray[22] = dateUpdated_pst;
                    ++last;
                }
                Row aRow = RowFactory.create((Object[])rowArray);
                lRow.add(aRow);
                ++lcount;
            }
            
           // System.out.println("ASSAM --- "+lRow);
            return lRow; 
            
            
        }
        );
    //############################## FLAT MAP ENDS############################################################################################   
        
        System.out.println("L2  "+l2);
        l2.collect();
        System.out.println("L2 -- COUNT --- "+l2.count());
        
        JavaRDD<Row> jj=l2;
        
       
        
        //System.out.println("JJ COLLECT   "+jj.collect());
        
       // System.out.println("JJ COUNT  "+jj.count());
        
        //DataFrame df1;
       /* System.out.println("COUNT "+l2.count());
        return l2;*/
        System.out.println(".........inside....getDataFrameFromAdrouterRows.......");
        ArrayList<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col1"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col2"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col3"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col4"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col5"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col6"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col7"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col8"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col9"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col10"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col11"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col12"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col13"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col14"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col15"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col16"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col17"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col18"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col19"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col20"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col21"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col22"), (DataType)DataTypes.StringType, (boolean)true));
        fields.add(DataTypes.createStructField((String)colProp.getProperty("adr_col23"), (DataType)DataTypes.StringType, (boolean)true));
        StructType schema = DataTypes.createStructType(fields);
        DataFrame df1 = sqlContext.createDataFrame(jj, schema);
        
      // df1.show();
	     
	 return df1;
    }

	/*@Override
	public DataFrame funcAuditTable(JavaRDD<Row> rowRDD, SQLContext sqlContext, Properties colProp) {
        return null;
    }
	*/
	

}
