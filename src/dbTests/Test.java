package dbTests;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.codehaus.jackson.JsonNode;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mysql.jdbc.Statement;

import conn.MySqlConnect;

public class Test {
	static String sql1 = "SELECT file_batch_id,file_name,status FROM file_load_control";
	static String sql2 = "SELECT file_batch_id,file_name,status FROM file_load_control order by  file_batch_id";
	
	public static void main(String[] args) throws SQLException, JSONException {
		MySqlConnect m1=new MySqlConnect();
		
		java.sql.Statement stmt = m1.connect().createStatement();
		//ResultSet rs = m1.connect().prepareStatement(sql1).executeQuery();
		ResultSet rs = stmt.executeQuery(sql1);
		
		
		
		MySqlConnect m2=new MySqlConnect();
		java.sql.Statement stmt2 = m2.connect().createStatement();
		ResultSet rs11 = stmt2.executeQuery(sql2);
		 
		int[] col={1,2,3};			
		while(rs.next()&&rs11.next()){
			
			System.out.print(rs.getObject(col[0]));   System.out.print("  "+rs11.getObject(col[0]));
			System.out.print(" "+rs.getObject(col[1]));   System.out.print("  "+rs11.getObject(col[1]));
			System.out.print(" "+rs.getObject(col[2]));   System.out.println("  "+rs11.getObject(col[2]));
			
			if(!rs.getObject(col[0]).equals(rs11.getObject(col[0])) ){
				System.out.println("false");
				break;
			}
		    
			if(!rs.getObject(col[1]).equals(rs11.getObject(col[1])) ){
				System.out.println("false");
				break;
			}
			
			if(!rs.getObject(col[2]).equals(rs11.getObject(col[2])) ){
				System.out.println("false");
				break;
			}
		}
		
		m1.disconnect();
		 m2.disconnect();
	}

}
