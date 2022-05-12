package pyspark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PostgreSQLJDBC {
	static List<HashMap<String, String>> listofMaps = new ArrayList<HashMap<String, String>>();
	
	public List<HashMap<String, String>> fetch() {
		Connection c = null;
		Statement stmt = null;
		
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/jdbc",
					"postgres", "admin");
			c.setAutoCommit(false);
			System.out.println("opened database successfully");
			
			stmt = c.createStatement();
			ResultSet rs = stmt.executeQuery( "SELECT * FROM public.employees_premdm;" );
			while( rs.next() ) {
//				putting each in map
				HashMap<String, String> empMap = new HashMap<>();
				empMap.put("empid",rs.getString("empid"));
				empMap.put("empname",rs.getString("empname"));
				empMap.put("change_code",rs.getString("change_code"));
				
				listofMaps.add(empMap);
			}
			
			rs.close();
			stmt.close();
			c.close();
		}
		catch ( Exception e) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage());
			System.exit(0);
		}
		System.out.println(listofMaps.size());
		System.out.println("operation done successfully");
		
		return listofMaps;
	}
}
		
