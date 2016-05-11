package postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import loader.JSONlogger;

public class DBconnector {
	
	private static Connection conn=null;
	private JSONlogger jsonLogger=null;
	
	public DBconnector(JSONlogger jsonLogger,String dbpath, String user, String password) throws ClassNotFoundException, SQLException{	
		conn=openDBConnection(dbpath,user,password);
		this.jsonLogger=jsonLogger;
	}
	
	public JSONlogger getLog(){
		return this.jsonLogger;
	}
	
	private static Connection openDBConnection(String dbpath, String user, String password) throws ClassNotFoundException, SQLException{
		System.out.println("Opening database connection...");
		Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://"+dbpath;
        Properties props = new Properties();
        props.setProperty("user",user);
        props.setProperty("password",password);
        Connection conn = DriverManager.getConnection(url, props);
        //conn.setAutoCommit(false);
        System.out.println("Connection established!");
		return conn;		
	}
	
	public static void closeDBConnection(Connection conn) throws SQLException{
		System.out.println("Commiting changes...");
		conn.commit();
		System.out.println("Closing postgresql connection...");		
		conn.close();
	}
	
	public long executeQueryReturnLong(String query) throws SQLException{
		PreparedStatement st = conn.prepareStatement(query);
		ResultSet rs = st.executeQuery();
		rs.next();
		long result = rs.getLong(1);
		rs.close();
		st.close();
		return result;	
	}
	
	public long executeQueryReturnLong(PreparedStatement st) throws SQLException{		
		ResultSet rs = st.executeQuery();
		rs.next();		
		long result = rs.getLong(1);		
		rs.close();
		st.close();
		return result;	
	}
	
	public boolean executeQueryReturnBoolean(PreparedStatement st) throws SQLException{		
		ResultSet rs = st.executeQuery();
		rs.next();		
		boolean result = rs.getBoolean(1);		
		rs.close();
		st.close();
		return result;	
	}
	
	public String executeQueryReturnString(String query) throws SQLException{
		PreparedStatement st = conn.prepareStatement(query);
		ResultSet rs = st.executeQuery();
		rs.next();
		String result;
		try{
			result = rs.getString(1);
		} catch (Exception e){
			return null;
		}
		rs.close();
		st.close();
		return result;	
	}
	
	public void executeQuery(String query) throws SQLException{
		PreparedStatement st = conn.prepareStatement(query);
		st.executeQuery();
		st.close();			
	}
	
	public long executeUpdate(String query) throws SQLException{
		PreparedStatement st = conn.prepareStatement(query);
		int result = st.executeUpdate();
		st.close();
		return result;	
	}
	
	public long executeUpdate(PreparedStatement st) throws SQLException{	
		int result = st.executeUpdate();
		st.close();		
		return result;	
	}	
	
	public Connection getConnection(){
		return conn;
	}
	
	
}
