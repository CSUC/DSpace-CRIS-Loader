package loader;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import postgresql.DBconnector;
import postgresql.DButils;

public class Loader {
	
	static Logger log = Logger.getLogger(Loader.class.getName());
	
	private JSONlogger jsonLogger;
	
	private DBconnector conn = null;
	
	private int numResUpdated = 0;
	private int numPrjUpdated = 0;
	private int numOrgUpdated = 0;
	private int numResUnitUpdated = 0;
	private int numResNonUpdated = 0;
	private int numPrjNonUpdated = 0;
	private int numOrgNonUpdated = 0;
	private int numResUnitNonUpdated = 0;
	private int numResAdded = 0;
	private int numPrjAdded = 0;
	private int numOrgAdded = 0;
	private int numResUnitAdded = 0;
	
	public Loader(String dbPath, String user, String password, String outputFile) throws Exception{		
		try {
			
			jsonLogger = new JSONlogger();
			
			if (outputFile != null){
				//all output goes to output.txt file
				System.setOut(new PrintStream(new FileOutputStream(outputFile)));
			}			
			
			conn=new DBconnector(jsonLogger,dbPath,user,password);

			System.out.println("Cleaning jdyna_prop_seq ... ");
			DButils.cleanCRIS_PROPid(conn);
			System.out.println("Cleaned!");
			System.out.println("Cleaning jdyna_values_seq ... ");
			DButils.cleanJDYNAid(conn);
			System.out.println("Cleaned!");
			
		} catch (ClassNotFoundException e1) {			
			e1.printStackTrace();
			throw new Exception("org.postgresql.Driver not found");
		} catch (SQLException e2){			
			e2.printStackTrace();
			throw e2;
			//throw new Exception("Problem opening the postgresql connection!");
		}
	}
	
	public Loader(String dbPath, String user, String password, String outputFile, String[][] resUpdateShortnames, String[][] prjUpdateShortnames, String[][] orgUpdateShortnames, String[][] dptUpdateShortnames) throws Exception{		
		try {
			
			jsonLogger = new JSONlogger();
			
			if (outputFile != null){
				//all output goes to output.txt file
				System.setOut(new PrintStream(new FileOutputStream(outputFile)));
			}			
			
			conn=new DBconnector(jsonLogger,dbPath,user,password);

			System.out.println("Cleaning jdyna_prop_seq ... ");
			DButils.cleanCRIS_PROPid(conn);
			System.out.println("Cleaned!");
			System.out.println("Cleaning jdyna_values_seq ... ");
			DButils.cleanJDYNAid(conn);
			System.out.println("Cleaned!");
			
		} catch (ClassNotFoundException e1) {			
			e1.printStackTrace();
			throw new Exception("org.postgresql.Driver not found");
		} catch (SQLException e2){			
			e2.printStackTrace();
			throw e2;
			//throw new Exception("Problem opening the postgresql connection!");
		}
	}
	
	public void addOrgUnit(ArrayList<Attribute> values, String sourceid, String sourceref) throws Exception{
		if (conn == null){
			throw new Exception("Connection to dspace database not found!");
		}
		else{
			try {
				DButils.addOrgUnit(conn, values,sourceid,sourceref);
				numOrgAdded++;
			} catch (SQLException e) {				
				e.printStackTrace();
				throw new Exception("Problem adding org unit");
			}	
		}
	}
	
	public void updateOrgUnit(ArrayList<Attribute> values, long ouid, String sourceid, String sourceref) throws Exception{
		if (conn == null){
			throw new Exception("Connection to dspace database not found!");
		}
		else{			
			try {				
				int attributesUpdated = DButils.updateOrgUnit(conn,values,ouid,sourceid,sourceref);
				if (attributesUpdated > 0) numOrgUpdated++;
				else numOrgNonUpdated++;
			} catch (SQLException e) {				
				e.printStackTrace();
				throw new Exception("Problem updating project");
			}			
		}			
	}
	
	public void addProject(ArrayList<Attribute> values, String sourceid, String sourceref) throws Exception{
		if (conn == null){
			throw new Exception("Connection to dspace database not found!");
		}
		else{			
			try {
				DButils.addProject(conn, values,sourceid,sourceref);
				numPrjAdded++;
			} catch (SQLException e) {				
				e.printStackTrace();
				throw new Exception("Problem adding project");
			}			
		}			
	}
	
	public void updateProject(ArrayList<Attribute> values, long pjid, String sourceid, String sourceref) throws Exception{
		if (conn == null){
			throw new Exception("Connection to dspace database not found!");
		}
		else{			
			try {				
				int attributesUpdated = DButils.updateProject(conn,values,pjid,sourceid,sourceref);
				if (attributesUpdated > 0) numPrjUpdated++;
				else numPrjNonUpdated++;
			} catch (SQLException e) {				
				e.printStackTrace();
				throw new Exception("Problem updating project");
			}			
		}			
	}
	
	public void addResearcher(ArrayList<Attribute> values, String sourceid, String sourceref) throws Exception{
		if (conn == null){
			throw new Exception("Connection to dspace database not found!");
		}
		else{			
			try {						
				DButils.addResearcher(conn, values,sourceid,sourceref);		
				numResAdded++;
			} catch (SQLException e) {				
				e.printStackTrace();
				throw new Exception("Problem adding researcher");
			}			
		}			
	}
	
	public void addResearcher(ArrayList<Attribute> values, int manualID, String sourceid, String sourceref) throws Exception{
		if (conn == null){
			throw new Exception("Connection to dspace database not found!");
		}
		else{			
			try {						
				DButils.addResearcher(conn,values,manualID,sourceid,sourceref);		
				numResAdded++;
			} catch (SQLException e) {				
				e.printStackTrace();
				throw new Exception("Problem adding researcher");
			}			
		}			
	}
	
	public void updateResearcher(ArrayList<Attribute> values, long rpid, String sourceid, String sourceref) throws Exception{
		if (conn == null){
			throw new Exception("Connection to dspace database not found!");
		}
		else{			
			try {				
				int attributesUpdated = DButils.updateResearcher(conn,values,rpid,sourceid,sourceref);	
				if (attributesUpdated > 0) numResUpdated++;
				else numResNonUpdated++;				
			} catch (SQLException e) {				
				e.printStackTrace();
				throw new Exception("Problem updating researcher");
			}			
		}			
	}
	
	public void addResearch(int typo_id, ArrayList<Attribute> values, String sourceid, String sourceref) throws Exception{
		if (conn == null){
			throw new Exception("Connection to dspace database not found!");
		}
		else{			
			try {
				DButils.addResearch(conn, values, typo_id,sourceid,sourceref);
				numResUnitAdded++;
			} catch (SQLException e) {				
				e.printStackTrace();
				throw new Exception("Problem adding research object");
			}			
		}			
	}
	
	public void updateResearch(ArrayList<Attribute> values, long doid, String sourceid, String sourceref) throws Exception{
		if (conn == null){
			throw new Exception("Connection to dspace database not found!");
		}
		else{			
			try {				
				int attributesUpdated = DButils.updateResearch(conn,values,doid,sourceid,sourceref);		
				if (attributesUpdated > 0) numResUnitUpdated++;
				else numResUnitNonUpdated++;				
			} catch (SQLException e) {				
				e.printStackTrace();
				throw new Exception("Problem updating researcher");
			}			
		}			
	} 
		
	public void close() throws Exception{
		if (conn == null){
			throw new Exception("Connection to dspace database not found!");
		}		
		else{
			System.out.println("Cleaning jdyna_values_seq ... ");
			DButils.cleanJDYNAid(conn);
			System.out.println("Cleaned!");			
			System.out.println("Cleaning jdyna_prop_seq ... ");
			DButils.cleanCRIS_PROPid(conn);			
			System.out.println("Cleaned!");
			System.out.println("---------------------------");
			System.out.println("New researchers added:"+numResAdded);
			System.out.println("New projects added:"+numPrjAdded);
			System.out.println("New org units added:"+numOrgAdded);
			System.out.println("New researcher units added:"+numResUnitAdded);
			System.out.println("---------------------------");
			System.out.println("Non updated researchers:"+numResNonUpdated);
			System.out.println("Non updated projects:"+numPrjNonUpdated);
			System.out.println("Non updated org units:"+numOrgNonUpdated);
			System.out.println("Non updated researcher units:"+numResUnitNonUpdated);			
			System.out.println("---------------------------");
			System.out.println("Updated researchers:"+numResUpdated);
			System.out.println("Updated projects:"+numPrjUpdated);
			System.out.println("Updated org units:"+numOrgUpdated);
			System.out.println("Updated researcher units:"+numResUnitUpdated);
		}
		jsonLogger.close();
	}
	
	public void closeWithResultsFile(String outputFileName) throws Exception{
		if (conn == null){
			throw new Exception("Connection to dspace database not found!");
		}		
		else{
			System.out.println("Cleaning jdyna_values_seq ... ");
			DButils.cleanJDYNAid(conn);
			System.out.println("Cleaned!");			
			System.out.println("Cleaning jdyna_prop_seq ... ");
			DButils.cleanCRIS_PROPid(conn);			
			System.out.println("Cleaned!");
			System.out.println("---------------------------");
			FileWriter file = new FileWriter(outputFileName+".txt");
			file.write("New researchers added:"+numResAdded);
			file.write('\n');
			file.write("New projects added:"+numPrjAdded);
			file.write('\n');
			file.write("New org units added:"+numOrgAdded);
			file.write('\n');
			file.write("New researcher units added:"+numResUnitAdded);
			file.write('\n');
			file.write("---------------------------");
			file.write('\n');
			file.write("Non updated researchers:"+numResNonUpdated);
			file.write('\n');
			file.write("Non updated projects:"+numPrjNonUpdated);
			file.write('\n');
			file.write("Non updated org units:"+numOrgNonUpdated);
			file.write('\n');
			file.write("Non updated researcher units:"+numResUnitNonUpdated);
			file.write('\n');
			file.write("---------------------------");
			file.write('\n');
			file.write("Updated researchers:"+numResUpdated);
			file.write('\n');
			file.write("Updated projects:"+numPrjUpdated);
			file.write('\n');
			file.write("Updated org units:"+numOrgUpdated);
			file.write('\n');
			file.write("Updated researcher units:"+numResUnitUpdated);			
			file.flush();
			file.close();			
		}
		jsonLogger.close();
	}
	
	/*protected void finalize() throws Throwable { 
		close();
		super.finalize(); 
	}*/	
	
	public Long getRPidBySourceidANDsourceref(String sourceid, String sourceref) throws SQLException{
		return DButils.getRPidBySourceidANDsourceref(conn, sourceid, sourceref);
	}
	
	public Long getPJidBySourceidANDsourceref(String sourceid, String sourceref) throws SQLException{
		return DButils.getPJidBySourceidANDsourceref(conn, sourceid, sourceref);
	}
	
	public Long getOUidBySourceidANDsourceref(String sourceid, String sourceref) throws SQLException{
		return DButils.getOUidBySourceidANDsourceref(conn, sourceid, sourceref);
	}

	public DBconnector getDBconnector(){
		return conn;
	}
	
}
