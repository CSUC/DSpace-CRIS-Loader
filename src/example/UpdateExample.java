package example;

import java.util.ArrayList;

import loader.Attribute;
import loader.Attribute.Dtype;
import loader.Loader;

public class UpdateExample {
	
	//postgres connection credentials
	private static String databaseUrl = "localhost:5432/dspace";
	private static String user = "dspace";
	private static String password = "";
	
	public static void main(String[] args) throws Exception {		
		
		Loader CRISloader = new Loader(databaseUrl,user,password,null); //instantiate the CRIS loader
		
		ArrayList<Attribute> newProjectValues = new ArrayList<Attribute>();
		newProjectValues.add(new Attribute("title","DSpace-CRIS loader project",Dtype.TEXT));
		newProjectValues.add(new Attribute("status","open",Dtype.TEXT));
		newProjectValues.add(new Attribute("startdate","2015-01-15",Dtype.DATE));
		CRISloader.addProject(newProjectValues, "project2", "CSUC"); //add project to the system
		
		ArrayList<Attribute> updateProjectValues = new ArrayList<Attribute>();
		updateProjectValues.add(new Attribute("title","DSpace-CRIS loader project *UPDATED",Dtype.TEXT));
		updateProjectValues.add(new Attribute("status","open",Dtype.TEXT));
		updateProjectValues.add(new Attribute("code","12345",Dtype.TEXT));
		Attribute startDate = new Attribute("startdate","2016-01-15",Dtype.DATE);
		startDate.setUpdateCondition(">");
		updateProjectValues.add(startDate);	
		CRISloader.updateProject(updateProjectValues, CRISloader.getPJidBySourceidANDsourceref("project2","CSUC"), "project2", "CSUC"); //update project		
		
		CRISloader.close();
	}
}
