package example;

import java.util.ArrayList;

import loader.Attribute;
import loader.Attribute.Dtype;
import loader.Loader;

public class RelationExample {
	
	//postgres connection credentials
	private static String databaseUrl = "localhost:5432/dspace";
	private static String user = "dspace";
	private static String password = "";
	
	public static void main(String[] args) throws Exception {
		
		Loader CRISloader = new Loader(databaseUrl,user,password,null); //instantiate the CRIS loader
		
		ArrayList<Attribute> researcherValues = new ArrayList<Attribute>();
		researcherValues.add(new Attribute("fullName","Pablo Buenaposada Sanchez",Dtype.TEXT));
		researcherValues.add(new Attribute("preferredName","Pablo B.",Dtype.TEXT));
		researcherValues.add(new Attribute("translatedName","Pablo B.",Dtype.TEXT));
		researcherValues.add(new Attribute("email","pablo.buenaposada@csuc.cat",Dtype.TEXT));
		researcherValues.add(new Attribute("personalsite","Contact page","http://www.csuc.cat/en/personal/buenaposada-pablo"));
		CRISloader.addResearcher(researcherValues, "person1", "CSUC"); //add researcher to the system
		
		ArrayList<Attribute> projectValues = new ArrayList<Attribute>();
		projectValues.add(new Attribute("title","DSpace-CRIS loader project",Dtype.TEXT));
		projectValues.add(new Attribute("startdate","2015-01-15",Dtype.DATE));
		projectValues.add(new Attribute("principalinvestigator",String.valueOf(CRISloader.getRPidBySourceidANDsourceref("person1","CSUC")),Dtype.RPPOINTER));		
		CRISloader.addProject(projectValues, "project1", "CSUC"); //add project to the system
		
		ArrayList<Attribute> orgunitValues = new ArrayList<Attribute>();
		orgunitValues.add(new Attribute("name","DSpace-CRIS loader organization unit",Dtype.TEXT));
		orgunitValues.add(new Attribute("date","2015-01-15",Dtype.DATE));
		orgunitValues.add(new Attribute("director",String.valueOf(CRISloader.getRPidBySourceidANDsourceref("person1","CSUC")),Dtype.RPPOINTER));		
		CRISloader.addOrgUnit(orgunitValues, "org1", "CSUC"); //add org unit to the system
		
		CRISloader.close();
	}
}
