package example;

import java.util.ArrayList;

import loader.Attribute;
import loader.Attribute.Dtype;
import loader.Loader;

public class MainExample {
	
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
					
		CRISloader.close();
	}
}
