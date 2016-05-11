package loader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JSONlogger {
	
	private static final int RESEARCHER_POS = 0;
	private static final int PROJECT_POS = 1;
	private static final int ORGUNIT_POS = 2;
	private static final int RESEARCHUNIT_POS = 3;
	
	private JSONArray all = new JSONArray();
	
	private static FileWriter file;
	
	public JSONlogger() throws IOException{
		file = new FileWriter("jsonData.json");
		all.add(new JSONObject());
		all.add(new JSONObject());
		all.add(new JSONObject());
		all.add(new JSONObject());
	}
	
	public void addResearcherAttribute(int id,String entityStatus, String attribute, String value, String status){
		addAttribute(RESEARCHER_POS,id,entityStatus,attribute,value,status);
	}
	
	public void addProjectAttribute(int id, String entityStatus,String attribute, String value, String status){
		addAttribute(PROJECT_POS,id,entityStatus,attribute,value,status);
	}
	
	public void addResearchAttribute(int id, String entityStatus,String attribute, String value, String status){
		addAttribute(RESEARCHUNIT_POS,id,entityStatus,attribute,value,status);
	}
	
	public void addOrgAttribute(int id, String entityStatus,String attribute, String value, String status){
		addAttribute(ORGUNIT_POS,id,entityStatus,attribute,value,status);
	}
		
	private void addAttribute(int type, int id, String entityStatus, String attribute, String value, String status){		
		//JSONObject entity = (JSONObject) all.get(type);
		JSONArray attributes = new JSONArray();
		JSONObject newAttribute = new JSONObject();	
		
		newAttribute.put("attribute", attribute);
		newAttribute.put("value", escapeFuckingDoubleQuoteMarks(value));
		newAttribute.put("status", status);		
		newAttribute.put("entitystatus", entityStatus);
		attributes.add(newAttribute);
						
		if (((JSONObject) all.get(type)).containsKey((new Integer(id)))){ //if entity exist in json, append the attribute
			attributes = (JSONArray) ((JSONObject) all.get(type)).get(new Integer(id));
			attributes.add(newAttribute);			
		}
		else ((JSONObject) all.get(type)).put(id,attributes); //if doesn't exist, create it and append the attribute
	}
	
	private String escapeFuckingDoubleQuoteMarks(String input){
		return input.replaceAll("\\\"", "");
	}
	
	private String escapeFuckingSimpleQuoteMarks(String input){
		return input.replaceAll("\\\'", "\\\\'");
	}
	
	private void write(){
        try {
            file.write(escapeFuckingSimpleQuoteMarks(all.toJSONString())); 
            file.flush();
            System.out.println("Successfully Copied JSON Report to File...");
        } catch (IOException e) {
            e.printStackTrace(); 
        }
	}
	
	public void close(){
		write();
		try {
			file.close();
			String template = FileUtils.readFileToString(new File("reportTemplate.html"));
			String json = FileUtils.readFileToString(new File("jsonData.json"));
			json = escapeFuckingSimpleQuoteMarks(json);
			template = template.replaceFirst("var json = '';", "var json = '"+json+"';");
			file = new FileWriter("report.html");
			file.write(template);
			file.flush();
			file.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	

}
