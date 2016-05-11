package postgresql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import loader.Attribute;
import loader.Attribute.Dtype;

public class DButils {
	
	private static int allocationSize = 50;
	
	//sourceref and sourceid constants
	private static String SOURCE_SPLIT_REGEXP = "\\|";
	private static String SOURCE_CONCAT = "|";
	
	public static void cleanCRIS_PROPid(DBconnector conn) throws SQLException{
		long maxCRISPROPid = conn.executeQueryReturnLong(SQLsentences.MAXCRIS_PROPid); //get bigger id found in the database
		long newCRISPROPseq = conn.executeQueryReturnLong(SQLsentences.lastCRIS_PROPseq)*allocationSize; //get bigger id delivered by the sequencer
			
		if (maxCRISPROPid >= newCRISPROPseq){
			PreparedStatement st = conn.getConnection().prepareStatement(SQLsentences.newCRIS_PROPseq);
			st.setLong(1,(maxCRISPROPid/50)+1);
			conn.executeUpdate(st.toString());
		}		
	}
	
	public static void cleanJDYNAid(DBconnector conn) throws SQLException{
		long maxJDYNAid = conn.executeQueryReturnLong(SQLsentences.MAXJDYNAid); //get bigger id found in the database
		long newJDYNAseq = conn.executeQueryReturnLong(SQLsentences.lastJDYNAseq)*allocationSize; //get bigger id delivered by the sequencer
		
		if (maxJDYNAid >= newJDYNAseq){
			PreparedStatement st = conn.getConnection().prepareStatement(SQLsentences.newJDYNAseq);
			st.setLong(1,(maxJDYNAid/50)+1);
			conn.executeUpdate(st.toString());
		}				
	}
	
	private static long getNextCRIS_PROPid(DBconnector conn) throws SQLException{
		long idBase = conn.executeQueryReturnLong(SQLsentences.CRIS_PROPid);
		idBase *= allocationSize; //hibernate things...
		
		long idMax;
		try {
			idMax = conn.executeQueryReturnLong(SQLsentences.MAXCRIS_PROPid);
		} catch (SQLException e) {
			idMax=-1;
		}		 
		
		if(idMax >= idBase) return idMax+1;
        else return idBase;		
	}
	
	private static long getNextJDYNAid(DBconnector conn) throws SQLException{
		long idBase = conn.executeQueryReturnLong(SQLsentences.JDYNAid);
		idBase *= allocationSize; //hibernate things...
		
		long idMax;
		try {
			idMax = conn.executeQueryReturnLong(SQLsentences.MAXJDYNAid);
		} catch (SQLException e) {
			idMax=-1;
		}		 
		
		if(idMax >= idBase) return idMax+1;
        else return idBase;		
	}	
	
	private static long addCRIS_Entity(DBconnector conn, int typo_id, String seqTableName, String idPrefix, String tableName, String sourceid, String sourceref) throws SQLException{
		long id = getNextCRIS_EntityID(conn, seqTableName);
		String CRISid = idPrefix+String.format("%05d",id);		
		String uuid = getNewUUID(conn);
		
		String query = SQLsentences.insertCRIS_ENTITY;
		query = query.replace("$tableName", tableName);
		
		if (typo_id == -1){		
			query = query.replace("$typo_id_col","");
			query = query.replace("$typo_id_val","");
		}
		else {
			query = query.replace("$typo_id_col",",typo_id");
			query = query.replace("$typo_id_val",",'"+typo_id+"'");
		}
		
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setLong(1, id);
		st.setString(2, CRISid);
		st.setString(3, uuid);
		st.setString(4, sourceid);
		st.setString(5, sourceref);
		conn.executeUpdate(st);
		st.close();
		System.out.println("Added row into "+tableName+" ID:"+id+" uuid:"+uuid);		
		return id;		
	}
	
	private static long addCRIS_Entity(DBconnector conn,int manualID, int typo_id, String seqTableName, String idPrefix, String tableName, String sourceid, String sourceref) throws SQLException{
		long id = manualID;
		String CRISid = idPrefix+String.format("%05d",id);		
		String uuid = getNewUUID(conn);
		
		String query = SQLsentences.insertCRIS_ENTITY;
		query = query.replace("$tableName", tableName);
		
		if (typo_id == -1){		
			query = query.replace("$typo_id_col","");
			query = query.replace("$typo_id_val","");
		}
		else {
			query = query.replace("$typo_id_col",",typo_id");
			query = query.replace("$typo_id_val",",'"+typo_id+"'");
		}
		
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setLong(1, id);
		st.setString(2, CRISid);
		st.setString(3, uuid);
		st.setString(4, sourceid);
		st.setString(5, sourceref);
		conn.executeUpdate(st);
		st.close();
		System.out.println("Added row into "+tableName+" ID:"+id+" uuid:"+uuid);		
		return id;		
	}
	
	
	private static String getNewUUID(DBconnector conn) throws SQLException{
		UUID uuid = UUID.randomUUID();
        return uuid.toString();
	}
	
	private static long getNextCRIS_EntityID(DBconnector conn, String tableName) throws SQLException{
		String query = SQLsentences.nextvalCRIS_EntityID.replace("$tableName",tableName);		
		return conn.executeQueryReturnLong(query);		
	}
	
	private static long addJDYNA_VALUE(DBconnector conn, String dtype, String sortvalue, String textvalue, String linkdescription, String linkvalue, long rpvalue, long dovalue, String datevalue) throws SQLException{
		long id = getNextJDYNAid(conn);
		PreparedStatement st = conn.getConnection().prepareStatement(SQLsentences.insertJDYNA_VALUE);
		st.setString(1,dtype);
		st.setLong(2,id);
		if (sortvalue != null) st.setString(3,sortvalue.toLowerCase());		
		else st.setNull(3,Types.VARCHAR);
		st.setString(4,textvalue);	
		if (linkdescription == null) st.setString(5,linkdescription);
		else st.setString(5,linkdescription.substring(0,Math.min(linkdescription.length(),254)));
		if (linkvalue == null) st.setString(6,linkvalue);
		else st.setString(6,linkvalue.substring(0,Math.min(linkvalue.length(),254)));
		if (rpvalue != -1) st.setInt(7, (int) rpvalue);	
		else st.setNull(7,Types.INTEGER);
		if (dovalue != -1) st.setInt(8, (int) dovalue);
		else st.setNull(8,Types.INTEGER);		
		if (datevalue == null) st.setNull(9,Types.DATE);		
		else st.setDate(9,java.sql.Date.valueOf(datevalue));
		conn.executeUpdate(st);
		st.close();
		System.out.println("Added row into jdyna_values ID:"+id+" dtype:"+dtype+" sortvalue:"+sortvalue+" textvalue:"+textvalue+" linkdescription:"+linkdescription+" linkvalue:"+linkvalue+" rpvalue:"+rpvalue+" dovalue:"+dovalue);
		return id;		
	}	
	
	private static long updateJDYNA_VALUE(DBconnector conn, String dtype, long id, String sortvalue, String textvalue, String linkdescription, String linkvalue, String datevalue, String condition, String conditionVar) throws SQLException{
		
		long result = 0;
		String query = SQLsentences.updateJDYNAvalue;
		query = query.replace("$conditionVar", conditionVar);
		query = query.replace("$condition", condition);			
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		
		Map<String, Object> compareVariables = new HashMap<String, Object>();
		compareVariables.put("sortvalue",sortvalue );
		compareVariables.put("textvalue",textvalue);
		compareVariables.put("linkdescription",linkdescription);
		compareVariables.put("linkvalue",linkvalue);	
		if (datevalue != null) compareVariables.put("datevalue",java.sql.Date.valueOf(datevalue));
		else compareVariables.put("datevalue",null);
		Object conditionValue = compareVariables.get(conditionVar);		
		
		if (sortvalue != null) st.setString(1,sortvalue.toLowerCase());		
		else st.setNull(1,Types.VARCHAR);			 
		if (datevalue != null) st.setDate(2,java.sql.Date.valueOf(datevalue)); 					
		else st.setNull(2,Types.DATE);		
		if (textvalue != null) st.setString(3,textvalue);		
		else st.setNull(3,Types.VARCHAR);
		if (linkdescription != null) st.setString(4,linkdescription);		
		else st.setNull(4,Types.VARCHAR);
		if (linkvalue != null) st.setString(5,linkvalue);		
		else st.setNull(5,Types.VARCHAR);
		st.setInt(6, (int)id);
		if (conditionValue.getClass().getName().equals("java.sql.Date")) st.setDate(7,(java.sql.Date) conditionValue);		
		else st.setString(7,(String) conditionValue);		
		result = conn.executeUpdate(st);
		st.close();		
		if (result > 0) System.out.println("Updated row in jdyna_values ID:"+id+" sortvalue:"+sortvalue+" textvalue:"+textvalue+" linkdescription:"+linkdescription+" linkvalue:"+linkvalue+" datevalue:"+datevalue);
		return result;
	}
	
	public static void addResearcher(DBconnector conn, ArrayList<Attribute> values, String sourceid, String sourceref) throws Exception{
		System.out.println("Adding new researcher with sourceid:" + sourceid + " and sourceref:"+ sourceref);
		values = setAttributesIDs(conn,values, "cris_rp_pdef");	
		System.out.println("All attribute shortnames found in the system!");		
		long rpid = addCRIS_Entity(conn,-1,"cris_rpage_seq","rp","cris_rpage",sourceid,sourceref);		
		for (Attribute attribute: values){			
			long jdynaID = addJDYNA_VALUE(conn, attribute.getDtype(), (""+attribute.getValue()).toLowerCase(), attribute.getValue(), attribute.getLinkdescription(), attribute.getLinkvalue(),attribute.getRPvalue(),attribute.getDOvalue(),attribute.getDatevalue());			
			addCRIS_PROP(conn,"cris_rp_prop",jdynaID,rpid, attribute.getTypo_id(),getNextPositiondef(conn, "cris_rp_prop", rpid, attribute.getTypo_id()));
			conn.getLog().addResearcherAttribute((int)rpid,"new",attribute.getShortname(),attribute.getValue(),"new");
		}		
		System.out.println("New researcher added!");
	}
	
	public static void addResearcher(DBconnector conn, ArrayList<Attribute> values, int manualID, String sourceid, String sourceref) throws Exception{
		System.out.println("Adding new researcher with sourceid:" + sourceid + " and sourceref:"+ sourceref);
		values = setAttributesIDs(conn,values, "cris_rp_pdef");	
		System.out.println("All attribute shortnames found in the system!");		
		long rpid = addCRIS_Entity(conn,manualID,-1,"cris_rpage_seq","rp","cris_rpage",sourceid,sourceref);		
		for (Attribute attribute: values){			
			long jdynaID = addJDYNA_VALUE(conn, attribute.getDtype(), (""+attribute.getValue()).toLowerCase(), attribute.getValue(), attribute.getLinkdescription(), attribute.getLinkvalue(),attribute.getRPvalue(),attribute.getDOvalue(),attribute.getDatevalue());			
			addCRIS_PROP(conn,"cris_rp_prop",jdynaID,rpid, attribute.getTypo_id(),getNextPositiondef(conn, "cris_rp_prop", rpid, attribute.getTypo_id()));
			conn.getLog().addResearcherAttribute((int)rpid,"new",attribute.getShortname(),attribute.getValue(),"new");
		}		
		System.out.println("New researcher added!");
	}
	
	public static void addResearch(DBconnector conn, ArrayList<Attribute> values, int typo_id, String sourceid, String sourceref) throws Exception{		
		System.out.println("Adding new research object with sourceid:" + sourceid + " and sourceref:"+ sourceref);
		values = setAttributesIDs(conn, values, "cris_do_pdef");
		System.out.println("All attribute shortnames found in the system!");
		long doid = addCRIS_Entity(conn,typo_id,"CRIS_DYNAOBJ_SEQ","do","cris_do",sourceid,sourceref);
		for (Attribute attribute: values){			
			long jdynaID = addJDYNA_VALUE(conn, attribute.getDtype(), attribute.getValue(), attribute.getValue(), attribute.getLinkdescription(), attribute.getLinkvalue(),attribute.getRPvalue(),attribute.getDOvalue(),attribute.getDatevalue());
			addCRIS_PROP(conn,"cris_do_prop",jdynaID,doid, attribute.getTypo_id(),getNextPositiondef(conn, "cris_do_prop", doid, attribute.getTypo_id()));
			conn.getLog().addResearchAttribute((int)doid,"new",attribute.getShortname(),attribute.getValue(),"new");
		}		
		System.out.println("New researcher object added!");
	}
	
	public static void addProject(DBconnector conn, ArrayList<Attribute> values, String sourceid, String sourceref) throws Exception{		
		System.out.println("Adding new project...");
		values = setAttributesIDs(conn, values, "cris_pj_pdef");	
		
		System.out.println("All attribute shortnames found in the system!");
		long pjid = addCRIS_Entity(conn,-1,"cris_project_seq","pj","cris_project",sourceid,sourceref);
		for (Attribute attribute: values){			
			long jdynaID = addJDYNA_VALUE(conn, attribute.getDtype(), attribute.getValue(), attribute.getValue(), attribute.getLinkdescription(), attribute.getLinkvalue(),attribute.getRPvalue(),attribute.getDOvalue(),attribute.getDatevalue());
			addCRIS_PROP(conn,"cris_pj_prop",jdynaID,pjid, attribute.getTypo_id(),getNextPositiondef(conn, "cris_pj_prop", pjid, attribute.getTypo_id()));
			conn.getLog().addProjectAttribute((int)pjid,"new",attribute.getShortname(),attribute.getValue(),"new");
		}		
		System.out.println("New project added!");
	}
	
	public static void addOrgUnit(DBconnector conn, ArrayList<Attribute> values, String sourceid, String sourceref) throws Exception{		
		System.out.println("Adding new org unit...");
		values = setAttributesIDs(conn, values, "cris_ou_pdef");	
		System.out.println("All attribute shortnames found in the system!");
		long ouid = addCRIS_Entity(conn,-1,"cris_ou_seq","ou","cris_orgunit",sourceid,sourceref);
		for (Attribute attribute: values){			
			long jdynaID = addJDYNA_VALUE(conn, attribute.getDtype(), attribute.getValue(), attribute.getValue(), attribute.getLinkdescription(), attribute.getLinkvalue(),attribute.getRPvalue(),attribute.getDOvalue(),attribute.getDatevalue());
			addCRIS_PROP(conn,"cris_ou_prop",jdynaID,ouid, attribute.getTypo_id(),getNextPositiondef(conn, "cris_ou_prop", ouid, attribute.getTypo_id()));
			conn.getLog().addOrgAttribute((int)ouid,"new",attribute.getShortname(),attribute.getValue(),"new");
		}		
		System.out.println("New org unit added!");
	}
	
	private static Attribute setAttributePROPid(DBconnector conn, Attribute attribute, String tableName) throws SQLException{
		attribute.setTypo_id(getPROPid(conn, tableName, attribute.getShortname()));
		return attribute;
	}
	
	private static ArrayList<Attribute> setAttributesIDs(DBconnector conn, ArrayList<Attribute> values, String tableName) throws Exception {
		Iterator<Attribute> iter = values.iterator();
		
		while (iter.hasNext()){	
			Attribute attribute = iter.next();
			try{		
				attribute = setAttributePROPid(conn,attribute,tableName);
			}catch(Exception e){
				System.out.println("Problem while finding the ID of "+attribute.getShortname()+" shortname, probably shortname not found or already not defined");
				throw e;
			}
			try{
				attribute = setAttributePointervalue(conn,attribute);
			}catch(Exception e){
				System.out.println("Pointer to another object (sourceid:"+attribute.getValue()+") not found");
				throw(e);
			}
		}
		return values;
	}
	
	private static Attribute setAttributePointervalue(DBconnector conn, Attribute attribute) throws SQLException{
		if (attribute.getDtype().equals(Attribute.Dtype.RPPOINTER.getDtype())){
			attribute.setRPvalue(Long.parseLong(attribute.getValue()));			
		}
		else if(attribute.getDtype().equals(Attribute.Dtype.DOPOINTER.getDtype())){
			attribute.setDOvalue(Long.parseLong(attribute.getValue()));			
		}
		else if(attribute.getDtype().equals(Attribute.Dtype.PJPOINTER.getDtype())){
			attribute.setPJvalue(Long.parseLong(attribute.getValue()));			
		}
		else if(attribute.getDtype().equals(Attribute.Dtype.OUPOINTER.getDtype())){
			attribute.setOUvalue(Long.parseLong(attribute.getValue()));			
		}
		return attribute;		
	}
	
	private static long getPROPid(DBconnector conn, String pdefTable, String shortname) throws SQLException{
		String query = SQLsentences.PROPid;
		query = query.replace("$pdefTable", pdefTable);
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setString(1,shortname);		
		long result = conn.executeQueryReturnLong(st);
		st.close();
		return result;		
	}	
	
	private static void addCRIS_PROP(DBconnector conn, String tableName, long value_id, long parent_id, long typo_id, int positiondef) throws SQLException{
		long id = getNextCRIS_PROPid(conn);
		String query = SQLsentences.insertCRIS_PROP.replace("$tableName", tableName);
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setLong(1,id);
		st.setInt(2,positiondef);
		st.setLong(3,value_id);
		st.setLong(4,parent_id);
		st.setLong(5,typo_id);
		conn.executeUpdate(st);
		st.close();
		System.out.println("Added row into "+tableName+" ID:"+id+" valueid:"+value_id+" parentid:"+parent_id+" typoid:"+typo_id+" positiondef:"+positiondef);
	}
	
	/*private static long getRPid(DBConnector conn, String name) throws SQLException{
		name=name.toLowerCase();
		PreparedStatement st = conn.getConnection().prepareStatement(SQLSentences.getRPid);
		st.setString(1,name);		
		long rpid = conn.executeQueryReturnLong(st);
		st.close();
		return rpid;		
	}*/
	
	/*private static long getDOid(DBConnector conn, String name) throws SQLException{
		name=name.toLowerCase();
		PreparedStatement st = conn.getConnection().prepareStatement(SQLSentences.getDOid);
		st.setString(1,name);
		long doid = conn.executeQueryReturnLong(st);		
		st.close();
		return doid;
	}*/
	
	
	
	public static long checkIfExistsJDYNAvalue(DBconnector conn, String value) throws SQLException{
		value=value.toLowerCase();
		PreparedStatement st = conn.getConnection().prepareStatement(SQLsentences.checkJDYNAvalue);
		st.setString(1,value);
		long result = conn.executeQueryReturnLong(st);		
		st.close();		
		return result;
	}
	
	public static boolean checkDuplicatedJDYNAvalues(DBconnector conn, String shortname, String dtype, String crisPropTable, String crisPdefTable) throws SQLException{
		String query = SQLsentences.checkDuplicatedJDYNAvalues;
		query = query.replace("$dtype", dtype);
		query = query.replace("$crisPropTable", crisPropTable);		
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setString(1, shortname);
		st.setString(2, crisPdefTable);
		ResultSet rs = st.executeQuery();
		System.out.println("Duplicated values by shortname '"+shortname+"':");
		boolean duplicated = false;
		while(rs.next()){
			System.out.println(rs.getString(1)+" | "+rs.getInt(2)+" times");
			duplicated = true;
		}
		return duplicated;		
	}
	
	public static void checkSimilarityJDYNAvalues(DBconnector conn, String string, float minSimilarity) throws SQLException{
		String query = SQLsentences.checkSimilarityJDYNAvalues;
		query = query.replace("$string", string);
		query = query.replace("$minSimilarity", Float.toString(minSimilarity));
		PreparedStatement st = conn.getConnection().prepareStatement(query);		
		ResultSet rs = st.executeQuery();
		System.out.println("Similarity values for '"+string+"':");		
		while(rs.next()){
			System.out.println(rs.getString(1)+" | "+rs.getFloat(2));			
		}		
	}	
	
	public static boolean checkJDYNAvalueByShortname(DBconnector conn, String value, String propTable, String pdefTable, String shortname ) throws SQLException{
		String query = SQLsentences.checkJDYNAvalueByShortname;
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setString(1, value);
		st.setString(2, shortname);
		long result = conn.executeQueryReturnLong(st);		
		st.close();		
		if (result > 0) return true;
		else return false;		
	}
	
	public static long getDOid(DBconnector conn, String sourceref, String sourceid) throws SQLException{
		String query = SQLsentences.getCRIS_ENTITY;
		query = query.replace("$tableName", "cris_do");
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setString(1, sourceref);
		//st.setString(2, sourceid); revisar porque no hay parametro 2 y se esta poniendo
		long result = conn.executeQueryReturnLong(st);		
		st.close();		
		return result;
	}
	
	private static long getCRISidBySourceidANDsourceref(DBconnector conn, String sourceid, String sourceref, String tableName) throws SQLException{
		String query = SQLsentences.getCRIS_ENTITY;
		query = query.replace("$tableName", tableName);
		PreparedStatement st = conn.getConnection().prepareStatement(query);				
		st.setString(1, sourceid);
		st.setString(2, sourceref);
		long result = conn.executeQueryReturnLong(st);		
		st.close();		
		return result;
	}
	
	public static long getRPidBySourceidANDsourceref(DBconnector conn, String sourceid, String sourceref) throws SQLException{
		return getCRISidBySourceidANDsourceref(conn,sourceid,sourceref,"cris_rpage");
	}
	
	public static long getPJidBySourceidANDsourceref(DBconnector conn, String sourceid, String sourceref) throws SQLException{
		return getCRISidBySourceidANDsourceref(conn,sourceid,sourceref,"cris_project");
	}
	
	public static long getOUidBySourceidANDsourceref(DBconnector conn, String sourceid, String sourceref) throws SQLException{
		return getCRISidBySourceidANDsourceref(conn,sourceid,sourceref,"cris_orgunit");
	}	
	
	public static long getRPidByORCID(DBconnector conn,String orcid) throws SQLException{
		return getParentIdByLinkdescriptionANDshortname(conn,"cris_rp_prop","cris_rp_pdef",orcid,"orcid");
	}
	
	public static String getRPfield(DBconnector conn, long rpid, String shortname) throws SQLException{
		String query = SQLsentences.getRPfield;
		query = query.replace("$tableName", "cris_rp_pdef");
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setString(1, String.valueOf(rpid));
		st.setString(2, shortname);
		String result = conn.executeQueryReturnString(st.toString());
		st.close();		
		return result;
	}	
	
	public static boolean checkJDYNAlinkdescriptionByShortname(DBconnector conn, String value, String propTable, String pdefTable, String shortname) throws SQLException{
		String query = SQLsentences.checkJDYNAlinkdescriptionByShortname;
		//query = query.replace("$value", value);
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setString(1, value);
		st.setString(2, shortname);		
		long result = conn.executeQueryReturnLong(st);		
		st.close();		
		if (result > 0) return true;
		else return false;		
	}			
		
	private static int getNextPositiondef(DBconnector conn, String tableName, long parentId, long typoId) throws SQLException{
		String query = SQLsentences.getNextPositiondef;
		query = query.replace("$tableName", tableName);
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setLong(1,parentId);
		st.setLong(2,typoId);
		long result = conn.executeQueryReturnLong(st);
		st.close();		
		return (int)(result);
	}
	
	public static int updateResearcher(DBconnector conn, ArrayList<Attribute> values, long rpId, String sourceid, String sourceref) throws Exception{
		System.out.println("Updating researcher with sourceid:" + sourceid + " and sourceref:"+ sourceref);
		values = setAttributesIDs(conn, values, "cris_rp_pdef");	
		System.out.println("All attribute shortnames found in the system!");	
		
		int somethingHasChanged = 0; 
		for (Attribute attribute: values){
			if (getIfShortnameIsRepeteableOrNot(conn,"cris_rp_pdef",attribute.getShortname()) &&  !checkIfAttributeExists(conn,attribute,rpId,"cris_rp_prop","cris_rp_pdef")){
				long jdynaID = addJDYNA_VALUE(conn,attribute.getDtype(),(""+attribute.getValue()).toLowerCase(),attribute.getValue(),attribute.getLinkdescription(),attribute.getLinkvalue(),attribute.getRPvalue(),attribute.getDOvalue(),attribute.getDatevalue());			
				addCRIS_PROP(conn,"cris_rp_prop",jdynaID,rpId,attribute.getTypo_id(),getNextPositiondef(conn,"cris_rp_prop",rpId,attribute.getTypo_id()));
				somethingHasChanged++;
				conn.getLog().addResearcherAttribute((int)rpId,"updated",attribute.getShortname(),attribute.getValue(),"new");

			}			
			else if(!getIfShortnameIsRepeteableOrNot(conn,"cris_rp_pdef",attribute.getShortname()) && !checkIfAttributeExists(conn,attribute,rpId,"cris_rp_prop","cris_rp_pdef") ){
				
				
				long jdynaID = getJDYNAidByParentIdANDshortname(conn,"cris_rp_prop","cris_rp_pdef",rpId,attribute.getShortname());								
				long rowsUpdated = updateJDYNA_VALUE(conn,attribute.getDtype(),jdynaID,(""+attribute.getValue()).toLowerCase(),attribute.getValue(),attribute.getLinkdescription(),attribute.getLinkvalue(),attribute.getDatevalue(),attribute.getUpdateCondition(),attribute.getUpdateVar());
				
				
				
				
				if (rowsUpdated <= 0) {
					System.out.println("attribute "+attribute.getShortname()+" with value "+attribute.getValue()+" already found, not added");
					conn.getLog().addResearcherAttribute((int)rpId,"updated",attribute.getShortname(),attribute.getValue(),"already found");
				}
				else {
					somethingHasChanged++;
					conn.getLog().addResearcherAttribute((int)rpId,"updated",attribute.getShortname(),attribute.getValue(),"updated");
				}

			}
			else {
				System.out.println("attribute "+attribute.getShortname()+" with value "+attribute.getValue()+" already found, not added");
				conn.getLog().addResearcherAttribute((int)rpId,"updated",attribute.getShortname(),attribute.getValue(),"already found");
			}
		}	
		if (somethingHasChanged > 0) updateSource(conn,"cris_rpage",sourceid,sourceref,rpId);
		System.out.println("Researcher Updated!");
		return somethingHasChanged;
	}
	
	private static boolean checkIfAttributeExists(DBconnector conn,Attribute attribute, long parentId, String propTable, String pdefTable) throws SQLException{
		if(attribute.getDtype().equals(Dtype.TEXT.getDtype())){
			return checkJDYNAvalueByParentIdANDshortname(conn, ""+attribute.getValue(),propTable,pdefTable,parentId,attribute.getShortname());
		}
		else if(attribute.getDtype().equals(Dtype.LINK.getDtype())){
			return checkJDYNAlinkvalueByParentIdANDshortname(conn,attribute.getLinkvalue(),propTable,pdefTable,parentId,attribute.getShortname());
		}
		else if(attribute.getDtype().equals(Dtype.DATE.getDtype())){
			return checkJDYNAdateByParentIdANDshortname(conn,attribute.getDatevalue(), propTable, pdefTable, parentId, attribute.getShortname()); 
		}
		else{ //so its a pointer
			return checkJDYNApointerByParentIdANDshortname(conn,attribute.getPointerValue(),propTable,pdefTable,parentId,attribute.getShortname());
		}
	}
	
	public static int updateProject(DBconnector conn, ArrayList<Attribute> values, long pjId, String sourceid, String sourceref) throws Exception{
		System.out.println("Updating project with sourceid:" + sourceid + " and sourceref:"+ sourceref);
		values = setAttributesIDs(conn, values, "cris_pj_pdef");	
		System.out.println("All attribute shortnames found in the system!");		
		
		int somethingHasChanged = 0; 
		for (Attribute attribute: values){
			if (getIfShortnameIsRepeteableOrNot(conn,"cris_pj_pdef",attribute.getShortname()) &&  !checkIfAttributeExists(conn,attribute,pjId,"cris_pj_prop","cris_pj_pdef")){
				long jdynaID = addJDYNA_VALUE(conn,attribute.getDtype(),(""+attribute.getValue()).toLowerCase(),attribute.getValue(),attribute.getLinkdescription(),attribute.getLinkvalue(),attribute.getRPvalue(),attribute.getDOvalue(),attribute.getDatevalue());			
				addCRIS_PROP(conn,"cris_pj_prop",jdynaID,pjId,attribute.getTypo_id(),getNextPositiondef(conn,"cris_pj_prop",pjId,attribute.getTypo_id()));
				somethingHasChanged++;
				conn.getLog().addProjectAttribute((int)pjId,"updated",attribute.getShortname(),attribute.getValue(),"new");

			}			
			else if(!getIfShortnameIsRepeteableOrNot(conn,"cris_pj_pdef",attribute.getShortname()) && !checkIfAttributeExists(conn,attribute,pjId,"cris_pj_prop","cris_pj_pdef")){
				long rowsUpdated = 0;
				try{
					long jdynaID = getJDYNAidByParentIdANDshortname(conn,"cris_pj_prop","cris_pj_pdef",pjId,attribute.getShortname());								
					rowsUpdated = updateJDYNA_VALUE(conn,attribute.getDtype(),jdynaID,(""+attribute.getValue()).toLowerCase(),attribute.getValue(),attribute.getLinkdescription(),attribute.getLinkvalue(),attribute.getDatevalue(),attribute.getUpdateCondition(),attribute.getUpdateVar());
					
				}catch(Exception e){
					long jdynaID = addJDYNA_VALUE(conn,attribute.getDtype(),(""+attribute.getValue()).toLowerCase(),attribute.getValue(),attribute.getLinkdescription(),attribute.getLinkvalue(),attribute.getRPvalue(),attribute.getDOvalue(),attribute.getDatevalue());			
					addCRIS_PROP(conn,"cris_pj_prop",jdynaID,pjId,attribute.getTypo_id(),getNextPositiondef(conn,"cris_pj_prop",pjId,attribute.getTypo_id()));
					rowsUpdated = 1;
				}				
				if (rowsUpdated <= 0){
					System.out.println("attribute "+attribute.getShortname()+" with value "+attribute.getValue()+" already found, not added");
					conn.getLog().addProjectAttribute((int)pjId,"updated",attribute.getShortname(),attribute.getValue(),"already found");
				}
				else {
					somethingHasChanged++;
					conn.getLog().addProjectAttribute((int)pjId,"updated",attribute.getShortname(),attribute.getValue(),"updated");
				}
			}
			else {
				System.out.println("attribute "+attribute.getShortname()+" with value "+attribute.getValue()+" already found, not added");
				conn.getLog().addProjectAttribute((int)pjId,"updated",attribute.getShortname(),attribute.getValue(),"already found");
			}

		}
		if (somethingHasChanged > 0) updateSource(conn,"cris_project",sourceid,sourceref,pjId);
		System.out.println("Project Updated!");
		return somethingHasChanged;
	} 
	
	public static int updateOrgUnit(DBconnector conn, ArrayList<Attribute> values, long ouId, String sourceid, String sourceref) throws Exception{
		System.out.println("Updating org unit with sourceid:" + sourceid + " and sourceref:"+ sourceref);
		values = setAttributesIDs(conn, values, "cris_ou_pdef");	
		System.out.println("All attribute shortnames found in the system!");
		
		int somethingHasChanged = 0; 
		for (Attribute attribute: values){
			if (getIfShortnameIsRepeteableOrNot(conn,"cris_ou_pdef",attribute.getShortname()) &&  !checkIfAttributeExists(conn,attribute,ouId,"cris_ou_prop","cris_ou_pdef")){
				long jdynaID = addJDYNA_VALUE(conn,attribute.getDtype(),(""+attribute.getValue()).toLowerCase(),attribute.getValue(),attribute.getLinkdescription(),attribute.getLinkvalue(),attribute.getRPvalue(),attribute.getDOvalue(),attribute.getDatevalue());			
				addCRIS_PROP(conn,"cris_ou_prop",jdynaID,ouId,attribute.getTypo_id(),getNextPositiondef(conn,"cris_ou_prop",ouId,attribute.getTypo_id()));
				somethingHasChanged++;
				conn.getLog().addOrgAttribute((int)ouId,"updated",attribute.getShortname(),attribute.getValue(),"new");

			}			
			else if(!getIfShortnameIsRepeteableOrNot(conn,"cris_ou_pdef",attribute.getShortname()) && !checkIfAttributeExists(conn,attribute,ouId,"cris_ou_prop","cris_ou_pdef") ){
				long jdynaID = getJDYNAidByParentIdANDshortname(conn,"cris_ou_prop","cris_ou_pdef",ouId,attribute.getShortname());								
				long rowsUpdated = updateJDYNA_VALUE(conn,attribute.getDtype(),jdynaID,(""+attribute.getValue()).toLowerCase(),attribute.getValue(),attribute.getLinkdescription(),attribute.getLinkvalue(),attribute.getDatevalue(),attribute.getUpdateCondition(),attribute.getUpdateVar());
				if (rowsUpdated <= 0){
					System.out.println("attribute "+attribute.getShortname()+" with value "+attribute.getValue()+" already found, not added");
					conn.getLog().addOrgAttribute((int)ouId,"updated",attribute.getShortname(),attribute.getValue(),"already found");
				}
				else{
					somethingHasChanged++;
					conn.getLog().addOrgAttribute((int)ouId,"updated",attribute.getShortname(),attribute.getValue(),"updated");
				}
			}
			else {
				System.out.println("attribute "+attribute.getShortname()+" with value "+attribute.getValue()+" already found, not added");
				conn.getLog().addOrgAttribute((int)ouId,"updated",attribute.getShortname(),attribute.getValue(),"already found");
			}
		}
		if (somethingHasChanged > 0) updateSource(conn,"cris_orgunit",sourceid,sourceref,ouId);
		System.out.println("Org Unit Updated!");
		return somethingHasChanged;
	} 
	
	public static int updateResearch(DBconnector conn, ArrayList<Attribute> values, long doId, String sourceid, String sourceref) throws Exception{
		System.out.println("Updating research unit with sourceid:" + sourceid + " and sourceref:"+ sourceref);
		values = setAttributesIDs(conn, values, "cris_do_pdef");	
		System.out.println("All attribute shortnames found in the system!");
		
		int somethingHasChanged = 0; 
		for (Attribute attribute: values){
			if (getIfShortnameIsRepeteableOrNot(conn,"cris_do_pdef",attribute.getShortname()) &&  !checkIfAttributeExists(conn,attribute,doId,"cris_do_prop","cris_do_pdef")){
				long jdynaID = addJDYNA_VALUE(conn,attribute.getDtype(),(""+attribute.getValue()).toLowerCase(),attribute.getValue(),attribute.getLinkdescription(),attribute.getLinkvalue(),attribute.getRPvalue(),attribute.getDOvalue(),attribute.getDatevalue());			
				addCRIS_PROP(conn,"cris_do_prop",jdynaID,doId,attribute.getTypo_id(),getNextPositiondef(conn,"cris_do_prop",doId,attribute.getTypo_id()));
				somethingHasChanged++;
				conn.getLog().addResearchAttribute((int)doId,"updated",attribute.getShortname(),attribute.getValue(),"new");
			}			
			else if(!getIfShortnameIsRepeteableOrNot(conn,"cris_do_pdef",attribute.getShortname()) && !checkIfAttributeExists(conn,attribute,doId,"cris_do_prop","cris_do_pdef") ){
				long jdynaID = getJDYNAidByParentIdANDshortname(conn,"cris_do_prop","cris_do_pdef",doId,attribute.getShortname());								
				long rowsUpdated = updateJDYNA_VALUE(conn,attribute.getDtype(),jdynaID,(""+attribute.getValue()).toLowerCase(),attribute.getValue(),attribute.getLinkdescription(),attribute.getLinkvalue(),attribute.getDatevalue(),attribute.getUpdateCondition(),attribute.getUpdateVar());
				if (rowsUpdated <= 0){
					System.out.println("attribute "+attribute.getShortname()+" with value "+attribute.getValue()+" already found, not added");
					conn.getLog().addResearchAttribute((int)doId,"updated",attribute.getShortname(),attribute.getValue(),"already found");
				}
				else{
					somethingHasChanged++;
					conn.getLog().addResearchAttribute((int)doId,"updated",attribute.getShortname(),attribute.getValue(),"updated");
				}
			}
			else {
				System.out.println("attribute "+attribute.getShortname()+" with value "+attribute.getValue()+" already found, not added");
				conn.getLog().addResearchAttribute((int)doId,"updated",attribute.getShortname(),attribute.getValue(),"already found");
			}
		}
		if (somethingHasChanged > 0) updateSource(conn,"cris_orgunit",sourceid,sourceref,doId);
		System.out.println("Researcher Unit Updated!");
		return somethingHasChanged;
	} 
	
	private static boolean checkJDYNAvalueByParentIdANDshortname(DBconnector conn, String value, String propTable, String pdefTable, long parentId, String shortname) throws SQLException{
		String query = SQLsentences.checkJDYNAvalueByParentIdANDshortname;
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);		
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setString(1,value);
		st.setLong(2,parentId);
		st.setString(3,shortname);
		long result = conn.executeQueryReturnLong(st);		
		st.close();		
		if (result > 0) return true;
		else return false;		
	}
	
	private static boolean checkJDYNAdateByParentIdANDshortname(DBconnector conn, String date, String propTable, String pdefTable, long parentId, String shortname) throws SQLException{
		String query = SQLsentences.checkJDYNAdateByParentIdANDshortname;
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);		
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setDate(1,java.sql.Date.valueOf(date));
		st.setLong(2,parentId);
		st.setString(3,shortname);
		long result = conn.executeQueryReturnLong(st);		
		st.close();		
		if (result > 0) return true;
		else return false;		
	}
	
	private static boolean checkJDYNAlinkvalueByParentIdANDshortname(DBconnector conn, String value, String propTable, String pdefTable, long parentId, String shortname) throws SQLException{
		String query = SQLsentences.checkJDYNAlinkvalueByParentIdANDshortname;
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);		
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setString(1,value);
		st.setLong(2,parentId);
		st.setString(3,shortname);
		long result = conn.executeQueryReturnLong(st);		
		st.close();		
		if (result > 0) return true;
		else return false;		
	}
	
	private static boolean checkJDYNApointerByParentIdANDshortname(DBconnector conn, long pointer, String propTable, String pdefTable, long parentId, String shortname) throws SQLException{
		String query = SQLsentences.checkJDYNApointerByParentIdANDshortname;
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);		
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setLong(1,pointer);
		st.setLong(2,pointer);
		st.setLong(3,pointer);
		st.setLong(4,pointer);
		st.setLong(5,parentId);
		st.setString(6,shortname);
		long result = conn.executeQueryReturnLong(st);		
		st.close();		
		if (result > 0) return true;
		else return false;		
	}		
		
	public static long getParentIdByLinkdescriptionANDshortname(DBconnector conn,String propTable,String pdefTable,String value,String shortname) throws SQLException{
		String query = SQLsentences.getParentIdByLinkdescriptionANDshortname;
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);		
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setString(1,value);
		st.setString(2,shortname);
		long result = conn.executeQueryReturnLong(st);		
		st.close();
		return result;
	}
	
	private static String getSourceId(DBconnector conn, String tableName, long id) throws SQLException{
		String query = SQLsentences.getSourceId;
		query = query.replace("$tableName", tableName);
		PreparedStatement st = conn.getConnection().prepareStatement(query);		
		st.setLong(1,id);		
		String result =  conn.executeQueryReturnString(st.toString());
		st.close();
		return result;
	}
	
	private static String getSourceRef(DBconnector conn, String tableName, long id) throws SQLException{
		String query = SQLsentences.getSourceRef;
		query = query.replace("$tableName", tableName);
		PreparedStatement st = conn.getConnection().prepareStatement(query);		
		st.setLong(1,id);			
		String result = conn.executeQueryReturnString(st.toString());
		st.close();
		return result;
	}	
	
	private static void updateSource(DBconnector conn, String tableName, String newSourceId, String newSourceRef, long id) throws SQLException{
		String sourceId = getSourceId(conn,tableName,id); 
		String sourceRef = getSourceRef(conn,tableName,id);

		List<String> sourceRefSplit = new LinkedList<String> (Arrays.asList(sourceRef.split(SOURCE_SPLIT_REGEXP)));
		List<String> sourceIdSplit = new LinkedList<String> (Arrays.asList(sourceId.split(SOURCE_SPLIT_REGEXP)));		
		if(sourceRefSplit.contains(newSourceRef)){	
			int sourceRefIndex = sourceRefSplit.indexOf(newSourceRef);
			if(!sourceIdSplit.get(sourceRefIndex).equals(newSourceId)){				
				sourceIdSplit.set(sourceRefIndex,newSourceId);
				System.out.println("Sourceid updated");
			}
			else{
				System.out.println("Sourceid and Sourceref NO need update");
				return;
			}
		}
		else{
			sourceIdSplit.add(newSourceId);
			sourceRefSplit.add(newSourceRef);
			System.out.println("Sourceid and Sourceref updated");
		}
		
		newSourceId = sourceIdSplit.toString().replaceAll("\\[|\\]", "").replaceAll(", ",SOURCE_CONCAT);
		newSourceRef = sourceRefSplit.toString().replaceAll("\\[|\\]", "").replaceAll(", ",SOURCE_CONCAT);
		
		String query = SQLsentences.updateSource;
		query = query.replace("$tableName", tableName);
		PreparedStatement st = conn.getConnection().prepareStatement(query);		
		st.setString(1,newSourceId);
		st.setString(2,newSourceRef);
		st.setLong(3,id);
		conn.executeUpdate(st);
		st.close();		
	}	
	
	public static long getParentIdBySortvalueANDshortname(DBconnector conn,String propTable,String pdefTable,String value,String shortname) throws SQLException{
		String query = SQLsentences.getParentIdBySortvalueANDshortname;
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);		
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setString(1,value);
		st.setString(2,shortname);
		long result = conn.executeQueryReturnLong(st);		
		st.close();
		return result;
	}
	
	public static long getParentIdByTwoSortvaluesANDshortnames(DBconnector conn,String propTable,String pdefTable,String value1,String shortname1,String value2,String shortname2) throws SQLException{
		String query = SQLsentences.getParentIdByTwoSortvaluesANDshortnames;
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);		
		PreparedStatement st = conn.getConnection().prepareStatement(query);		
		st.setString(1,shortname1);
		st.setString(2,value1);
		st.setString(3,shortname2);
		st.setString(4,value2);		
		long result = conn.executeQueryReturnLong(st);		
		st.close();
		return result;
	}
	
	/*private static long getJDYNAidByParentIdANDshortnameANDvalue(DBConnector conn,String propTable,String pdefTable,long parentId,String shortname,String value) throws SQLException{
		String query = SQLSentences.getJDYNAidByParentIdANDshortnameANDvalue;		
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);		
		PreparedStatement st = conn.getConnection().prepareStatement(query);		
		st.setString(1,value);
		st.setLong(2,parentId);
		st.setString(3,shortname);
		long result = conn.executeQueryReturnLong(st);		
		st.close();
		return result;
	}*/
	
	private static boolean getIfShortnameIsRepeteableOrNot(DBconnector conn, String pdefTable, String shortname) throws SQLException{
		String query = SQLsentences.IsShortnameRepeateable;
		query = query.replace("$pdefTable", pdefTable);
		PreparedStatement st = conn.getConnection().prepareStatement(query);		
		st.setString(1,shortname);			
		boolean result = conn.executeQueryReturnBoolean(st);
		st.close();
		return result;
	}
	
	private static long getJDYNAidByParentIdANDshortname(DBconnector conn,String propTable,String pdefTable,long parentId,String shortname) throws SQLException{
		String query = SQLsentences.getJDYNAidByParentIdANDshortname;		
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);		
		PreparedStatement st = conn.getConnection().prepareStatement(query);		
		st.setLong(1,parentId);
		st.setString(2,shortname);
		long result = conn.executeQueryReturnLong(st);		
		st.close();
		return result;
	}
	
	public static int getItemIDbyMetadata(DBconnector conn, String element, String qualifier, String value) throws SQLException{
		String query = SQLsentences.getItemIDbyMetadata;
		PreparedStatement st = conn.getConnection().prepareStatement(query);		
		st.setString(1,element);
		st.setString(2,qualifier);
		st.setString(3,value);		
		long result = conn.executeQueryReturnLong(st);		
		st.close();
		return (int)result;
	}	
	
	public static String getMetadataValueByItemIandMetadata(DBconnector conn, String element, String qualifier, int itemID) throws SQLException{
		String query = SQLsentences.getItemIDbyMetadata;
		PreparedStatement st = conn.getConnection().prepareStatement(query);		
		st.setString(1,element);
		st.setString(2,qualifier);
		st.setInt(3,itemID);		
		String result = conn.executeQueryReturnString(st.toString());		
		st.close();
		return result;
	}
	
	
	/*public static boolean checkIfExistsOrcid(DBConnector conn, String orcid) throws SQLException{
		return checkJDYNAvalueByShortname(conn,orcid,"cris_rp_prop","cris_rp_pdef","orcid");
	}*/
	
	public static String getJDYNAvalueByParentIdANDshortname(DBconnector conn, String propTable, String pdefTable, long parentId, String shortname) throws SQLException{
		String query = SQLsentences.getJDYNAvalueByParentIdANDshortname;
		query = query.replace("$propTable", propTable);
		query = query.replace("$pdefTable", pdefTable);		
		PreparedStatement st = conn.getConnection().prepareStatement(query);
		st.setLong(1,parentId);
		st.setString(2,shortname);
		String result = conn.executeQueryReturnString(st.toString());		
		st.close();		
		return result;
	}
	

}
