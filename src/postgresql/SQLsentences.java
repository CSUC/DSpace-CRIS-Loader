package postgresql;

public class SQLsentences {
	
	//manage jdyna_prop_seq
	public static String MAXCRIS_PROPid= "SELECT greatest((SELECT max(id) FROM cris_do_prop),(SELECT max(id) FROM cris_rp_prop),(SELECT max(id) FROM cris_ou_prop),(SELECT max(id) FROM cris_pj_prop))";
	public static String newCRIS_PROPseq = "ALTER SEQUENCE jdyna_prop_seq RESTART WITH ?";	
	public static String lastCRIS_PROPseq = "SELECT last_value FROM jdyna_prop_seq";
	public static String nextvalCRIS_PROPseq= "SELECT nextval('jdyna_prop_seq')";
	
	//manage jdyna_values_seq
	public static String MAXJDYNAid = "SELECT max(id) FROM jdyna_values";
	public static String newJDYNAseq = "ALTER SEQUENCE jdyna_values_seq RESTART WITH ?";
	public static String lastJDYNAseq = "SELECT last_value FROM jdyna_values_seq";	
	public static String nextvalJDYNAseq = "SELECT nextval('jdyna_values_seq')";
	
	//get last id's delivered
	public static String CRIS_PROPid = "SELECT last_value from jdyna_prop_seq";
	public static String JDYNAid = "SELECT last_value FROM jdyna_values_seq";
	
	//public static String newUUID = "SELECT uuid_generate_v4()";
	public static String insertCRIS_ENTITY = "INSERT INTO $tableName (id,crisid,status,uuid,timestampcreated,timestamplastmodified,sourceid,sourceref $typo_id_col) VALUES (?,?,'t',?,now(),now(),?,? $typo_id_val)";
	public static String insertJDYNA_VALUE = "INSERT INTO jdyna_values (dtype,id,sortvalue,textvalue,linkdescription,linkvalue,rpvalue,dovalue,datevalue) VALUES (?,?,substring(? from 1 for 253),?,?,?,?,?,?)";
	public static String PROPid = "SELECT id FROM $pdefTable WHERE shortname=?";
	public static String insertCRIS_PROP = "INSERT INTO $tableName (id,positiondef,visibility,value_id,parent_id,typo_id) VALUES (?,?,'1',?,?,?)";
	public static String nextvalCRIS_EntityID = "SELECT nextval('$tableName')";
	public static String getNextPositiondef = "SELECT MAX(positiondef)+1 FROM $tableName WHERE parent_id=? AND typo_id=?";
	public static String updateSource = "UPDATE $tableName SET sourceid=? , sourceref=? WHERE id=?";
	public static String updateJDYNAvalue = "UPDATE jdyna_values SET (sortvalue,datevalue,textvalue,linkdescription,linkvalue)=(?,?,?,?,?) WHERE id=? AND ? $condition $conditionVar";
	
	//conditional inserts
	//public static String updateJDYNAdateIfGreater = "UPDATE jdyna_values SET datevalue=? AND sortvalue=lower(to_char(?, 'MM-DD-YYYY'T'HH24:MI:SS')) AND textvalue=to_char(?, 'MM-DD-YYYY'T'HH24:MI:SS') WHERE id=? AND datevalue < ?"; 
	
	//checkers and id resolvers querys
	public static String IsShortnameRepeateable = "SELECT repeatable FROM $pdefTable WHERE shortname=?";
	public static String getRPfield = "SELECT textvalue FROM jdyna_values WHERE id IN(SELECT value_id FROM cris_rp_prop WHERE parent_id=? AND typo_id=("+PROPid+"))";
	//public static String getRPid = "SELECT parent_id FROM cris_rp_prop WHERE value_id IN (SELECT id FROM jdyna_values WHERE dtype='text' AND lower(sortvalue)=? LIMIT 1);";
	//public static String getDOid = "SELECT parent_id FROM cris_do_prop WHERE value_id IN (SELECT id FROM jdyna_values WHERE dtype='text' AND lower(sortvalue)=? LIMIT 1);";
	public static String checkJDYNAvalueByParentIdANDshortname = "SELECT count(id) FROM jdyna_values WHERE sortvalue=lower(?) AND id IN (SELECT value_id FROM $propTable WHERE parent_id=? AND typo_id IN ("+PROPid+"))";
	public static String checkJDYNApointerByParentIdANDshortname = "SELECT count(id) FROM jdyna_values WHERE (rpvalue=? OR ouvalue=? OR projectvalue=? OR dovalue=?) AND id IN (SELECT value_id FROM $propTable WHERE parent_id=? AND typo_id IN ("+PROPid+"))";
	public static String checkJDYNAlinkvalueByParentIdANDshortname = "SELECT count(id) FROM jdyna_values WHERE lower(linkvalue)=lower(?) AND id IN (SELECT value_id FROM $propTable WHERE parent_id=? AND typo_id IN ("+PROPid+"))";
	public static String checkJDYNAdateByParentIdANDshortname = "SELECT count(id) FROM jdyna_values WHERE datevalue=? AND id IN (SELECT value_id FROM $propTable WHERE parent_id=? AND typo_id IN ("+PROPid+"))";
	public static String checkJDYNAvalue = "SELECT count(*) from jdyna_values WHERE lower(sortvalue)=?";
	public static String checkDuplicatedJDYNAvalues = "SELECT textvalue,count(*) FROM jdyna_values WHERE dtype='$dtype' AND id IN (SELECT value_id FROM $crisPropTable where typo_id IN ("+PROPid+")) GROUP BY textvalue HAVING count(*) >1;";
	public static String checkSimilarityJDYNAvalues = "SELECT sortvalue,similarity(sortvalue,'$string') AS sim FROM jdyna_values GROUP BY sortvalue HAVING similarity(sortvalue,'$string') > $minSimilarity ORDER BY sim DESC;";
	public static String getCRIS_ENTITY = "SELECT id FROM $tableName WHERE sourceid LIKE ? AND sourceref LIKE ?";
	public static String checkJDYNAvalueByShortname = "SELECT count(id) FROM jdyna_values WHERE sortvalue=lower(?) AND id IN (SELECT value_id FROM $propTable WHERE typo_id IN ("+PROPid+"))";
	public static String checkJDYNAlinkdescriptionByShortname = "SELECT count(id) FROM jdyna_values WHERE lower(linkdescription)=lower(?) AND id IN (SELECT value_id FROM $propTable WHERE typo_id IN ("+PROPid+"))";
	public static String getSourceId = "SELECT sourceid FROM $tableName WHERE id=?";
	public static String getSourceRef = "SELECT sourceref FROM $tableName WHERE id=?";
	public static String getParentIdByLinkdescriptionANDshortname = "SELECT parent_id FROM $propTable WHERE value_id IN (SELECT id FROM jdyna_values WHERE lower(linkdescription)=lower(?) AND id IN (SELECT value_id FROM $propTable WHERE typo_id IN (SELECT id FROM $pdefTable WHERE shortname=?)) LIMIT 1)";
	public static String getParentIdBySortvalueANDshortname = "SELECT parent_id FROM $propTable WHERE value_id IN (SELECT id FROM jdyna_values WHERE sortvalue=lower(?) AND id IN (SELECT value_id FROM $propTable WHERE typo_id IN (SELECT id FROM $pdefTable WHERE shortname=?)) LIMIT 1)";
	public static String getParentIdByTwoSortvaluesANDshortnames = "SELECT parent_id FROM jdyna_values INNER JOIN $propTable ON (jdyna_values.id=value_id) WHERE (typo_id IN ("+PROPid+") AND sortvalue=lower(?)) OR (typo_id IN ("+PROPid+") AND sortvalue=lower(?)) GROUP BY parent_id HAVING COUNT(parent_id) > 1";
	public static String getJDYNAidByParentIdANDshortnameANDvalue = "SELECT id FROM jdyna_values WHERE lower(sortvalue)=lower(?) AND id IN (SELECT value_id FROM $propTable WHERE parent_id=? AND typo_id IN ("+PROPid+"))";
	public static String getJDYNAidByParentIdANDshortname = "SELECT id FROM jdyna_values WHERE id IN (SELECT value_id FROM $propTable WHERE parent_id=? AND typo_id IN ("+PROPid+") LIMIT 1)";
	public static String getDCmetadataID= "SELECT metadata_field_id FROM metadatafieldregistry WHERE element=? AND qualifier=?";
	public static String getItemIDbyMetadata = "SELECT item_id FROM metadatavalue WHERE metadata_field_id IN ("+getDCmetadataID+") AND lower(text_value)=lower(?)";
	public static String getMetadataValueByItemIandMetadata = "SELECT text_value WHERE metadata_field_id IN ("+getDCmetadataID+") AND item_id=?";
	public static String getJDYNAvalueByParentIdANDshortname = "SELECT textvalue FROM jdyna_values WHERE id IN (SELECT value_id FROM $propTable WHERE parent_id=? AND typo_id IN ("+PROPid+"))";

}
