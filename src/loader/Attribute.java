package loader;

import java.text.SimpleDateFormat;

public class Attribute {
	
	private long typo_id=-1;
	private String shortname=null;
	private String value=null;
	private String linkdescription=null;
	private String linkvalue=null;
	private String dtype=null;
	private String datevalue=null;
	private long rpvalue=-1;
	private long dovalue=-1;
	private long pjvalue=-1;
	private long ouvalue=-1;
	private String updateCondition="!=";
	private String updateVar="sortvalue";
	
	public enum Dtype{
		LINK("link"),TEXT("text"),RPPOINTER("rppointer"),DOPOINTER("dopointer"),OUPOINTER("oupointer"),PJPOINTER("pjpointer"),DATE("date");
		
		private String dtype;
		
		Dtype(String dtype){
			this.dtype=dtype;
		}
		
		public String getDtype(){
			return this.dtype;
		}		
	}
	
	public Attribute(String shortname, String value, Dtype type) throws Exception{ //constructor for non link type
		if (type.equals(Dtype.PJPOINTER) || type.equals(Dtype.OUPOINTER) || type.equals(Dtype.RPPOINTER) || type.equals(Dtype.TEXT) || type.equals(Dtype.DOPOINTER) || type.equals(Dtype.DATE)){
			this.dtype=type.getDtype();
		}
		else throw new Exception("Invalid type declaration");			
			
		if(type.equals(Dtype.DATE))	{
			this.datevalue=formatTimeStamp(value);			
		}
		this.value=value;	
		this.shortname=shortname;
	}
	
	public Attribute(String shortname,String linkdescription,String linkvalue){ //constructor for link type
		this.shortname=shortname;
		this.linkdescription=linkdescription;
		this.linkvalue=linkvalue;
		this.value=linkvalue;
		this.dtype=Dtype.LINK.getDtype();
	}	
	
	public String getDtype(){
		return this.dtype;
	}
	
	public String getValue(){
		return this.value;
	}
	
	public String getLinkdescription(){
		return this.linkdescription;
	}
	
	public String getLinkvalue(){
		return this.linkvalue;
	}
	
	public long getTypo_id(){
		return this.typo_id;
	}
	
	public void setTypo_id(long id){
		this.typo_id=id;		
	}
	
	public String getShortname(){
		return this.shortname;
	}
	
	public void setUpdateCondition(String updateCondition){
		this.updateCondition = updateCondition;
		if (updateCondition != "==" && updateCondition != "!=") this.updateVar = "datevalue";
		
	}	
	
	public String getUpdateCondition(){
		return this.updateCondition;
	}
	
	public String getUpdateVar(){
		return this.updateVar;
	}
	
	public void setValue(String value){
		this.value=value;
	}
	
	public long getPointerValue(){ //if you don't want to check dtype (lazy ones) before call the right getXXvalue, do this...		
		if(dtype.equals(Dtype.RPPOINTER.dtype)) return rpvalue;				
		else if(dtype.equals(Dtype.PJPOINTER.dtype)) return pjvalue;		
		else if(dtype.equals(Dtype.OUPOINTER.dtype)) return ouvalue;		
		else return dovalue;		
	}
	
	public long getRPvalue(){
		return this.rpvalue;
	}
	
	public void setRPvalue(long rpvalue){
		this.rpvalue=rpvalue;
	}
	
	public long getOUvalue(){
		return this.ouvalue;
	}
	
	public void setOUvalue(long ouvalue){
		this.ouvalue=ouvalue;
	}
	
	public long getPJvalue(){
		return this.pjvalue;
	}
	
	public void setPJvalue(long pjvalue){
		this.pjvalue=pjvalue;
	}
	
	public long getDOvalue(){
		return this.dovalue;
	}
	
	public void setDOvalue(long dovalue){
		this.dovalue=dovalue;
	}
	
	public String getDatevalue(){
		return this.datevalue;
	}
	
	public void setDatevalue(String datevalue){
		this.datevalue=datevalue;
	}	
	
	public static boolean checkDtype(String dtype){		
		for (Dtype dtypes : Dtype.values()) {
		        if (dtypes.name().equalsIgnoreCase(dtype)) return true;
		}
		return false;
	}

	@Override
	public String toString() {
		return "Attribute [typo_id=" + typo_id + ", shortname=" + shortname
				+ ", value=" + value + ", linkdescription=" + linkdescription
				+ ", linkvalue=" + linkvalue + ", dtype=" + dtype
				+ ", rpvalue=" + rpvalue + ", dovalue=" + dovalue + "]";
	}
	
	private String formatTimeStamp(String inputString) throws Exception{
		String inputStringModded = inputString;
		if (inputString.contains("T")) inputStringModded = inputString.substring(0, inputString.indexOf("T"));			
		
		SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-MM-dd");			
	    try{
	       format.parse(inputStringModded);	       
	       return inputStringModded;
	    }
	    catch(Exception e){	    	
			throw new Exception("Invalid timestamp date "+ inputString);	    
	    }	    
	}	

}
