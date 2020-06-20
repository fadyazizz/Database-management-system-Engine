package team_3id_milad_gar7i_ana;

import java.io.Serializable;

public class DataWithType implements Serializable {
	String colName;
	String colType;
	
	public DataWithType(String colName, String colType) {
		this.colName=colName;
		this.colType=colType;
	}
	public String toString() { 
		return "Column name: " + colName + " Column Type " + colType;
	}
}