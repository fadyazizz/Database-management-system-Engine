package team_3id_milad_gar7i_ana;

public class SQLTerm {
	private String _strTableName;
	private String _strColumnName;
	private String _strOperator;
	private Object _objValue;
	
	public SQLTerm() {
		
	}
	
	public void setTableName(String _strTableName) {
		this._strTableName=_strTableName;
	}
	public void setColName(String _strColumnName) {
		this._strColumnName=_strColumnName;
	}
	public void setOperator(String _strOperator) {
		this._strOperator=_strOperator;
	}
	public void setObjValue(Object _objValue) {
		this._objValue=_objValue;
	}
	
	public String getTableName() {
		return _strTableName;
	}
	public String getColName() {
		return _strColumnName;
	}
	public String getOperator() {
		return _strOperator;
	}
	public Object getObjValue() {
		return _objValue;
	}


}