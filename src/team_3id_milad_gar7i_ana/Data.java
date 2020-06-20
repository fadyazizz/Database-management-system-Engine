package team_3id_milad_gar7i_ana;
import java.io.Serializable;

public class Data implements Serializable {
	Object value;
	String name;
	//String dataType;
	
	
	
	public Data(Object value,String name) {
		this.name=name;
		this.value=value;
		//this.dataType=dataType;
	}
	
	public String toString() {
		return "(" + name + "," + value + ")";
	}

}
