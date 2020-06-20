package team_3id_milad_gar7i_ana;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable  {
	
	
	//Vector OrderVector;
	int NumberOfPages=0;
	String clustering;
	Vector<DataWithType> dataType;
	
	//Vector<String> pages;
	
	public Table(String clustering) {
		this.clustering=clustering;
		//this.pages=new Vector<String>(1,1);
		this.dataType= new Vector<DataWithType>();
		

	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
