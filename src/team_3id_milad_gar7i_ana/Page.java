package team_3id_milad_gar7i_ana;
import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {
Vector<Record> records;
String Rightlinker;
final static int N=200;

public Page() {
	this.records=new Vector<Record>(1,1);
	
	
}



}
