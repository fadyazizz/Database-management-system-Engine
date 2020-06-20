package team_3id_milad_gar7i_ana;

import java.awt.Point;
import java.awt.Polygon;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Locale;
import java.util.Vector;

public class Test {
	
	
	
	
	
	public static void Insert(DBApp app,String strTableName) throws DBAppException {
		

		for (int i = 0; i < 10; i++) {
			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
			double tempid = Math.random() * 100000;
			double tempname = Math.random() * 100000;
			
			int id = (int) tempid%10000;
			int nameint = (int) tempname%10000;
			String name = "" + nameint;
			 
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String("Hoda"));
			Point p=new Point(id,3);
			Point p2=new Point(i,2);
			Point p3=new Point(+10,10);
			Point p4=new Point(20,20);
			Point p5=new Point(+10,10);
			Polygon polyy=new Polygon();
			polyy.addPoint(1, i*22);
			polyy.addPoint(i+1, i*8);
			polyy.addPoint(i+10,i*5);
			polyy.addPoint(i+20, i*90);
			polyy.addPoint(i+10+1, 10);
			htblColNameValue.put("gpa", polyy);
			System.out.println("IDValue "+id+" gpa"+polyy+" name"+name);
			app.insertIntoTable(strTableName, htblColNameValue);
//			
		}
	}
	
	public static void delete(DBApp app,String strTableName) throws DBAppException {
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		//htblColNameValue.put("id", new Integer(180));
		Polygon polyy=new Polygon();
		polyy.addPoint(2, 44);
		polyy.addPoint(2, 16);
		polyy.addPoint(+10, 2*5);
		polyy.addPoint(+20, 2*90);
		polyy.addPoint(+10+2, 10);
		htblColNameValue.put("gpa", polyy);
		//htblColNameValue.put("gpa", new Double(0.95));
		
	app.deleteFromTable(strTableName, htblColNameValue);
	}
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		DBApp app = new DBApp();
		app.init();
		String strTableName = "Student";
		
		
//		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//		htblColNameType.put("gpa", "java.awt.Polygon");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("id","java.lang.Integer" );
//		app.createTable(strTableName, "gpa", htblColNameType);
//		app.createBTreeIndex("Student", "id");
//		app.createBTreeIndex("Student", "name");
//		app.createRTreeIndex(strTableName, "gpa");
//		
//		Insert(app,strTableName);
		//delete(app, strTableName);
		
//		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//		Polygon polyy=new Polygon();
//		polyy.addPoint(2, 2);
//		polyy.addPoint(2, 2);
//		polyy.addPoint(2, 2);
//		polyy.addPoint(2, 2);
//		polyy.addPoint(2, 2);
//		htblColNameValue.put("name", "X");
//		app.updateTable(strTableName, "(1,66)(4,24)(13,15)(23,270)(14,9)", htblColNameValue);
		//pageNumber: 1 IDValue: 3 Name: Hoda Polygonn: (1,66)(4,24)(13,15)(23,270)(14,10) total number of points: 5area: 5720
		
//		Polygon polyy=new Polygon();
//		polyy.addPoint(1,88);
//		polyy.addPoint(5,32);
//		polyy.addPoint(14,20);
//		polyy.addPoint(24,360);
//		polyy.addPoint(15,10);
		//(1,88)(5,32)(14,20)(24,360)(15,10)
//		SQLTerm[] arrSQLTerms= new SQLTerm[1];
//		arrSQLTerms[0] = new SQLTerm();
//		arrSQLTerms[0].setTableName("Student");
//		arrSQLTerms[0].setColName("gpa");
//		arrSQLTerms[0].setOperator("=");
//		arrSQLTerms[0].setObjValue(polyy);
//		arrSQLTerms[1] = new SQLTerm();
//		arrSQLTerms[1].setTableName("Student");
//		arrSQLTerms[1].setColName( "id");
//		arrSQLTerms[1].setOperator("=");
//		arrSQLTerms[1].setObjValue(new Integer(1));
//		arrSQLTerms[2] = new SQLTerm();
//		arrSQLTerms[2].setTableName("Student");
//		arrSQLTerms[2].setColName("name");
//		arrSQLTerms[2].setOperator("=");
//		arrSQLTerms[2].setObjValue("Hoda");
//		String[]strarrOperators = new String[0];
//        strarrOperators[0] = "AND";
//		strarrOperators[1] = "XOR";
//		long startTime = System.currentTimeMillis();
//		Iterator<Record> resultSet = app.selectFromTable(arrSQLTerms , strarrOperators);
//		long endTime   = System.currentTimeMillis();
//		System.out.printf("Time for query = %d ms\n", endTime - startTime);
//		while(resultSet.hasNext()) {
//			System.out.println(" Iteration over set in main method ");
//			Record r = (Record)resultSet.next();
//			
//				System.out.println("SET RESULT MAIN METHOD: " + r);
//		}
//		
//		BPlusTree b = (BPlusTree) DBApp.deserialize("Student_idIndex.txt");
//		BPlusTree nam = (BPlusTree) DBApp.deserialize("Student_nameIndex.txt");
//		RTree gpa = (RTree) DBApp.deserialize("Student_gpaIndex.txt");
//		System.out.println(b);
//		System.out.println(nam);
//		System.out.println(gpa);
//		Table t = (Table) DBApp.deserialize("Student.txt");
//		for (int i = 0; i < t.NumberOfPages; i++) {
//			Page p = (Page) DBApp.deserialize("page_Student" + i + ".txt");
//			for (int j = 0; j < p.records.size(); j++) {
//				Hashtable<String, Object> h = DBApp.CovertRecordToHashTable(p.records.elementAt(j));
//				Polygonn temp=(Polygonn)h.get("gpa");
//				int area=temp.getArea();
//				System.out.println("pageNumber: " + i + " IDValue: " + h.get("id") + " Name: " + h.get("name")+" Polygonn: "+h.get("gpa")+"area: "+area);
//			}
//		}
		
	}
	
	
	
	
	
	
	

}
