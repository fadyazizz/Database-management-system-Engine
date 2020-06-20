package team_3id_milad_gar7i_ana;

import java.awt.Polygon;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import javax.swing.text.TabExpander;

import org.apache.commons.lang3.builder.CompareToBuilder;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import team_3id_milad_gar7i_ana.BPlusTree.LeafNode;
import team_3id_milad_gar7i_ana.BPlusTree.Node;

public class DBApp {

	static int MaximumRowsCountinPage;
	static int NodeSize;
	static Tables All_Tables;

	public void init() {
		File metadata = new File("data/metadata.csv"); // encapsulates the skeleton of each table in DB
		File tables = new File("data/Tables.txt"); // encapsulates all table names in the DB
		try {
			if (!metadata.exists()) { // create a metadata file if it doesn't already exist
				// System.out.println("MetaData file Doesn't exit so I'll create it.");
				// System.out.println("-----------------------------------------------------");
				String[] names = new String[5];
				names[0] = "Table Name";
				names[1] = "Column Name";
				names[2] = "Column Type";
				names[3] = "Key";
				names[4] = "Indexed";
				MetaDataWriter(names); // method we created to write in metadata csv file
			}

			if (!tables.exists()) { // create a tables file with an empty tables vector if it doesn't already exist
				// System.out.println("Tables file Doesn't exit so I'll create it.");
				// System.out.println("-----------------------------------------------------");
				serialize("Tables.txt", new Tables());
			}
			// Was missing!
			All_Tables = (Tables) deserialize("Tables.txt");
			// System.out.println("Number of Tables we Have in DB Engine: " +
			// All_Tables.tables.size());
			// System.out.println("-----------------------------------------------------");
			ReadFromConfigFile();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void PrinterHash(Hashtable<String, String> htblColNameType) {

		Enumeration<String> enumeration = htblColNameType.keys();
		while (enumeration.hasMoreElements()) {
			System.out.println(enumeration.nextElement());
		}

	}

	public static void PrintVector(Vector v) { // prints values in a vector
		Enumeration enu = v.elements();

		// System.out.println("The values for the vector are: ");

		// Displaying the Enumeration
		while (enu.hasMoreElements()) {
			System.out.println(enu.nextElement());
		}
	}

	public static void ReadFromConfigFile() { // reads from the DBApp.properties config file we created
		try {
			File file_config = new File("config/DBApp.properties");
			All_Tables = (Tables) deserialize("Tables.txt");

			FileReader reader = new FileReader(file_config);
			Properties props = new Properties();
			props.load(reader);

			MaximumRowsCountinPage = Integer.parseInt(props.getProperty("MaximumRowsCountinPage"));
			NodeSize = Integer.parseInt(props.getProperty("NodeSize"));

			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Vector<DataWithType> ReadMetadata(String strTableName) {
		String line = "";
		String splitBy = ",";
		Vector<DataWithType> d = new Vector<DataWithType>();
		try {

			FileReader filereader = new FileReader("data/metadata.csv");

			// create csvReader object passing
			// file reader as a parameter
			CSVReader csvReader = new CSVReader(filereader);
			String[] rowdata;

			// we are going to read data line by line
			while ((rowdata = csvReader.readNext()) != null) {
				if (rowdata[0].equals(strTableName)) {
					d.add(new DataWithType(rowdata[1], rowdata[2]));

				}
			}
			Table table = (Table) deserialize(strTableName + ".txt");
			for (int i = 0; i < d.size(); i++) {
				// System.out.println(d.elementAt(i).toString());
				table.dataType.add(d.elementAt(i));
			}
			serialize(strTableName + ".txt", table);
			return d;
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return d;
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws IOException, DBAppException {
		try {
			// System.out.println("WE ARE IN CREATE TABLE:- ");
			for (int i = 0; i < All_Tables.tables.size(); i++) {
				if (All_Tables.tables.elementAt(i).equals(strTableName)) {
					throw new DBAppException("Name used before");
				}
			}

			// was Missing: Deserialize, add, serialize!
			All_Tables = (Tables) deserialize("Tables.txt");
			All_Tables.tables.add(strTableName);
			serialize("Tables.txt", All_Tables);

			// serialize(strTableName + ".txt", tableAtHand);

			String[] rowdata = new String[5];
			rowdata[0] = strTableName;
			rowdata[4] = "false";
			Enumeration<String> enumeration = htblColNameType.keys();
			Table tableAtHand;
			String cluster = "";
			while (enumeration.hasMoreElements()) {
				rowdata[3] = "false";

				String key = enumeration.nextElement();

				if (key.equals(strClusteringKeyColumn)) {
					cluster = key;

					rowdata[3] = "true";
				}
				rowdata[1] = key;
				rowdata[2] = htblColNameType.get(key);

				// System.out.println("Column Name: " + key + " Data Type: " +
				// htblColNameType.get(key));
				// System.out.println("-----------------------------------------------------");
				MetaDataWriter(rowdata);

			}
			tableAtHand = new Table(cluster);

			serialize(strTableName + ".txt", tableAtHand);

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void serialize(String path, Object o) throws IOException {
		File file = new File("data/" + path);

		FileOutputStream fileOutputStream = null;

		fileOutputStream = new FileOutputStream(file);

		ObjectOutputStream out = new ObjectOutputStream(fileOutputStream);

		out.writeObject(o);

		out.flush();
		out.close();

	}

	public static Object deserialize(String path) throws IOException, ClassNotFoundException {
		File file = new File("data/" + path);
		FileInputStream instream = new FileInputStream(file);
		ObjectInputStream in = new ObjectInputStream(instream);
		Object temp = in.readObject();
		return temp;

	}

	// comparing if the data in table bigger than dataToBeInserted return 1
	// comparing if the data in table smaller than dataToBeInserted return -1
	// comparing if the data in table equals dataToBeInserted return 0

	public static int Compare(Object dataInTable, Object dataToBeInserted) {
		if (dataInTable instanceof Integer) {
			// System.out.println("I am being compared");
			Integer dataInTableint = (Integer) dataInTable;
			Integer dataToBeInsertedint = (Integer) dataToBeInserted;
			return dataInTableint.compareTo(dataToBeInsertedint);

		}

		if (dataInTable instanceof String) {
			String dataInTableString = (String) dataInTable;
			String dataToBeInsertedString = (String) dataToBeInserted;

			return dataInTableString.compareTo(dataToBeInsertedString);

		}

		if (dataInTable instanceof Double) {
			Double dataInTableDouble = (Double) dataInTable;
			Double dataToBeInsertedDouble = (Double) dataToBeInserted;

			return dataInTableDouble.compareTo(dataToBeInsertedDouble);

		}
		if (dataInTable instanceof Polygonn) {
			Polygonn dataInTablePolygon = (Polygonn) dataInTable;
			Polygonn dataToBeInsertedPolygon = (Polygonn) dataToBeInserted;
			return ((Polygonn) dataInTablePolygon).compareTo(dataToBeInsertedPolygon);
		}
		if (dataInTable instanceof Boolean) {
			// System.err.println(dataInTable + " -- " + dataToBeInserted);
			return ((Boolean) dataInTable).compareTo((Boolean) dataToBeInserted);
		}

		// todo another if for ploygon
		else {

			Date dataInTabledate = (Date) dataInTable;
			Date dataToBeInsertedDate = (Date) dataToBeInserted;

			return dataInTabledate.compareTo(dataToBeInsertedDate);

		}

	}

	public static boolean Equal(Object dataInTable, Object dataToBeInserted) {
		if (dataInTable instanceof Integer) {
			// System.out.println("I am being compared");
			Integer dataInTableint = (Integer) dataInTable;
			Integer dataToBeInsertedint = (Integer) dataToBeInserted;
			return dataInTableint.equals(dataToBeInsertedint);

		}

		if (dataInTable instanceof String) {
			String dataInTableString = (String) dataInTable;
			String dataToBeInsertedString = (String) dataToBeInserted;

			return dataInTableString.equals(dataToBeInsertedString);

		}

		if (dataInTable instanceof Double) {
			Double dataInTableDouble = (Double) dataInTable;
			Double dataToBeInsertedDouble = (Double) dataToBeInserted;

			return dataInTableDouble.equals(dataToBeInsertedDouble);

		}
		if (dataInTable instanceof Polygonn) {
			Polygonn dataInTablePolygon = (Polygonn) dataInTable;
			Polygonn dataToBeInsertedPolygon = (Polygonn) dataToBeInserted;
			return ((Polygonn) dataInTablePolygon).equals(dataToBeInsertedPolygon);
		}
		if (dataInTable instanceof Boolean) {
			// System.err.println(dataInTable + " -- " + dataToBeInserted);
			return ((Boolean) dataInTable).equals((Boolean) dataToBeInserted);
		}

		// todo another if for ploygon
		else {

			Date dataInTabledate = (Date) dataInTable;
			Date dataToBeInsertedDate = (Date) dataToBeInserted;

			return dataInTabledate.equals(dataToBeInsertedDate);

		}

	}

	public static Vector<String> allIndexes(String tableName) throws Exception {
		FileReader filereader;
		Vector<String> indexes = new Vector<String>(1, 1);
		try {
			filereader = new FileReader("data/metadata.csv");

			CSVReader csvReader = new CSVReader(filereader);
			String[] rowdata;

			// we are going to read data line by line
			while ((rowdata = csvReader.readNext()) != null) {
				if (rowdata[0].equals(tableName) && rowdata[4].equals("true")) {
					// System.out.println(rowdata[1]);
					indexes.add(rowdata[1]);
				}

			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// create csvReader object passing
		// file reader as a parameter

		return indexes;
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		// normal insertion in a page (fe space)
		// increase size of pages w insert in the last new inserted
		try {
			Object ClusteringValue = null;

			if (DBApp.All_Tables.tables.contains(strTableName)) {
				ReadMetadata(strTableName); // inserts colname and coltype in dataType vector in class table
				// 1- gebna el table file
				Table table = (Table) DBApp.deserialize(strTableName + ".txt");
				Record recToBeInserted = new Record();
				Data temp = null;
				// empties the hashtable into a Record vector for simplicity
				for (String key : htblColNameValue.keySet()) {
					if (checkDataType(key, htblColNameValue.get(key), table)) {

						if (htblColNameValue.get(key) instanceof Polygon) {
							System.out.println("replace value in table");
							htblColNameValue.replace(key, new Polygonn((Polygon) htblColNameValue.get(key)));
						}
						if (table.clustering.equals(key)) {
							ClusteringValue = htblColNameValue.get(key);
						}
						temp = new Data(htblColNameValue.get(key), key);
						recToBeInserted.coloumnsOfData.add(temp);

					} else {
						throw new DBAppException("Wrong Data Type");
					}
				}
				recToBeInserted.coloumnsOfData.add(new Data(new Date(), "TouchDate"));

				// 2- check if table file contains any pages
				// if it doesn't, insert a new page b el data sorted
				// Vector columns = new Vector();
				if (table.NumberOfPages == 0) {
					Page p1 = new Page();
					table.NumberOfPages++;
					Record r1 = new Record();
					for (String x : htblColNameValue.keySet()) {
						Data d = new Data(htblColNameValue.get(x), x);
						r1.coloumnsOfData.add(d);
					}

					r1.coloumnsOfData.add(new Data(new Date(), "TouchDate"));

					// table.index_of_cluster = clusterindex;
					p1.records.add(r1);
					// table.pages.add("page_" + strTableName + 0 + ".txt");

					// p1.rec.add(e);
					serialize("page_" + strTableName + 0 + ".txt", p1);
					serialize(strTableName + ".txt", table);
//this part is added in milestone 2 start..................
					try {
						Vector<String> allIndexes = DBApp.allIndexes(strTableName);
						// Vector<DataWithType> metadata=ReadMetadata(strTableName);

						for (int i = 0; i < allIndexes.size(); i++) {
							// TODO changes were made here
							Object o = (Object) DBApp.deserialize(strTableName + "_" + allIndexes.get(i) + "Index.txt");
							if (o instanceof BPlusTree) {
								BPlusTree index = (BPlusTree) o;
								index.insert((Comparable) htblColNameValue.get(allIndexes.get(i)),
										(Object) "page_Student0.txt");
							} else {
								RTree index = (RTree) o;
								Polygonn temppoly = (Polygonn) htblColNameValue.get(allIndexes.get(i));
								index.insert((Comparable) temppoly.getArea(), (Object) "page_Student0.txt");
							}
						}

					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

				else {

					int i = 0;
					int j = 0;
					int k = 0;
					boolean PositionFound = false;

//this part was added in milestone 2 start..................

					Vector<String> allIndexes = null;

					try {
						allIndexes = DBApp.allIndexes(strTableName);
						String ClusteringCol = table.clustering;
						if (allIndexes.contains(ClusteringCol)) {
							if (htblColNameValue.get(ClusteringCol) instanceof Polygonn) {
								RTree index = (RTree) DBApp
										.deserialize(strTableName + "_" + ClusteringCol + "Index.txt");
								Polygonn value = (Polygonn) htblColNameValue.get(ClusteringCol);

								Vector<String> VectorOfPages = index.search(value.getArea());
								if (VectorOfPages == null) {
									RTree.LeafNode TheNode = (RTree.LeafNode) DBApp.deserialize(RTree.lastSearchedPage);
									int posOfVector = Collections.binarySearch(TheNode.keys, value.getArea());
									posOfVector = (-posOfVector) - 2;
									if (posOfVector < 0) {
										posOfVector = 0;
									}
									if (posOfVector == 0) {
										VectorOfPages = (Vector<String>) TheNode.values.get(posOfVector);
										i = DBApp.getPageNumber(VectorOfPages.elementAt(0));
									} else {
										// changed----------------------------------------------------------------
										VectorOfPages = (Vector<String>) TheNode.values.get(posOfVector);
										i = DBApp.getPageNumber(VectorOfPages.elementAt(0));
									}
								} else {
									i = DBApp.getPageNumber(VectorOfPages.elementAt(0));
								}
//end
							} else {
								BPlusTree index = (BPlusTree) DBApp
										.deserialize(strTableName + "_" + ClusteringCol + "Index.txt");
								Comparable value = (Comparable) htblColNameValue.get(ClusteringCol);
								Vector<String> VectorOfPages = index.search(value);
								if (VectorOfPages == null) {
									LeafNode TheNode = (LeafNode) DBApp.deserialize(BPlusTree.lastSearchedPage);
									int posOfVector = Collections.binarySearch(TheNode.keys,
											htblColNameValue.get(table.clustering));
									posOfVector = (-posOfVector) - 2;
									if (posOfVector < 0) {
										posOfVector = 0;
									}
									if (posOfVector == 0) {
										VectorOfPages = (Vector<String>) TheNode.values.get(posOfVector);
										i = DBApp.getPageNumber(VectorOfPages.elementAt(0));
									} else {
										// changed----------------------------------------------------------------
										VectorOfPages = (Vector<String>) TheNode.values.get(posOfVector);
										i = DBApp.getPageNumber(VectorOfPages.elementAt(0));
									}
								} else {
									i = DBApp.getPageNumber(VectorOfPages.elementAt(0));
								}
								// end

							}
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

// this part was added in milestone 2 end......................
					System.out.println(i + "  " + table.NumberOfPages);
					// we are getting page by page
					boolean eq = false;
					int lastDuplicatepos = 0;

					for (; i < table.NumberOfPages; i++) {

						Page page = (Page) DBApp.deserialize("page_" + strTableName + i + ".txt");
						Record First = page.records.elementAt(0);
						Record Last = page.records.elementAt(page.records.size() - 1);
						Object ValueInFirst = null;
						Object ValueInLast = null;
						// 7awelhom hashtable
						for (int ClusterPos = 0; ClusterPos < First.coloumnsOfData.size(); ClusterPos++) {
							if (First.coloumnsOfData.elementAt(ClusterPos).name.equals(table.clustering)) {
								ValueInFirst = First.coloumnsOfData.elementAt(ClusterPos).value;
								break;
							}
						}
						for (int ClusterPos = 0; ClusterPos < Last.coloumnsOfData.size(); ClusterPos++) {
							if (Last.coloumnsOfData.elementAt(ClusterPos).name.equals(table.clustering)) {
								ValueInLast = Last.coloumnsOfData.elementAt(ClusterPos).value;
								break;
							}
						}
						int CompareFirst = Compare(ValueInFirst, ClusteringValue);
						int CompareLast = Compare(ValueInLast, ClusteringValue);
						if (CompareFirst >= 0 || CompareLast >= 0) {
							// for (j = 0; j < page.records.size(); j++) {
							int lastocc = lastOccurrenceIndex(0, page.records.size() - 1, ClusteringValue, page,
									table.clustering);
							// Object clusteringdata = rec.get(table.clustering);
							int comp = leastgreaterIndex(0, page.records.size() - 1, ClusteringValue, page,
									table.clustering);

							if (lastocc >= 0 && page.records.size() < MaximumRowsCountinPage) {
								eq = true;
								lastDuplicatepos = lastocc;
							}
							if (comp >= 0) {
								// System.out.println("I am here in comp"+comp);
								// eq=false;
								PositionFound = true;
								page = null;
								// record = null;
								// data = null;
								InsertHelper(i, comp, strTableName, PositionFound, recToBeInserted, htblColNameValue,
										allIndexes);
								return;
							}

							// }
						}
						if (eq) {
							InsertHelper(i, lastDuplicatepos, strTableName, true, recToBeInserted, htblColNameValue,
									allIndexes);
							return;
						}

					}

					InsertHelper(table.NumberOfPages - 1, 0, strTableName, false, recToBeInserted, htblColNameValue,
							allIndexes);

				}

			}

			else {
				throw new DBAppException("Table not Found");
			}

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static int getPageNumber(String nameOfLastPage) {
		String pageNum = "";
		for (int i = nameOfLastPage.length() - 1; i >= 0; i--) {
			if (nameOfLastPage.charAt(i) < 48 || nameOfLastPage.charAt(i) > 57) {
				continue;
			}
			pageNum = "" + nameOfLastPage.charAt(i) + pageNum;
		}
		return Integer.parseInt(pageNum);

	}

	public static void DeleteLastValueFromIndexes(String pageName, String tableName, Vector<String> allIndexes,
			int pageIndex) {
		try {
			Page p = (Page) DBApp.deserialize(pageName);
			Hashtable<String, Object> hash = DBApp.CovertRecordToHashTable(p.records.elementAt(p.records.size() - 1));
			for (int i = 0; i < allIndexes.size(); i++) {
				String IndexName = allIndexes.get(i);
				Object o = deserialize(tableName + "_" + IndexName + "Index.txt");
				if (o instanceof BPlusTree) {
					BPlusTree b = (BPlusTree) o;
					b.PageDeleter(pageName, (Comparable) hash.get(IndexName));
					// int tmp=pageIndex+1;
				} else {
					RTree b = (RTree) o;
					Polygonn pp = (Polygonn) hash.get(IndexName);
					int area = pp.getArea();
					b.PageDeleter(pageName, (Comparable) area);
				}
				// b.insert((Comparable)hash.get(IndexName),"page_"+tableName+tmp+".txt" );
			}
		}

		catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void insertIntoIndexes(Vector<String> allIndexes, Hashtable<String, Object> htblColNameValue,
			String pageName, String tableName) {
		try {

			for (int i = 0; i < allIndexes.size(); i++) {
				if (htblColNameValue.get(allIndexes.get(i)) instanceof Polygonn) {
					String IndexName = allIndexes.get(i);
					RTree b = (RTree) deserialize(tableName + "_" + IndexName + "Index.txt");

					Polygonn p = (Polygonn) htblColNameValue.get(IndexName);
					int area = p.getArea();
					b.insert((Comparable) area, pageName);
				} else {
					String IndexName = allIndexes.get(i);
					BPlusTree b = (BPlusTree) deserialize(tableName + "_" + IndexName + "Index.txt");
					b.insert((Comparable) htblColNameValue.get(IndexName), pageName);
				}
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Hashtable<String, Object> CovertRecordToHashTable(Record record) {
		Hashtable<String, Object> returned = new Hashtable<String, Object>();
		for (int i = 0; i < record.coloumnsOfData.size(); i++) {
			Data d = record.coloumnsOfData.elementAt(i);
			returned.put(d.name, d.value);
		}
		return returned;
	}

	public static void InsertHelper(int pageIndex, int RecordIndex, String tableName, boolean Found, Record newRecord,
			Hashtable<String, Object> htblColNameValue, Vector<String> allIndexes)
			throws ClassNotFoundException, IOException {

		Page pageAti = (Page) deserialize("Page_" + tableName + pageIndex + ".txt");
		if (!Found) { // mafeesh 7aga akbar mn el value el I want to insert
			if (pageAti.records.size() < MaximumRowsCountinPage) { // if page at hand not full aslun, just insert feeha
				pageAti.records.add(newRecord);
				insertIntoIndexes(allIndexes, htblColNameValue, "page_" + tableName + pageIndex + ".txt", tableName);
				serialize("page_" + tableName + pageIndex + ".txt", pageAti);

			} else { // if page at hand full, create a new page and insert feeha
				Page newPage = new Page();
				newPage.records.add(newRecord);
				Table table = (Table) deserialize(tableName + ".txt");
				table.NumberOfPages++;
				serialize("page_" + tableName + (table.NumberOfPages - 1) + ".txt", newPage);
				insertIntoIndexes(allIndexes, htblColNameValue,
						"page_" + tableName + (table.NumberOfPages - 1) + ".txt", tableName);
				serialize(tableName + ".txt", table);
			}
		} else {
			// zabat el cases!!!!!

			if (pageIndex != 0 && RecordIndex == 0) {
				Page p = (Page) deserialize("page_" + tableName + (pageIndex - 1) + ".txt");
				if (p.records.size() < MaximumRowsCountinPage) {
					p.records.add(newRecord);
					serialize("page_" + tableName + (pageIndex - 1) + ".txt", p);
					insertIntoIndexes(allIndexes, htblColNameValue, "page_" + tableName + (pageIndex - 1) + ".txt",
							tableName);

					return;
				}
			}

			boolean created = CreatePageOrNot(pageIndex, tableName);
			if (created) {
				pageAti = null;
				switchCreated(pageIndex, RecordIndex, tableName, newRecord, htblColNameValue, allIndexes);
				// DBApp.WasItFull = true;
			} else {
				Page p = (Page) deserialize("page_" + tableName + pageIndex + ".txt");
				if (p.records.size() < DBApp.MaximumRowsCountinPage) {
					// DBApp.WasItFull = false;
				} else {
					// DBApp.WasItFull = true;
				}
				switchNotCreated(pageIndex, RecordIndex, tableName, newRecord, htblColNameValue, allIndexes);

			}

		}

	}

	public static void switchNotCreated(int pageIndex, int RecordIndex, String tableName, Record newRecord,
			Hashtable<String, Object> htblColNameValue, Vector<String> allIndexes)
			throws ClassNotFoundException, IOException {
		// System.out.println("-----------------SwitchNot--------------------");
		// System.out.println("------------------------------SwitchNotCreated--------------------------------------------
		// "
		// + htblColNameValue.get("id") + " " + htblColNameValue.get("name"));
		Table table = (Table) deserialize(tableName + ".txt");
		Page p = (Page) deserialize("page_" + tableName + pageIndex + ".txt");

		Record Last = null;

		if (p.records.size() < MaximumRowsCountinPage) {
			insertIntoIndexes(allIndexes, htblColNameValue, "page_" + tableName + pageIndex + ".txt", tableName);
			p.records.add(RecordIndex, newRecord);
			// added in ms2 start
			// DBApp.PageInsertionHappendTo = "page_" + tableName + pageIndex + ".txt";
			// added in ms2 end
			serialize("page_" + tableName + pageIndex + ".txt", p);
			return;
		}
		// System.out.println("Enclosure-----------------------------------");
		DeleteLastValueFromIndexes("page_" + tableName + pageIndex + ".txt", tableName, allIndexes, pageIndex);
		// System.out.println("Enclosure-----------------------------------");
		insertIntoIndexes(allIndexes, htblColNameValue, "page_" + tableName + pageIndex + ".txt", tableName);
		p.records.add(RecordIndex, newRecord);
		Last = p.records.remove(p.records.size() - 1);
		// added in ms2 start
		// DBApp.PageInsertionHappendTo = "page_" + tableName + pageIndex + ".txt";
		// added in ms2 end
		serialize("page_" + tableName + pageIndex + ".txt", p);
		for (int i = pageIndex + 1; i < table.NumberOfPages; i++) {
			Page pTemp = (Page) deserialize("page_" + tableName + i + ".txt");
			Hashtable<String, Object> hash = CovertRecordToHashTable(Last);
			if (pTemp.records.size() < MaximumRowsCountinPage) {

				insertIntoIndexes(allIndexes, hash, "page_" + tableName + i + ".txt", tableName);
				pTemp.records.add(0, Last);
				serialize("page_" + tableName + i + ".txt", pTemp);
				return;
			}
			DeleteLastValueFromIndexes("page_" + tableName + i + ".txt", tableName, allIndexes, i);
			insertIntoIndexes(allIndexes, hash, "page_" + tableName + i + ".txt", tableName);
			pTemp.records.add(0, Last);
			Last = pTemp.records.remove(pTemp.records.size() - 1);
			serialize("page_" + tableName + i + ".txt", pTemp);

		}

	}

	public static void switchCreated(int pageIndex, int RecordIndex, String tableName, Record newRecord,
			Hashtable<String, Object> htblColNameValue, Vector<String> allIndexes)
			throws ClassNotFoundException, IOException {
		System.out.println("------------------------------SwitchCreated-------------------------------------------- "
				+ htblColNameValue.get("id") + " " + htblColNameValue.get("name"));
		// System.out.println("ana fe switchCreated " + "RecordIndexIs " + RecordIndex +
		// " PageIndex " + pageIndex
		// + " Record data " + newRecord.coloumnsOfData.elementAt(2).value);
		Table table = (Table) deserialize(tableName + ".txt");
		Page p = (Page) deserialize("page_" + tableName + pageIndex + ".txt");

		Record Last = null;
		DeleteLastValueFromIndexes("page_" + tableName + pageIndex + ".txt", tableName, allIndexes, pageIndex);
		insertIntoIndexes(allIndexes, htblColNameValue, "page_" + tableName + pageIndex + ".txt", tableName);
		p.records.add(RecordIndex, newRecord);
		Last = p.records.remove(p.records.size() - 1);
		// added in ms2 start
		// DBApp.PageInsertionHappendTo = "page_" + tableName + pageIndex + ".txt";
		// added in ms2 end
		serialize("page_" + tableName + pageIndex + ".txt", p);
		for (int i = pageIndex + 1; i < table.NumberOfPages - 1; i++) {
			Hashtable<String, Object> hash = CovertRecordToHashTable(Last);
			Page pTemp = (Page) deserialize("page_" + tableName + i + ".txt");
			DeleteLastValueFromIndexes("page_" + tableName + i + ".txt", tableName, allIndexes, i);
			insertIntoIndexes(allIndexes, hash, "page_" + tableName + i + ".txt", tableName);
			pTemp.records.add(0, Last);
			Last = pTemp.records.remove(pTemp.records.size() - 1);
			serialize("page_" + tableName + i + ".txt", pTemp);
		}

		Page LastPage = (Page) deserialize("page_" + tableName + (table.NumberOfPages - 1) + ".txt");
		insertIntoIndexes(allIndexes, CovertRecordToHashTable(Last),
				"page_" + tableName + (table.NumberOfPages - 1) + ".txt", tableName);
		LastPage.records.add(Last);
		serialize("page_" + tableName + (table.NumberOfPages - 1) + ".txt", LastPage);

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		// todo if given the clustering coloumn
		try {
			int i = 0;
			int rec = 0;
			boolean breaker = true;
			Hashtable<String, Object> forchecking = new Hashtable<String, Object>();

			if (DBApp.All_Tables.tables.contains(strTableName)) {
				// System.out.println("-----------ana 3adet name check---------------");
				ReadMetadata(strTableName); // inserts colname and coltype in dataType vector in class table

				Table table = (Table) deserialize(strTableName + ".txt");
				for (String x : htblColNameValue.keySet()) {
					forchecking.put(x, htblColNameValue.get(x));
					if (htblColNameValue.get(x) instanceof Polygon) {
						System.out.println("Hashtableeee " + htblColNameValue);
						htblColNameValue.replace(x, new Polygonn((Polygon) htblColNameValue.get(x)));
						System.out.println("Hashtableeee " + htblColNameValue);
					}
				}
				int countIndexes = 0;
				Vector<String> indexes = allIndexes(strTableName);
				for (int ok = 0; ok < indexes.size(); ok++) {
					if (htblColNameValue.containsKey(indexes.elementAt(ok))) {
						countIndexes++;
					}
				}
				if (countIndexes > 0) {
					System.out.println(indexes);
					deleteFromTableUsingIndexes(strTableName, htblColNameValue, indexes);
					return;
				}

				System.err.println("delete with no index");

				for (i = 0; i < table.NumberOfPages; i++) {
					// table = (Table) deserialize(strTableName + ".txt");
					Page p = (Page) deserialize("page_" + strTableName + i + ".txt");
					int firstocc = 0;
					int lastocc = p.records.size();
					if (htblColNameValue.containsKey(table.clustering)) {
						System.err.println("delete with no index but with clustering val");
						Hashtable<String, Object> first = CovertRecordToHashTable(p.records.elementAt(0));
						Hashtable<String, Object> Last = CovertRecordToHashTable(
								p.records.elementAt(p.records.size() - 1));
						Object clusterdata = htblColNameValue.get(table.clustering);
						int comp1 = Compare(first.get(table.clustering), clusterdata);
						int comp2 = Compare(Last.get(table.clustering), clusterdata);
						// System.out.println("ana hena yastaaaaaa "+i);
						if (comp1 > 0 || comp2 < 0) {
							continue;
						}

						// System.out.println("ana hena yastaaaaaa"+i);
						firstocc = DBApp.firstOccurenceIndex(0, p.records.size() - 1,
								htblColNameValue.get(table.clustering), p, table.clustering);
						lastocc = DBApp.lastOccurrenceIndex(0, p.records.size() - 1,
								htblColNameValue.get(table.clustering), p, table.clustering);
						lastocc++;
						if (firstocc == -1) {
							System.out.println("Yasta el value mesh magouda yasta");
							break;
						}

					}

					int count = 0;
					for (rec = firstocc; rec < lastocc - count; rec++) {
						// optimization for delete written in ms2 ---- start-----

						// end of optimization

						Record record = p.records.elementAt(rec);
						breaker = true;
						// changed in ms2 start
						Hashtable<String, Object> hash = CovertRecordToHashTable(record);
//						end		
						// new Hashtable<String, Object>();
//						for (int dataIndex = 0; dataIndex < record.coloumnsOfData.size(); dataIndex++) {
//							hash.put(record.coloumnsOfData.elementAt(dataIndex).name,
//									record.coloumnsOfData.elementAt(dataIndex).value);
//
//						}

						// System.out.println("-----------------------------------------------------------");
						// benkaren kol el data
						for (String x : htblColNameValue.keySet()) {
							// System.out.println(" ANA HENA!" + x + " " + htblColNameValue.get(x));
							if (checkDataType(x, forchecking.get(x), table)) {

								Object dataInTable;
								Object DataGiven;
								DataGiven = htblColNameValue.get(x);
								dataInTable = hash.get(x);
								System.out.println("Data Given" + DataGiven + "Data in table" + dataInTable);
								if (!Equal(dataInTable, DataGiven)) {

									breaker = false;
									break;
								}
							} else {
								throw new DBAppException("Wrong Data Type");
							}

						}

						if (breaker) {
							count++;
							if (p.records.size() == 1) {
								// System.out.println("ANA HENA YA WELAD");
								Record recordd = p.records.remove(rec);
								// PagePrinter(p);
								File f = new File("data/" + "page_" + strTableName + i + ".txt");
								// System.out.println(f.exists() + " HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH ");
								f.delete();
								for (int in = i + 1; in < table.NumberOfPages; in++) {
									Page temp = (Page) deserialize("page_" + strTableName + (in) + ".txt");
									System.out.println(in + " LOLLL " + (in - 1));
									serialize("page_" + strTableName + (in - 1) + ".txt", temp);
								}
								i--;

								table.NumberOfPages--;
								File lastt = new File("data/page_" + strTableName + (table.NumberOfPages) + ".txt");
								lastt.delete();
								serialize(strTableName + ".txt", table);
								deleteIndexOccurence(i + 1, strTableName, indexes, CovertRecordToHashTable(recordd));
								changeNameInLeafs(i + 2, strTableName, indexes);

							} else {
								// System.out.println("--------ana fe breaker else--------");
								Record recordd = p.records.remove(rec);
								rec--;
								serialize("page_" + strTableName + i + ".txt", p);
								serialize(strTableName + ".txt", table);
								deleteIndexOccurence(i + 1, strTableName, indexes, CovertRecordToHashTable(recordd));
							}

						}

					}
					// i--;

				}

				serialize(strTableName + ".txt", table);
			}

		} catch (DBAppException d) {
			// TODO Auto-generated catch block
//			System.out.println();
//			d.printStackTrace();
			throw new DBAppException("WRONG DATA TYPE !!!!!!!!!!!!!!!!");

		} catch (Exception e) {
			System.out.println(e.getStackTrace());
		}

	}

	private static void deleteFromTableUsingIndexes(String strTableName, Hashtable<String, Object> htblColNameValue,
			Vector<String> indexes) throws DBAppException {
		System.err.println("delete with index");
		try {
			Table table = (Table) deserialize(strTableName + ".txt");
			Vector<String> indexIntersection = new Vector<String>(1, 1);
			Vector<Vector<String>> allvaluespages = new Vector<Vector<String>>(1, 1);
			Hashtable<String, Object> forchecking = new Hashtable<String, Object>();
			for (String x : htblColNameValue.keySet()) {
				if (htblColNameValue.get(x) instanceof Polygonn) {
					forchecking.put(x, new Polygon());
				} else {
					forchecking.put(x, htblColNameValue.get(x));
				}
				if (indexes.contains(x)) {
					if (htblColNameValue.get(x) instanceof Polygonn) {
						RTree index = (RTree) deserialize(strTableName + "_" + x + "Index.txt");
						Polygonn poly = (Polygonn) htblColNameValue.get(x);
						int area = poly.getArea();
						System.out.println(area);
						Vector<String> valuesPages = index.search((Comparable) area);
						allvaluespages.add(valuesPages);
					} else {
						System.out.println("in else---=====--=-=-=++++");
						BPlusTree index = (BPlusTree) deserialize(strTableName + "_" + x + "Index.txt");
						Vector<String> valuesPages = index.search((Comparable) htblColNameValue.get(x));
						allvaluespages.add(valuesPages);

					}
				}
			}
			System.out.println(allvaluespages);
			int index = 0;
			int max = -1;
			for (int i = 0; i < allvaluespages.size(); i++) {

				if (allvaluespages.elementAt(i).size() > max) {
					index = i;
					max = allvaluespages.elementAt(i).size();
				}
			}
			indexIntersection = allvaluespages.remove(index);
			for (int i = 0; i < allvaluespages.size(); i++) {
				Vector<String> tointersect = allvaluespages.elementAt(i);

				for (int j = 0; j < indexIntersection.size(); j++) {
					if (!tointersect.contains(indexIntersection.elementAt(j))) {
						indexIntersection.remove(j);
						j--;
					}
				}
			}

			Vector<String> tempvec = new Vector<String>(1, 1);
			for (int i = 0; i < indexIntersection.size(); i++) {
				if (!tempvec.contains(indexIntersection.elementAt(i))) {
					tempvec.add(indexIntersection.elementAt(i));
				}
			}
			indexIntersection = tempvec;

			System.out.println("intersection " + indexIntersection);
			int ind = 0;
			while (!indexIntersection.isEmpty()) {

				String name = indexIntersection.remove(0);
				Page page = (Page) deserialize(name);
				int pagenum = getPageNumber(name);
				int firstocc = 0;
				int lastocc = page.records.size();
				if (htblColNameValue.containsKey(table.clustering)) {
					System.err.println("delete with index but with clustering val");
					Object clusteringValue = htblColNameValue.get(table.clustering);
					firstocc = firstOccurenceIndex(0, page.records.size() - 1, clusteringValue, page, table.clustering);
					lastocc = lastOccurrenceIndex(0, page.records.size() - 1, clusteringValue, page, table.clustering);
					lastocc++;
					if (firstocc == -1) {
						continue;
					}
				}
				int count = 0;
				int rec;
				boolean breaker;
				for (rec = firstocc; rec < lastocc - count; rec++) {
					Record record = page.records.elementAt(rec);
					breaker = true;
					Hashtable<String, Object> hash = CovertRecordToHashTable(record);
					for (String x : htblColNameValue.keySet()) {
						if (checkDataType(x, forchecking.get(x), table)) {
							Object dataInTable;
							Object DataGiven;
							DataGiven = htblColNameValue.get(x);
							dataInTable = hash.get(x);
							System.out.println("Data Given" + DataGiven + "Data in table" + dataInTable);
							if (!Equal(dataInTable, DataGiven)) {

								breaker = false;
								break;
							}
						} else {
							throw new DBAppException("Wrong Data Type");
						}

					}

					if (breaker) {
						count++;
						if (page.records.size() == 1) {
							Record recordd = page.records.remove(rec);
							File f = new File("data/" + "page_" + strTableName + pagenum + ".txt");
							f.delete();
							for (int in = pagenum + 1; in < table.NumberOfPages; in++) {
								Page temp = (Page) deserialize("page_" + strTableName + (in) + ".txt");
								System.out.println(in + " LOLLL " + (in - 1));
								serialize("page_" + strTableName + (in - 1) + ".txt", temp);
							}

							table.NumberOfPages--;
							File lastt = new File("data/page_" + strTableName + (table.NumberOfPages) + ".txt");
							lastt.delete();
							serialize(strTableName + ".txt", table);
							deleteIndexOccurence(pagenum, strTableName, indexes, CovertRecordToHashTable(recordd));
							changeName(indexIntersection, strTableName);
							changeNameInLeafs(pagenum + 1, strTableName, indexes);

						} else {
							// System.out.println("--------ana fe breaker else--------");

							Record recordd = page.records.remove(rec);
							rec--;
							serialize("page_" + strTableName + pagenum + ".txt", page);
							serialize(strTableName + ".txt", table);
							deleteIndexOccurence(pagenum, strTableName, indexes, CovertRecordToHashTable(recordd));
						}

					}

				}

			}

			serialize(strTableName + ".txt", table);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Vector<String> getAllLeafs(String tableName, String IndexName) {
		try {
			Vector<String> v = (Vector<String>) deserialize("allNodes" + IndexName + "_" + tableName + ".txt");
			Vector<String> result = new Vector<String>(1, 1);
			for (int i = 0; i < v.size(); i++) {
				Node n = (Node) deserialize(v.elementAt(i));
				if (n instanceof LeafNode) {
					result.add(n.path);
				}
			}

			return result;

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	private static void changeNameInLeafs(int nextpages, String strTableName, Vector<String> Indexes) {
		try {

			for (int coun = 0; coun < Indexes.size(); coun++) {
				Object o = deserialize(strTableName + "_" + Indexes.elementAt(coun) + "Index.txt");
				if (o instanceof BPlusTree) {
					BPlusTree index = (BPlusTree) o;
					Vector<String> allLeafs = getAllLeafs(strTableName, Indexes.elementAt(coun));
					// System.out.println("ChangeName "+allLeafs);
					for (int i = 0; i < allLeafs.size(); i++) {

						LeafNode leaf = (LeafNode) deserialize(allLeafs.elementAt(i));

						for (int j = 0; j < leaf.values.size(); j++) {
							Vector<String> v = (Vector<String>) leaf.values.get(j);
							for (int k = 0; k < v.size(); k++) {
								String name = v.elementAt(k);
								int num = getPageNumber(name);
								if (nextpages > num) {
									continue;
								}
								v.set(k, "page_" + strTableName + (num - 1) + ".txt");

							}
						}

						serialize(allLeafs.elementAt(i), leaf);
						// serialize("allLeafs"+Indexes.elementAt(coun)+"_"+strTableName+".txt",
						// allLeafs);

					}

				} else {
					// TODO do the same for RTrees
					RTree index = (RTree) o;
					Vector<String> allLeafs = getAllLeafs(strTableName, Indexes.elementAt(coun));
					// System.out.println("ChangeName "+allLeafs);
					for (int i = 0; i < allLeafs.size(); i++) {

						RTree.LeafNode leaf = (RTree.LeafNode) deserialize(allLeafs.elementAt(i));

						for (int j = 0; j < leaf.values.size(); j++) {
							Vector<String> v = (Vector<String>) leaf.values.get(j);
							for (int k = 0; k < v.size(); k++) {
								String name = v.elementAt(k);
								int num = getPageNumber(name);
								if (nextpages > num) {
									continue;
								}
								v.set(k, "page_" + strTableName + (num - 1) + ".txt");

							}
						}

						serialize(allLeafs.elementAt(i), leaf);
						// serialize("allLeafs"+Indexes.elementAt(coun)+"_"+strTableName+".txt",
						// allLeafs);

					}

				}

				serialize(strTableName + "_" + Indexes.elementAt(coun) + ".txt", o);

			}

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void deleteIndexOccurence(int pagenum, String tableName, Vector<String> indexes,
			Hashtable<String, Object> htblColNameValue) {
		try {

			String pageName = "page_" + tableName + pagenum + ".txt";
			for (int i = 0; i < indexes.size(); i++) {
				if (htblColNameValue.get(indexes.elementAt(i)) instanceof Polygonn) {
					RTree index = (RTree) deserialize(tableName + "_" + indexes.elementAt(i) + "Index.txt");
					Polygonn pol = (Polygonn) htblColNameValue.get(indexes.elementAt(i));
					int area = pol.getArea();
					index.search((Comparable) area);
					RTree.LeafNode lf = (RTree.LeafNode) deserialize(RTree.lastSearchedPage);
					lf.PageDeleter(pageName, (Comparable) area);

					Vector<String> val = index.search((Comparable) area);
					System.out.println("Delete Index Occurence" + val);

					if (val.size() == 0) {
						// index.delete((Comparable)htblColNameValue.get(indexes.elementAt(i)));
						index.checker(tableName, indexes.elementAt(i));
					}
					System.out.println("Delete index occ+/n " + index);
					serialize(tableName + "_" + indexes.elementAt(i) + "Index.txt", index);
				} else {
					BPlusTree index = (BPlusTree) deserialize(tableName + "_" + indexes.elementAt(i) + "Index.txt");

					index.search((Comparable) htblColNameValue.get(indexes.elementAt(i)));
					LeafNode lf = (LeafNode) deserialize(BPlusTree.lastSearchedPage);
					lf.PageDeleter(pageName, (Comparable) htblColNameValue.get(indexes.elementAt(i)));

					Vector<String> val = index.search((Comparable) htblColNameValue.get(indexes.elementAt(i)));
					System.out.println("Delete Index Occurence" + val);

					if (val.size() == 0) {
						// index.delete((Comparable)htblColNameValue.get(indexes.elementAt(i)));
						index.checker(tableName, indexes.elementAt(i));
					}
					System.out.println("Delete index occ+/n " + index);
					serialize(tableName + "_" + indexes.elementAt(i) + "Index.txt", index);
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void changeName(Vector<String> indexIntersection, String strTableName) {
		System.out.println("---Change Name " + indexIntersection);
		for (int i = 0; i < indexIntersection.size(); i++) {

			String name = indexIntersection.elementAt(i);

			int num = getPageNumber(name);
			indexIntersection.set(i, "page_" + strTableName + (num - 1) + ".txt");
		}
		System.out.println("---Change Name2 " + indexIntersection);
	}

	public static boolean CreatePageOrNot(int PageIndex, String tableName) throws ClassNotFoundException, IOException {
		Table table = (Table) deserialize(tableName + ".txt");
		int i = 0;
		for (i = PageIndex; i < table.NumberOfPages; i++) {
			Page p = (Page) deserialize("page_" + tableName + i + ".txt");
			if (p.records.size() < MaximumRowsCountinPage) {
				return false;
			}
		}

		Page newpa = new Page();
		serialize("page_" + tableName + table.NumberOfPages + ".txt", newpa);
		table.NumberOfPages++;
		// table.pages.add("page_" + tableName + (table.NumberOfPages - 1) + ".txt");
		serialize(tableName + ".txt", table);
		table = null;
		newpa = null;

		return true;

	}

	public static void MetaDataWriter(String[] rowdata) throws IOException {

		File file = new File("data/metadata.csv");
		FileWriter outputfile = new FileWriter(file, true); // true was added to ensure we append to file
		CSVWriter writer = new CSVWriter(outputfile);
		writer.writeNext(rowdata);
		// outputfile.append(""); makansh leeh lazma el line da
		writer.flush();
		writer.close();

	}

	public static boolean checkDataType(String colName, Object colData, Table t) {

		for (int i = 0; i < t.dataType.size(); i++) {
			if (t.dataType.elementAt(i).colName.equals(colName)) {
				String type = "class " + t.dataType.elementAt(i).colType.toLowerCase();
				String colType = colData.getClass().toString().toLowerCase();
				// System.out.println("Data Type in File " + type + " Data Type of Object " +
				// colType);
				if (colType.equals(type)) {
					return true;
				}
			}
		}
		return false;

	}

	public static void PagePrinter(Page p1) {
		for (int i = 0; i < p1.records.size(); i++) {
			Record r = p1.records.elementAt(i);
			for (int j = 0; j < r.coloumnsOfData.size(); j++) {
				Data d = r.coloumnsOfData.elementAt(j);
				System.out.println("DATA #" + i + " NAME : " + d.name + " VALUE : " + d.value);
			}
		}
	}

	public static void MetaDataUpdator(String tableName, String colName) throws IOException {
		File file = new File("data/metadata.csv");
		FileReader filereader = new FileReader("data/metadata.csv");
		CSVReader csvReader = new CSVReader(filereader);
		List<String[]> csvBody = csvReader.readAll();

		// create csvReader object passing
		// file reader as a parameter

		int counter = 0;
		file.delete();
		int rowIndex = 0;

		for (int i = 0; i < csvBody.size(); i++) {
			String[] arr = csvBody.get(i);
			if (arr[0].equals(tableName) && arr[1].equals(colName)) {
				rowIndex = i;
			}
		}
		FileWriter outputfile = new FileWriter(file); // true was added to ensure we append to file
		CSVWriter writer = new CSVWriter(outputfile);

		// get CSV row column and replace with by using row and column
		csvBody.get(rowIndex)[4] = "true";
		csvReader.close();

		// Write to CSV file which is open

		writer.writeAll(csvBody);
		writer.flush();
		writer.close();

	}

	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException {
		try {
			Vector<DataWithType> metadata = DBApp.ReadMetadata(strTableName);
			String colType = null;
			int i;
			for (i = 0; i < metadata.size(); i++) {
				if (strColName.equals(metadata.get(i).colName)) {
					colType = metadata.get(i).colType.toLowerCase();
					break;
				}

			}
			Vector<String> indexes = DBApp.allIndexes(strTableName);
			if (indexes.contains(strColName)) {
				System.out.println("index Already exists");
				return;
			}

			if (colType == null) {
				System.out.println("Coloumn not found!!!");
				return;

			}

			if (colType.equals("java.lang.boolean") || colType.equals("java.lang.integer")
					|| colType.equals("java.lang.string") || colType.equals("java.lang.double")
					|| colType.equals("java.util.Date")) {
				System.out.println("can't create index on Boolean or polygon");
				return;
			}
			try {
				DBApp.MetaDataUpdator(strTableName, strColName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

//				

			// System.out.println("-------DateS-------");
			RTree<Integer, Vector<String>> index = new RTree<Integer, Vector<String>>(DBApp.NodeSize + 1, strTableName,
					strColName);
			DBApp.IndexPopulatorR(index, strTableName, strColName);
			DBApp.serialize(strTableName + "_" + strColName + "Index.txt", index);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException {
		try {
			Vector<DataWithType> metadata = ReadMetadata(strTableName);
			String colType = null;
			int i;
			for (i = 0; i < metadata.size(); i++) {
				if (strColName.equals(metadata.get(i).colName)) {
					colType = metadata.get(i).colType;
					break;
				}

			}
			Vector<String> indexes = allIndexes(strTableName);
			if (indexes.contains(strColName)) {
				throw new DBAppException("Index Already Exists");
			}

			if (colType == null) {
				throw new DBAppException("colType is null");

			}

			if (colType.equals("java.lang.Boolean") || colType.equals("java.awt.Polygon") || colType.equals("java.util.Date")) {
				throw new DBAppException("cantt create index on Boolean or polygon");
				
			}

			try {
				DBApp.MetaDataUpdator(strTableName, strColName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			
			if (colType.equals("java.lang.Integer")) {
				System.out.println("-------Integer-------");
				BPlusTree<Integer, Vector<String>> index = new BPlusTree<Integer, Vector<String>>(DBApp.NodeSize + 1,
						strTableName, strColName);
				DBApp.IndexPopulator(index, strTableName, strColName);
				DBApp.serialize(strTableName + "_" + strColName + "Index.txt", index);
			}
			if (colType.equals("java.lang.String")) {
				System.out.println("-------String-------");
				BPlusTree<String, Vector<String>> index = new BPlusTree<String, Vector<String>>(DBApp.NodeSize + 1,
						strTableName, strColName);
				DBApp.IndexPopulator(index, strTableName, strColName);
				DBApp.serialize(strTableName + "_" + strColName + "Index.txt", index);
			}

			if (colType.equals("java.lang.double")) {
				System.out.println("-------Double-------");
				BPlusTree<Double, Vector<String>> index = new BPlusTree<Double, Vector<String>>(DBApp.NodeSize + 1,
						strTableName, strColName);
				DBApp.IndexPopulator(index, strTableName, strColName);
				DBApp.serialize(strTableName + "_" + strColName + "Index.txt", index);
			}
			if (colType.equals("java.util.Date")) {
				System.out.println("-------DateS-------");
				BPlusTree<Date, Vector<String>> index = new BPlusTree<Date, Vector<String>>(DBApp.NodeSize + 1,
						strTableName, strColName);
				DBApp.IndexPopulator(index, strTableName, strColName);
				DBApp.serialize(strTableName + "_" + strColName + "Index.txt", index);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAppException(e.getMessage());
		}
	}

	public static void IndexPopulator(BPlusTree index, String tableName, String colName) throws Exception {

		Table table = (Table) DBApp.deserialize(tableName + ".txt");
		int datak;
		int pagei;
		for (pagei = 0; pagei < table.NumberOfPages; pagei++) {
			Page page = (Page) DBApp.deserialize("page_" + tableName + pagei + ".txt");

			for (int recordj = 0; recordj < page.records.size(); recordj++) {
				Record r = page.records.elementAt(recordj);
				System.out.println("IndexPopulator " + r + " PAGE NUM " + pagei);
				Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
				for (datak = 0; datak < r.coloumnsOfData.size(); datak++) {
					htblColNameValue.put(r.coloumnsOfData.get(datak).name, r.coloumnsOfData.get(datak).value);
				}
				String name = ("page_" + tableName + pagei + ".txt");

				index.insert((Comparable) htblColNameValue.get(colName), name);
			}
		}

//		table.colNameIndex.put(colName, index);// added by Hoda
//		DBApp.serialize(tableName + ".txt", table);// added by Hoda
		DBApp.serialize(tableName + "_" + colName + "Index.txt", index);

	}

	public static void IndexPopulatorR(RTree index, String tableName, String colName) throws Exception {

		Table table = (Table) DBApp.deserialize(tableName + ".txt");
		int datak;
		int pagei;
		for (pagei = 0; pagei < table.NumberOfPages; pagei++) {
			Page page = (Page) DBApp.deserialize("page_" + tableName + pagei + ".txt");

			for (int recordj = 0; recordj < page.records.size(); recordj++) {
				Record r = page.records.elementAt(recordj);
				System.out.println("IndexPopulator " + r + " PAGE NUM " + pagei);
				Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
				for (datak = 0; datak < r.coloumnsOfData.size(); datak++) {
					htblColNameValue.put(r.coloumnsOfData.get(datak).name, r.coloumnsOfData.get(datak).value);
				}
				String name = ("page_" + tableName + pagei + ".txt");
				Polygonn temp = (Polygonn) htblColNameValue.get(colName);

				index.insert((Comparable) temp.getArea(), name);
			}
		}

		// table.colNameIndex.put(colName, index);// added by Hoda
		// DBApp.serialize(tableName + ".txt", table);// added by Hoda
		DBApp.serialize(tableName + "_" + colName + "Index.txt", index);

	}

	public Set<Record> SelectWithTreeIndex(String strTableName, Table table, String colName, Object Value,
			String operator) throws Exception {
		Set<Record> answer = new HashSet<Record>();
		Boolean isBtree = false;
		Boolean isRtree = false;
		BPlusTree Btree = new BPlusTree();
		RTree Rtree = new RTree();
		Vector<String> columnsWithIndex = allIndexes(strTableName);

		for (String key : columnsWithIndex) {
			if (colName.equals(key)) {
				Object o = deserialize(strTableName + "_" + colName + "Index.txt");
				if (o instanceof BPlusTree) {
					isBtree = true;
					Btree = (BPlusTree) DBApp.deserialize(strTableName + "_" + colName + "Index.txt");
				} else if (o instanceof RTree) {
					isRtree = true;
					Rtree = (RTree) DBApp.deserialize(strTableName + "_" + colName + "Index.txt");

				}
			}
		}

		if (isBtree) {
			Vector<String> treevalues = Btree.search((Comparable) Value);
			// remove duplicate pages
			Vector<String> pagesVector = new Vector<String>();
			for (int i = 0; i < treevalues.size(); i++) {
				pagesVector.add(treevalues.get(i));
			}
			if (pagesVector == null) {
				throw new DBAppException("Value not in Table");
			}
			LinkedHashSet<String> lhSet = new LinkedHashSet<String>(pagesVector);
			pagesVector.clear();
			pagesVector.addAll(lhSet);

			for (int pagei = 0; pagei < pagesVector.size(); pagei++) {
				Page page = (Page) DBApp.deserialize("" + pagesVector.get(pagei));
				for (int recordj = 0; recordj < page.records.size(); recordj++) {
					Record r = page.records.elementAt(recordj);
					for (int datak = 0; datak < r.coloumnsOfData.size(); datak++) {
						Data d = r.coloumnsOfData.get(datak);
						if (d.name.equals(colName) && d.value.equals(Value)) {
							answer.add(r);
						}
					}

				}
			}
		}
		if (isRtree) {

			Polygonn poly = new Polygonn((Polygon) Value);
			int area = poly.getArea();
			if (operator.equals("=")) {
				Vector<String> treevalues = Rtree.search((Comparable) area);

				Vector<String> pagesVector = new Vector<String>();
				for (int i = 0; i < treevalues.size(); i++) {
					pagesVector.add(treevalues.get(i));
				}
				if (pagesVector == null) {
					throw new DBAppException("Value not in Table");
				}
				LinkedHashSet<String> lhSet = new LinkedHashSet<String>(pagesVector);
				pagesVector.clear();
				pagesVector.addAll(lhSet);

				for (int pagei = 0; pagei < pagesVector.size(); pagei++) {
					Page page = (Page) DBApp.deserialize("" + pagesVector.get(pagei));
					for (int recordj = 0; recordj < page.records.size(); recordj++) {
						Record r = page.records.elementAt(recordj);
						for (int datak = 0; datak < r.coloumnsOfData.size(); datak++) {
							Data d = r.coloumnsOfData.get(datak);
							if (d.name.equals(colName) && Equal(poly, d.value)) {
								answer.add(r);
							}
						}

					}
				}
			} else {
				Vector<String> treevalues = Rtree.search((Comparable) area);

				Vector<String> pagesVector = new Vector<String>();
				for (int i = 0; i < treevalues.size(); i++) {
					pagesVector.add(treevalues.get(i));
				}
				if (pagesVector == null) {
					throw new DBAppException("Value not in Table");
				}
				LinkedHashSet<String> lhSet = new LinkedHashSet<String>(pagesVector);
				pagesVector.clear();
				pagesVector.addAll(lhSet);

				for (int pagei = 0; pagei < pagesVector.size(); pagei++) {
					Page page = (Page) DBApp.deserialize("" + pagesVector.get(pagei));
					for (int recordj = 0; recordj < page.records.size(); recordj++) {
						Record r = page.records.elementAt(recordj);
						for (int datak = 0; datak < r.coloumnsOfData.size(); datak++) {
							Data d = r.coloumnsOfData.get(datak);
							if (d.name.equals(colName) && Compare(d.value, poly) == 0) {
								answer.add(r);
							}
						}

					}
				}

			}

		}

		return answer;

	}

	public static ArrayList<Record> RecordEqual(Page page, String operator, Object value, String strTableName,
			String colName) throws ClassNotFoundException, IOException, DBAppException {
		ArrayList<Record> answer = new ArrayList<Record>();

		switch (operator) {
		case "=":
			if (contains(0, (page.records.size() - 1), value, page, colName)) {
				int firstOccur = firstOccurenceIndex(0, (page.records.size() - 1), value, page, colName);
				int lastOccur = lastOccurrenceIndex(0, (page.records.size() - 1), value, page, colName);
				if (firstOccur == -1 || lastOccur == -1) {
					return answer;
				}
				for (int i = firstOccur; i <= lastOccur; i++) {
					if (!(answer.contains(page.records.get(i)))) {
						answer.add(page.records.get(i));
					}
				}
			}

			break;
		case "!=":
			if (contains(0, (page.records.size() - 1), value, page, colName)) {
				int firstOccur = firstOccurenceIndex(0, (page.records.size() - 1), value, page, colName);
				int lastOccur = lastOccurrenceIndex(0, (page.records.size() - 1), value, page, colName);
				if (firstOccur == -1 || lastOccur == -1) {
					return answer;
				}
				for (int i = 0; i < firstOccur; i++) {
					if (!(answer.contains(page.records.get(i)))) {
						answer.add(page.records.get(i));
					}
				}
				for (int i = (lastOccur + 1); i < page.records.size(); i++) {
					if (!(answer.contains(page.records.get(i)))) {
						answer.add(page.records.get(i));
					}
				}
			} else {
				Set<Record> set = linearSelect(operator, strTableName, (Table) DBApp.deserialize(strTableName + ".txt"),
						colName, value);
				for (Record r : set) {
					answer.add(r);
				}

			}

			break;
		case ">":
			int leastgreater = leastgreaterIndex(0, (page.records.size() - 1), value, page, colName);
			if (leastgreater != -1) {
				for (int i = leastgreater; i < page.records.size(); i++) {
					if (!(answer.contains(page.records.get(i)))) {
						answer.add(page.records.get(i));
					}
				}
			}

			break;
		case ">=":
			if (contains(0, (page.records.size() - 1), value, page, colName)) {
				int firstOccur = firstOccurenceIndex(0, (page.records.size() - 1), value, page, colName);
				if (firstOccur == -1) {
					return answer;
				}
				for (int i = firstOccur; i < page.records.size(); i++) {
					if (!(answer.contains(page.records.get(i)))) {
						answer.add(page.records.get(i));
					}
				}
			} else {
				int leastgreater1 = leastgreaterIndex(0, (page.records.size() - 1), value, page, colName);
				if (leastgreater1 != -1) {
					for (int i = leastgreater1; i < page.records.size(); i++) {
						if (!(answer.contains(page.records.get(i)))) {
							answer.add(page.records.get(i));
						}
					}
				}

			}
			break;
		case "<":
			int greatestlesser = greatestlesserIndex(0, (page.records.size() - 1), value, page, colName);
			if (greatestlesser != -1) {
				for (int i = 0; i <= greatestlesser; i++) {
					if (!(answer.contains(page.records.get(i)))) {
						answer.add(page.records.get(i));
					}
				}
			}
			break;
		case "<=":
			if (contains(0, (page.records.size() - 1), value, page, colName)) {
				int lastOccur = lastOccurrenceIndex(0, (page.records.size() - 1), value, page, colName);
				if (lastOccur == -1) {
					return answer;
				}
				for (int i = 0; i <= lastOccur; i++) {
					if (!(answer.contains(page.records.get(i)))) {
						answer.add(page.records.get(i));
					}
				}
			} else {
				int greatestlesser1 = greatestlesserIndex(0, (page.records.size() - 1), value, page, colName);
				if (greatestlesser1 != -1) {
					for (int i = 0; i <= greatestlesser1; i++) {
						if (!(answer.contains(page.records.get(i)))) {
							answer.add(page.records.get(i));
						}
					}
				}

			}
			break;
		default:
			throw new DBAppException("Invalid Operator");
		}

		return answer;
	}

	public static boolean contains(int low, int high, Object key, Page p, String colName) {
		Object midVal = null;

		boolean ans = false;
		while (low <= high) {
			int mid = low + (high - low) / 2;
			Record r = p.records.get(mid);
			for (int j = 0; j < r.coloumnsOfData.size(); j++) {
				Data d = r.coloumnsOfData.get(j);
				if (d.name.equals(colName)) {
					midVal = d.value;
				}
			}
			if (Compare(midVal, key) == -1) {

				low = mid + 1;
			} else if (Compare(midVal, key) == 1) {

				high = mid - 1;
			} else if (Compare(midVal, key) == 0) {

				ans = true;
				break;
			}
		}

		return ans;
	}

	public static int firstOccurenceIndex(int low, int high, Object key, Page p, String colName) {
		int ans = -1;
		Object midVal = null;
		while (low <= high) {
			int mid = low + (high - low + 1) / 2;
			Record r = p.records.get(mid);
			for (int j = 0; j < r.coloumnsOfData.size(); j++) {
				Data d = r.coloumnsOfData.get(j);
				if (d.name.equals(colName)) {
					midVal = d.value;
				}
			}

			if (Compare(midVal, key) == -1) {

				low = mid + 1;
			} else if (Compare(midVal, key) == 1) {

				high = mid - 1;
			} else if (Compare(midVal, key) == 0) {

				ans = mid;
				high = mid - 1;
			}
		}

		return ans;
	}

	public static int lastOccurrenceIndex(int low, int high, Object key, Page p, String colName) {
		int ans = -1;
		Object midVal = null;
		while (low <= high) {
			int mid = low + (high - low + 1) / 2;
			Record r = p.records.get(mid);
			for (int j = 0; j < r.coloumnsOfData.size(); j++) {
				Data d = r.coloumnsOfData.get(j);
				if (d.name.equals(colName)) {
					midVal = d.value;
				}
			}

			if (Compare(midVal, key) == -1) {

				low = mid + 1;
			} else if (Compare(midVal, key) == 1) {

				high = mid - 1;
			} else if (Compare(midVal, key) == 0) {

				ans = mid;
				low = mid + 1;
			}
		}

		return ans;
	}

	public static int leastgreaterIndex(int low, int high, Object key, Page p, String colName) {
		int ans = -1;
		Object midVal = null;
		while (low <= high) {
			int mid = low + (high - low + 1) / 2;
			Record r = p.records.get(mid);
			for (int j = 0; j < r.coloumnsOfData.size(); j++) {
				Data d = r.coloumnsOfData.get(j);
				if (d.name.equals(colName)) {
					midVal = d.value;
				}
			}

			if (Compare(midVal, key) == -1) {

				low = mid + 1;
			} else if (Compare(midVal, key) == 1) {

				ans = mid;
				high = mid - 1;
			} else if (Compare(midVal, key) == 0) {

				low = mid + 1;
			}
		}

		return ans;
	}

	public static int greatestlesserIndex(int low, int high, Object key, Page p, String colName) {
		int ans = -1;
		Object midVal = null;
		while (low <= high) {
			int mid = low + (high - low + 1) / 2;
			Record r = p.records.get(mid);
			for (int j = 0; j < r.coloumnsOfData.size(); j++) {
				Data d = r.coloumnsOfData.get(j);
				if (d.name.equals(colName)) {
					midVal = d.value;
					System.out.println(d.value);
				}
			}

			if (Compare(midVal, key) == -1) {

				ans = mid;
				low = mid + 1;
			} else if (Compare(midVal, key) == 1) {
				System.out.println("BIGGER");

				high = mid - 1;
			} else if (Compare(midVal, key) == 0) {

				high = mid - 1;
			}
		}

		return ans;
	}

	public static ArrayList<Record> PageBinarySearch(String operator, String strTableName, Object value, int lo, int hi,
			String clusterKey, ArrayList<Record> answer) throws ClassNotFoundException, IOException, DBAppException {
		int mid = (lo + hi) / 2;
		Table table = (Table) DBApp.deserialize(strTableName + ".txt");
		if (lo > hi || lo >= table.NumberOfPages) {
			return answer;
		}

		Page page = (Page) DBApp.deserialize("page_" + strTableName + mid + ".txt");

		Record firstRec = page.records.get(0);
		Object firstValue;
		Record lastRec = page.records.get(page.records.size() - 1);
		Object lastValue;
		int cmpFirst = 2;
		int cmpLast = 2;

		for (int i = 0; i < firstRec.coloumnsOfData.size(); i++) {
			Data d = firstRec.coloumnsOfData.get(i);
			if (d.name.equals(clusterKey)) {
				firstValue = d.value;
				cmpFirst = Compare(d.value, value);
			}
		}
		for (int i = 0; i < lastRec.coloumnsOfData.size(); i++) {
			Data d = lastRec.coloumnsOfData.get(i);
			if (d.name.equals(clusterKey)) {
				lastValue = d.value;
				cmpLast = Compare(d.value, value);
			}
		}

		switch (operator) {
		case "=":
			if (cmpFirst == 0 || (cmpFirst == -1 && cmpLast == 1) || (cmpFirst == -1 && cmpLast == 0)) {
				ArrayList<Record> temp = RecordEqual(page, operator, value, strTableName, clusterKey);
				for (int tempi = 0; tempi < temp.size(); tempi++) {
					if (!(answer.contains(temp.get(tempi))))
						answer.add(temp.get(tempi));
				}
				PageBinarySearch(operator, strTableName, value, lo, (mid - 1), clusterKey, answer);
				PageBinarySearch(operator, strTableName, value, (mid + 1), hi, clusterKey, answer);

			} else if (cmpFirst == 1) {
				PageBinarySearch(operator, strTableName, value, lo, (mid - 1), clusterKey, answer);
			} else if (cmpLast == -1) {
				PageBinarySearch(operator, strTableName, value, (mid + 1), hi, clusterKey, answer);
			}
			break;
		case "!=":
			for (int i = 0; i < table.NumberOfPages; i++) {
				Page p = (Page) DBApp.deserialize("page_" + strTableName + i + ".txt");

				ArrayList<Record> temp = RecordEqual(p, operator, value, strTableName, clusterKey);
				for (int tempi = 0; tempi < temp.size(); tempi++) {
					if (!(answer.contains(temp.get(tempi))))
						answer.add(temp.get(tempi));
				}
			}

			break;
		case ">":
			if (cmpFirst == -1 && (cmpLast == 0 || cmpLast == -1)) {
				PageBinarySearch(operator, strTableName, value, (mid + 1), hi, clusterKey, answer);
			} else if (cmpFirst == 1) {
				for (int i = mid; i < table.NumberOfPages; i++) {
					Page pagex = (Page) DBApp.deserialize("page_" + strTableName + i + ".txt");
					ArrayList<Record> temp1 = RecordEqual(pagex, operator, value, strTableName, clusterKey);
					for (int tempi = 0; tempi < temp1.size(); tempi++) {
						if (!(answer.contains(temp1.get(tempi))))
							answer.add(temp1.get(tempi));
					}
				}
				PageBinarySearch(operator, strTableName, value, lo, (mid - 1), clusterKey, answer);

			} else if ((cmpFirst == 0 || cmpFirst == -1) && cmpLast == 1) {
				for (int i = mid; i < table.NumberOfPages; i++) {
					Page pagex = (Page) DBApp.deserialize("page_" + strTableName + i + ".txt");
					ArrayList<Record> temp2 = RecordEqual(pagex, operator, value, strTableName, clusterKey);
					for (int tempi = 0; tempi < temp2.size(); tempi++) {
						if (!(answer.contains(temp2.get(tempi))))
							answer.add(temp2.get(tempi));
					}
				}

			}
			break;
		case ">=":
			if (cmpFirst == -1 && cmpLast == -1) {
				PageBinarySearch(operator, strTableName, value, (mid + 1), hi, clusterKey, answer);
			} else if (cmpFirst == 0 || cmpFirst == 1) {
				for (int i = mid; i < table.NumberOfPages; i++) {
					Page pagex = (Page) DBApp.deserialize("page_" + strTableName + i + ".txt");
					ArrayList<Record> temp3 = RecordEqual(pagex, operator, value, strTableName, clusterKey);
					for (int tempi = 0; tempi < temp3.size(); tempi++) {
						if (!(answer.contains(temp3.get(tempi))))
							answer.add(temp3.get(tempi));
					}
				}
				PageBinarySearch(operator, strTableName, value, lo, (mid - 1), clusterKey, answer);
			} else if (cmpFirst == -1 && (cmpLast == 0 || cmpLast == 1)) {
				for (int i = mid; i < table.NumberOfPages; i++) {
					Page pagex = (Page) DBApp.deserialize("page_" + strTableName + i + ".txt");
					ArrayList<Record> temp4 = RecordEqual(pagex, operator, value, strTableName, clusterKey);
					for (int tempi = 0; tempi < temp4.size(); tempi++) {
						if (!(answer.contains(temp4.get(tempi))))
							answer.add(temp4.get(tempi));
					}
				}

			}

			break;
		case "<":
			if (cmpFirst == 1 || cmpFirst == 0) {
				PageBinarySearch(operator, strTableName, value, lo, (mid - 1), clusterKey, answer);
			} else if (cmpLast == -1) {
				// haya5od kol elly gowa el page wel pages elly ablaha then check page after
				for (int i = mid; i >= 0; i--) {
					Page pagex = (Page) DBApp.deserialize("page_" + strTableName + i + ".txt");
					ArrayList<Record> temp5 = RecordEqual(pagex, operator, value, strTableName, clusterKey);
					for (int tempi = 0; tempi < temp5.size(); tempi++) {
						if (!(answer.contains(temp5.get(tempi))))
							answer.add(temp5.get(tempi));
					}
				}
				PageBinarySearch(operator, strTableName, value, (mid + 1), hi, clusterKey, answer);
			} else if (cmpFirst == -1 && (cmpLast == 0 || cmpLast == 1)) {
				for (int i = mid; i >= 0; i--) {
					Page pagex = (Page) DBApp.deserialize("page_" + strTableName + i + ".txt");
					ArrayList<Record> temp6 = RecordEqual(pagex, operator, value, strTableName, clusterKey);
					for (int tempi = 0; tempi < temp6.size(); tempi++) {
						if (!(answer.contains(temp6.get(tempi))))
							answer.add(temp6.get(tempi));
					}
				}
			}

			break;
		case "<=":
			if (cmpFirst == 1) {
				PageBinarySearch(operator, strTableName, value, lo, (mid - 1), clusterKey, answer);
			} else if (cmpLast == -1 || cmpLast == 0) {
				for (int i = mid; i >= 0; i--) {
					Page pagex = (Page) DBApp.deserialize("page_" + strTableName + i + ".txt");
					ArrayList<Record> temp7 = RecordEqual(pagex, operator, value, strTableName, clusterKey);
					for (int tempi = 0; tempi < temp7.size(); tempi++) {
						if (!(answer.contains(temp7.get(tempi))))
							answer.add(temp7.get(tempi));
					}
				}
				PageBinarySearch(operator, strTableName, value, (mid + 1), hi, clusterKey, answer);
			} else if (cmpLast == 1 && (cmpFirst == 0 || cmpFirst == -1)) {
				for (int i = mid; i >= 0; i--) {
					Page pagex = (Page) DBApp.deserialize("page_" + strTableName + i + ".txt");
					ArrayList<Record> temp8 = RecordEqual(pagex, operator, value, strTableName, clusterKey);
					for (int tempi = 0; tempi < temp8.size(); tempi++) {
						if (!(answer.contains(temp8.get(tempi))))
							answer.add(temp8.get(tempi));
					}
				}
			}
			break;
		default:
			throw new DBAppException("Invalid Operator");
		}

		return answer;
	}

	public Iterator<Record> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws Exception {

		if (arrSQLTerms.length != 0) {

			ArrayList<String> operators = new ArrayList<String>();
			List<Set<Record>> sets = new ArrayList<Set<Record>>();
			// ba7ot el strarrOperators f ArrayList cause 3ayza keda

			for (int i = 0; i < strarrOperators.length; i++) {
				operators.add(strarrOperators[i]);
			}

			for (int sqlTermi = 0; sqlTermi < arrSQLTerms.length; sqlTermi++) {
				String strTableName = arrSQLTerms[sqlTermi].getTableName();
				String colName = arrSQLTerms[sqlTermi].getColName();
				Object Value = arrSQLTerms[sqlTermi].getObjValue();
				String operator = arrSQLTerms[sqlTermi].getOperator();
				if (!(DBApp.All_Tables.tables.contains(strTableName))) {
					throw new DBAppException("Table Name doesn't exist");
				}
				Table table = (Table) (Table) DBApp.deserialize(strTableName + ".txt");
				if (!(checkDataType(colName, Value, table))) { // check values entered by user are of correct DataTypes
					throw new DBAppException("Wrong Data Type");
				}
				if (Value instanceof Polygon) { // if user entered Polygon switch to Polygonn
					// 3ashan e7na 3amleen keda
					Value = new Polygonn((Polygon) Value);
					System.out.println("POLYYY" + Value);
				}

				Set<Record> itr = SelectWithTreeIndex(strTableName, table, colName, Value, operator);

				if (itr.size() == 0) {

					// law clustering yeb2a binary search
					if (table.clustering.equals(colName)) {
						System.out.println("BINARY INDEX ON COLUMN: " + colName);
						ArrayList<Record> temp = PageBinarySearch(operator, strTableName, Value, 0, table.NumberOfPages,
								colName, new ArrayList<Record>());
						Set<Record> settemp = new HashSet<Record>();
						for (int i = 0; i < temp.size(); i++) {
							settemp.add(temp.get(i));
						}
						if (settemp.size() == 0) {
							throw new DBAppException("Value not in Table");
						}
						sets.add(settemp);

					}

					// linear
					else {
						System.out.println("LINEAR INDEX ON COLUMN: " + colName);
						Set<Record> linear = linearSelect(operator, strTableName, table, colName, Value);
						if (linear.size() == 0) {
							throw new DBAppException("Value not in Table");
						}
						sets.add(linear);

					}
				} else {
					sets.add(itr);

					System.out.println("BTREE INDEX ON COLUMN: " + colName);
				}

			}

			if (operators.size() != 0 && sets.size() > 1) {

				Set<Record> firstSet = sets.get(0);
				sets.remove(0);
				return CombineSets(sets, operators, firstSet);

			} else {
				return sets.get(0).iterator();
			}

		}
		Set<Record> x = new HashSet<>();
		System.out.println("You didn't enter anything to query on :)!");

		return x.iterator();

	}

	public static Iterator<Record> CombineSets(List<Set<Record>> sets, ArrayList<String> operators,
			Set<Record> setAtHand) {

		while (operators.size() != 0 && sets.size() != 0) {

			boolean isOr = operators.get(0).equals("OR");
			boolean isAND = operators.get(0).equals("AND");
			boolean isXOR = operators.get(0).equals("XOR");

			Set<Record> set1 = sets.get(0);

			if (isAND) {
				Set<Record> intersection = SetAND(set1, setAtHand);
				sets.remove(0);
				operators.remove(0);
				return CombineSets(sets, operators, intersection);
			}
			if (isOr) {

				Set<Record> union = SetOR(set1, setAtHand);
				sets.remove(0);
				operators.remove(0);
				return CombineSets(sets, operators, union);
			}
			if (isXOR) {
				Set<Record> xor = SetXOR(set1, setAtHand);
				sets.remove(0);
				operators.remove(0);
				return CombineSets(sets, operators, xor);
			}

		}

		return setAtHand.iterator();
	}

	public static Set<Record> linearSelect(String operator, String strTableName, Table table, String colName,
			Object Value) throws ClassNotFoundException, IOException {
		Set<Record> result = new HashSet<Record>();
		for (int pagei = 0; pagei < table.NumberOfPages; pagei++) {
			Page page = (Page) DBApp.deserialize("page_" + strTableName + pagei + ".txt");
			for (int recordj = 0; recordj < page.records.size(); recordj++) {
				Record r = page.records.elementAt(recordj);
				for (int datak = 0; datak < r.coloumnsOfData.size(); datak++) {
					Data d = r.coloumnsOfData.get(datak);

					if (d.name.equals(colName)) {
						if (compareOperator(operator, d.value, Value)) {
							result.add(r);
						}
					}

				}

			}
		}
		return result;
	}

	public static Set<Record> SetAND(Set<Record> set1, Set<Record> set2) {
		Set<Record> intersection = new HashSet<Record>(set1); // use the copy constructor
		intersection.retainAll(set2);

		return intersection;

	}

	public static Set<Record> SetOR(Set<Record> set1, Set<Record> set2) {
		Set<Record> union = new HashSet<Record>(set1); // use the copy constructor
		union.addAll(set2);
		return union;

	}

	public static Set<Record> SetXOR(Set<Record> set1, Set<Record> set2) {
		Set<Record> xor = new HashSet<Record>();

		xor.addAll(set1);

		xor.addAll(set2);

		set1.retainAll(set2);

		xor.removeAll(set1);

		return xor;

	}

	public static boolean compareOperator(String operator, Object valueofRecord, Object ValueInCondition) {

		switch (operator) {
		case "=":
			if (Compare(valueofRecord, ValueInCondition) == 0) {
				return true;
			}
			break;
		case "!=":
			if (Compare(valueofRecord, ValueInCondition) != 0) {
				return true;
			}
			break;
		case ">":
			if (Compare(valueofRecord, ValueInCondition) == 1) {
				return true;
			}
			break;
		case ">=":
			if (Compare(valueofRecord, ValueInCondition) == 0 || Compare(valueofRecord, ValueInCondition) == 1) {
				return true;
			}
			break;
		case "<":
			if (Compare(valueofRecord, ValueInCondition) == -1) {
				return true;
			}
			break;
		case "<=":
			if (Compare(valueofRecord, ValueInCondition) == 0 || Compare(valueofRecord, ValueInCondition) == -1) {
				return true;
			}
			break;
		default:
			System.out.println("Invalid Operator");
		}
		return false;

	}

	public static Date UpdateDateformatter(String date) throws ParseException {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date date1 = formatter.parse(date);
		return date1;
	}

	public static Object typeCast(String strClusteringKey, Table table)
			throws ClassNotFoundException, ParseException, DBAppException {
		for (int i = 0; i < table.dataType.size(); i++) {
			// YYYY-MM-DD
			if (table.dataType.get(i).colName.equals(table.clustering)) {
				Class clusterClass = Class.forName(table.dataType.get(i).colType);
				switch (clusterClass.toString().toLowerCase()) {
				case "class java.lang.integer":
					return Integer.parseInt(strClusteringKey);
				case "class java.lang.double":
					return Double.parseDouble(strClusteringKey);
				case "class java.awt.polygon":
					return Polygonn.getPolygon(strClusteringKey);
				case "class java.lang.date":
					return UpdateDateformatter(strClusteringKey);
				case "class java.lang.boolean":
					return Boolean.parseBoolean(strClusteringKey);
				default:
					throw new DBAppException("InValid Data Type ");

				}
			}
		}
		return null;
	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		// strClusteringKey beyeb2a value of the clusterkey
		// htblColNameValue has only data values i want to change in a record
		try {
			boolean isClusterIndex = false;
			if (DBApp.All_Tables.tables.contains(strTableName)) {
				ReadMetadata(strTableName);
				Vector<String> columnsWithIndex = allIndexes(strTableName);
				Table table = (Table) DBApp.deserialize(strTableName + ".txt");

				for (String key : htblColNameValue.keySet()) {

					if (!(checkDataType(key, htblColNameValue.get(key), table))) {
						// check values entered by user are of correct DataTypes
						throw new DBAppException("Wrong Data Type");
					}
					if (htblColNameValue.get(key) instanceof Polygon) { // if user entered Polygon switch to Polygonn
						// 3ashan e7na 3amleen keda
						htblColNameValue.replace(key, new Polygonn((Polygon) htblColNameValue.get(key)));
					}
				}
				htblColNameValue.put("TouchDate", new Date()); // to update el touchDate

				// check if strClusteringKey has a Tree
				BPlusTree ClusterBtree = null;
				RTree ClusterRtree = null;
				for (String colName : columnsWithIndex) {
					Object o = deserialize(strTableName + "_" + colName + "Index.txt");
					if (colName.equals(table.clustering)) {
						isClusterIndex = true;
						if (o instanceof BPlusTree) {
							ClusterBtree = (BPlusTree) DBApp.deserialize(strTableName + "_" + colName + "Index.txt");
						}
						if (o instanceof RTree) {
							ClusterRtree = (RTree) DBApp.deserialize(strTableName + "_" + colName + "Index.txt");
						}

					}
				}

				// type cast strClusteringKey lel object el sa7 beta3o before passing it
				// anywhere

				Object clusterkey = typeCast(strClusteringKey, table);
				//System.out.println(clusterkey);
				if (clusterkey.equals(null)) {
					throw new ClassCastException();
				}
				// search by Tree
				if (isClusterIndex) {
//					System.out.println("CLUSTERING KEY INDEXED");
//					System.out.println("--------------------------------------------------------");
					if (ClusterBtree != null) {
						int pagei;
						Vector<String> treevalues = ClusterBtree.search((Comparable) clusterkey);
						if (treevalues == null) {
							throw new DBAppException("Wrong Clustering Key Value");
						}
						// remove duplicate pages
						Vector<String> pagesVector = new Vector<String>();
						for (int i = 0; i < treevalues.size(); i++) {
							//System.out.println("PAGES IN TREE: " + treevalues.get(i));
							pagesVector.add(treevalues.get(i));
						}
						LinkedHashSet<String> lhSet = new LinkedHashSet<String>(pagesVector);
						pagesVector.clear();
						pagesVector.addAll(lhSet);
						for (pagei = 0; pagei < pagesVector.size(); pagei++) {
							Page page = (Page) DBApp.deserialize("" + pagesVector.get(pagei));
							for (int recordj = 0; recordj < page.records.size(); recordj++) {
								Record r = page.records.elementAt(recordj);
								for (int i = 0; i < r.coloumnsOfData.size(); i++) {
									Data d = r.coloumnsOfData.get(i);
									if (d.name.equals(table.clustering) && Equal(clusterkey, d.value)) {
										helperUpdate("" + pagesVector.get(pagei), htblColNameValue, r, page, table,
												strTableName);

									}
								}
							}
						}
					} else {

						int pagei;
						int area = ((Polygonn) clusterkey).getArea();
						Vector<String> treevalues = ClusterRtree.search((Comparable) area);
						if (treevalues == null) {
							throw new DBAppException("Wrong Clustering Key Value");
						}
						// remove duplicate pages
						Vector<String> pagesVector = new Vector<String>();
						for (int i = 0; i < treevalues.size(); i++) {
							// System.out.println("PAGES IN TREE: " + treevalues.get(i));
							pagesVector.add(treevalues.get(i));
						}
						LinkedHashSet<String> lhSet = new LinkedHashSet<String>(pagesVector);
						pagesVector.clear();
						pagesVector.addAll(lhSet);
						for (pagei = 0; pagei < pagesVector.size(); pagei++) {
							Page page = (Page) DBApp.deserialize("" + pagesVector.get(pagei));
							for (int recordj = 0; recordj < page.records.size(); recordj++) {
								Record r = page.records.elementAt(recordj);
								for (int i = 0; i < r.coloumnsOfData.size(); i++) {
									Data d = r.coloumnsOfData.get(i);
									if (d.name.equals(table.clustering) && Equal(clusterkey, d.value)) {
										helperUpdate("" + pagesVector.get(pagei), htblColNameValue, r, page, table,
												strTableName);

									}
								}
							}
						}

					}

				}
				// else binary Search
				else {
//					System.out.println("BINARY SEARCH");
//					System.out.println("--------------------------------------------------------");
					int pagei;
					Boolean exists = false;
					for (pagei = 0; pagei < table.NumberOfPages; pagei++) {
						Page page = (Page) DBApp.deserialize("page_" + strTableName + pagei + ".txt");
						if (contains(0, (page.records.size() - 1), clusterkey, page, table.clustering)) {
							int firstOccur = firstOccurenceIndex(0, (page.records.size() - 1), clusterkey, page,
									table.clustering);
							int lastOccur = lastOccurrenceIndex(0, (page.records.size() - 1), clusterkey, page,
									table.clustering);
							if (firstOccur == -1 || lastOccur == -1) {
								break;
							}
							for (int i = firstOccur; i <= lastOccur; i++) {

								Record r = page.records.elementAt(i);
								for (int j = 0; j < r.coloumnsOfData.size(); j++) {
									Data d = r.coloumnsOfData.get(j);
									if (d.name.equals(table.clustering)) {
										exists = true;
										helperUpdate("page_" + strTableName + pagei + ".txt", htblColNameValue, r, page,
												table, strTableName);
									}
								}
							}
						}
					}
					if (exists == false) {
						throw new DBAppException("Value not found in Table");
					}

				}
				serialize(strTableName + ".txt", table);

			} else {
				throw new DBAppException("Table not found");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void updateTree(String pageName, Table table, String colName, Object oldValue, Object updatedValue,
			String strTableName) throws DBAppException {
		try {

			Vector<String> columnsWithIndex = allIndexes(strTableName);
			for (String key : columnsWithIndex) {
				if (colName.equals(key)) {

					Object o = deserialize(strTableName + "_" + colName + "Index.txt");
					if (o instanceof BPlusTree) {
						BPlusTree tree = (BPlusTree) o;

						Record r = new Record();
						r.coloumnsOfData.add(new Data(oldValue, colName));
						Hashtable<String, Object> temp = CovertRecordToHashTable(r);
						Vector<String> tempVector = new Vector<String>();
						tempVector.add(colName);
						deleteIndexOccurence(getPageNumber(pageName), strTableName, tempVector, temp);

						serialize(strTableName + "_" + colName + "Index.txt", tree);

						BPlusTree trees = (BPlusTree) DBApp.deserialize(strTableName + "_" + colName + "Index.txt");

					} else {
						RTree tree = (RTree) o;

						Record r = new Record();
						r.coloumnsOfData.add(new Data(oldValue, colName));
						Hashtable<String, Object> temp = CovertRecordToHashTable(r);
						Vector<String> tempVector = new Vector<String>();
						tempVector.add(colName);
						deleteIndexOccurence(getPageNumber(pageName), strTableName, tempVector, temp);

						serialize(strTableName + "_" + colName + "Index.txt", tree);

					}
				}
			}
		} catch (Exception e) {
			throw new DBAppException("Exception");

		}

	}

	public static void helperUpdate(String pageName, Hashtable<String, Object> dataToUpdate, Record oldRecord, Page p,
			Table table, String strTableName) {
		try {
			// check if any columns in dataArray have a tree
			// ha7ot dataArray hashtable f data 3ashan a3raf a3melo both values equal b3d
			ArrayList<Data> dataArray = new ArrayList<Data>();
			for (String key : dataToUpdate.keySet()) {
				dataArray.add(new Data(dataToUpdate.get(key), key));
			}

			for (int i = 0; i < oldRecord.coloumnsOfData.size(); i++) {
				Data oldData = oldRecord.coloumnsOfData.elementAt(i);
				for (Data updatedData : dataArray) {
					if (Equal(oldData.name, updatedData.name)) {

						Object oldDataV = oldData.value;
						Object newDataV = updatedData.value;
						oldData.value = updatedData.value;

						serialize(pageName, p);
						updateTree(pageName, table, oldData.name, oldDataV, newDataV, strTableName);

					}
				}

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void inserter(DBApp app, String strTableName) throws DBAppException {

		Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>();
		htblColNameValue2.put("id", new Integer(1));
		htblColNameValue2.put("name", new String("John Noor"));
		htblColNameValue2.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue2);

		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		htblColNameValue.put("id", new Integer(2));
		htblColNameValue.put("name", new String("Hoda"));
		htblColNameValue.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue);

		Hashtable<String, Object> htblColNameValue3 = new Hashtable<String, Object>();
		htblColNameValue3.put("id", new Integer(3));
		htblColNameValue3.put("name", new String("Rana"));
		htblColNameValue3.put("gpa", new Double(3));
		app.insertIntoTable(strTableName, htblColNameValue3);

		Hashtable<String, Object> htblColNameValue4 = new Hashtable<String, Object>();
		htblColNameValue4.put("id", new Integer(4));
		htblColNameValue4.put("name", new String("3asfoor"));
		htblColNameValue4.put("gpa", new Double(1));
		app.insertIntoTable(strTableName, htblColNameValue4);

		Hashtable<String, Object> htblColNameValue1 = new Hashtable<String, Object>();
		htblColNameValue1.put("id", new Integer(4));
		htblColNameValue1.put("name", new String("Fady"));
		htblColNameValue1.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue1);

		Hashtable<String, Object> htblColNameValue5 = new Hashtable<String, Object>();
		htblColNameValue5.put("id", new Integer(5));
		htblColNameValue5.put("name", new String("Mohsen"));
		htblColNameValue5.put("gpa", new Double(2));
		app.insertIntoTable(strTableName, htblColNameValue5);

		Hashtable<String, Object> htblColNameValue6 = new Hashtable<String, Object>();
		htblColNameValue6.put("id", new Integer(5));
		htblColNameValue6.put("name", new String("Ganzabeel"));
		htblColNameValue6.put("gpa", new Double(3));
		app.insertIntoTable(strTableName, htblColNameValue6);

		Hashtable<String, Object> htblColNameValue7 = new Hashtable<String, Object>();
		htblColNameValue7.put("id", new Integer(6));
		htblColNameValue7.put("name", new String("leefa"));
		htblColNameValue7.put("gpa", new Double(1));
		app.insertIntoTable(strTableName, htblColNameValue7);

		Hashtable<String, Object> htblColNameValue8 = new Hashtable<String, Object>();
		htblColNameValue8.put("id", new Integer(7));
		htblColNameValue8.put("name", new String("dodo"));
		htblColNameValue8.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue8);

		Hashtable<String, Object> htblColNameValue9 = new Hashtable<String, Object>();
		htblColNameValue9.put("id", new Integer(8));
		htblColNameValue9.put("name", new String("ranoon"));
		htblColNameValue9.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue9);

		Hashtable<String, Object> htblColNameValuee = new Hashtable<String, Object>();
		htblColNameValuee.put("id", new Integer(9));
		htblColNameValuee.put("name", new String("deemo"));
		htblColNameValuee.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValuee);

		Hashtable<String, Object> htblColNameValueee = new Hashtable<String, Object>();
		htblColNameValueee.put("id", new Integer(9));
		htblColNameValueee.put("name", new String("shikabala"));
		htblColNameValueee.put("gpa", new Double(3));
		app.insertIntoTable(strTableName, htblColNameValueee);
	}

	public static void inserter2(DBApp app, String strTableName) throws DBAppException, ParseException {
		Polygon polyy = new Polygon();
		polyy.addPoint(1, 88);
		polyy.addPoint(5, 32);
		polyy.addPoint(14, 20);
		polyy.addPoint(24, 360);
		polyy.addPoint(15, 10);

		Polygon polyy1 = new Polygon();
		polyy1.addPoint(1, 88);
		polyy1.addPoint(6, 50);
		polyy1.addPoint(14, 20);

		Polygon polyy2 = new Polygon();
		polyy2.addPoint(1, 90);
		polyy2.addPoint(6, 50);
		polyy2.addPoint(20, 20);
		polyy2.addPoint(20, 36);

		Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>();
		htblColNameValue2.put("id", new Integer(1));
		htblColNameValue2.put("name", new String("John Noor"));
		htblColNameValue2.put("gpa", new Double(1.5));
		htblColNameValue2.put("isHealthy", new Boolean(true));
		htblColNameValue2.put("birthday", UpdateDateformatter("1968-04-09"));
		htblColNameValue2.put("polygon", polyy);
		app.insertIntoTable(strTableName, htblColNameValue2);

		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		htblColNameValue.put("id", new Integer(2));
		htblColNameValue.put("name", new String("Hoda"));
		htblColNameValue.put("gpa", new Double(1.5));
		htblColNameValue.put("isHealthy", new Boolean(true));
		htblColNameValue.put("birthday", UpdateDateformatter("1968-04-09"));
		htblColNameValue.put("polygon", polyy1);
		app.insertIntoTable(strTableName, htblColNameValue);

		Hashtable<String, Object> htblColNameValue3 = new Hashtable<String, Object>();
		htblColNameValue3.put("id", new Integer(3));
		htblColNameValue3.put("name", new String("Rana"));
		htblColNameValue3.put("gpa", new Double(3));
		htblColNameValue3.put("isHealthy", new Boolean(false));
		htblColNameValue3.put("birthday", UpdateDateformatter("2000-2-25"));
		htblColNameValue3.put("polygon", polyy2);
		app.insertIntoTable(strTableName, htblColNameValue3);

		Hashtable<String, Object> htblColNameValue4 = new Hashtable<String, Object>();
		htblColNameValue4.put("id", new Integer(4));
		htblColNameValue4.put("name", new String("3asfoor"));
		htblColNameValue4.put("gpa", new Double(1));
		htblColNameValue4.put("isHealthy", new Boolean(true));
		htblColNameValue4.put("birthday", UpdateDateformatter("1968-04-09"));
		htblColNameValue4.put("polygon", polyy);
		app.insertIntoTable(strTableName, htblColNameValue4);

		Hashtable<String, Object> htblColNameValue1 = new Hashtable<String, Object>();
		htblColNameValue1.put("id", new Integer(4));
		htblColNameValue1.put("name", new String("Fady"));
		htblColNameValue1.put("gpa", new Double(1.5));
		htblColNameValue1.put("isHealthy", new Boolean(false));
		htblColNameValue1.put("birthday", UpdateDateformatter("1998-08-10"));
		htblColNameValue1.put("polygon", polyy1);
		app.insertIntoTable(strTableName, htblColNameValue1);

		Hashtable<String, Object> htblColNameValue5 = new Hashtable<String, Object>();
		htblColNameValue5.put("id", new Integer(5));
		htblColNameValue5.put("name", new String("Mohsen"));
		htblColNameValue5.put("gpa", new Double(2));
		htblColNameValue5.put("isHealthy", new Boolean(true));
		htblColNameValue5.put("birthday", UpdateDateformatter("1969-04-10"));
		htblColNameValue5.put("polygon", polyy1);
		app.insertIntoTable(strTableName, htblColNameValue5);

		Hashtable<String, Object> htblColNameValue6 = new Hashtable<String, Object>();
		htblColNameValue6.put("id", new Integer(5));
		htblColNameValue6.put("name", new String("Ganzabeel"));
		htblColNameValue6.put("gpa", new Double(3));
		htblColNameValue6.put("isHealthy", new Boolean(true));
		htblColNameValue6.put("birthday", UpdateDateformatter("1868-04-10"));
		htblColNameValue6.put("polygon", polyy2);
		app.insertIntoTable(strTableName, htblColNameValue6);

		Hashtable<String, Object> htblColNameValue7 = new Hashtable<String, Object>();
		htblColNameValue7.put("id", new Integer(6));
		htblColNameValue7.put("name", new String("leefa"));
		htblColNameValue7.put("gpa", new Double(1));
		htblColNameValue7.put("isHealthy", new Boolean(true));
		htblColNameValue7.put("birthday", UpdateDateformatter("2000-04-10"));
		htblColNameValue7.put("polygon", polyy);
		app.insertIntoTable(strTableName, htblColNameValue7);

		Hashtable<String, Object> htblColNameValue8 = new Hashtable<String, Object>();
		htblColNameValue8.put("id", new Integer(7));
		htblColNameValue8.put("name", new String("dodo"));
		htblColNameValue8.put("gpa", new Double(1.5));
		htblColNameValue8.put("isHealthy", new Boolean(true));
		htblColNameValue8.put("birthday", UpdateDateformatter("1999-01-06"));
		htblColNameValue8.put("polygon", polyy1);
		app.insertIntoTable(strTableName, htblColNameValue8);

		Hashtable<String, Object> htblColNameValue9 = new Hashtable<String, Object>();
		htblColNameValue9.put("id", new Integer(8));
		htblColNameValue9.put("name", new String("ranoon"));
		htblColNameValue9.put("gpa", new Double(1.5));
		htblColNameValue9.put("isHealthy", new Boolean(true));
		htblColNameValue9.put("birthday", UpdateDateformatter("1968-04-10"));
		htblColNameValue9.put("polygon", polyy2);
		app.insertIntoTable(strTableName, htblColNameValue9);

		Hashtable<String, Object> htblColNameValuee = new Hashtable<String, Object>();
		htblColNameValuee.put("id", new Integer(9));
		htblColNameValuee.put("name", new String("deemo"));
		htblColNameValuee.put("gpa", new Double(1.5));
		htblColNameValuee.put("isHealthy", new Boolean(true));
		htblColNameValuee.put("birthday", UpdateDateformatter("1968-04-10"));
		htblColNameValuee.put("polygon", polyy);
		app.insertIntoTable(strTableName, htblColNameValuee);

		Hashtable<String, Object> htblColNameValueee = new Hashtable<String, Object>();
		htblColNameValueee.put("id", new Integer(9));
		htblColNameValueee.put("name", new String("shikabala"));
		htblColNameValueee.put("gpa", new Double(3));
		htblColNameValueee.put("isHealthy", new Boolean(false));
		htblColNameValueee.put("birthday", UpdateDateformatter("1968-01-10"));
		htblColNameValueee.put("polygon", polyy2);
		app.insertIntoTable(strTableName, htblColNameValueee);
	}

	public static void inserter3(DBApp app, String strTableName) throws DBAppException {

		Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>();
		htblColNameValue2.put("id", new Integer(10));
		htblColNameValue2.put("name", new String("Sawsan"));
		htblColNameValue2.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue2);

		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		htblColNameValue.put("id", new Integer(11));
		htblColNameValue.put("name", new String("Bosayna"));
		htblColNameValue.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue);

		Hashtable<String, Object> htblColNameValue3 = new Hashtable<String, Object>();
		htblColNameValue3.put("id", new Integer(3));
		htblColNameValue3.put("name", new String("SikoBiko"));
		htblColNameValue3.put("gpa", new Double(3));
		app.insertIntoTable(strTableName, htblColNameValue3);

		Hashtable<String, Object> htblColNameValue4 = new Hashtable<String, Object>();
		htblColNameValue4.put("id", new Integer(40));
		htblColNameValue4.put("name", new String("Hamada"));
		htblColNameValue4.put("gpa", new Double(1));
		app.insertIntoTable(strTableName, htblColNameValue4);

		Hashtable<String, Object> htblColNameValue1 = new Hashtable<String, Object>();
		htblColNameValue1.put("id", new Integer(99));
		htblColNameValue1.put("name", new String("Basboosa"));
		htblColNameValue1.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue1);

		Hashtable<String, Object> htblColNameValue5 = new Hashtable<String, Object>();
		htblColNameValue5.put("id", new Integer(5));
		htblColNameValue5.put("name", new String("kiloBamya"));
		htblColNameValue5.put("gpa", new Double(2));
		app.insertIntoTable(strTableName, htblColNameValue5);

		Hashtable<String, Object> htblColNameValue6 = new Hashtable<String, Object>();
		htblColNameValue6.put("id", new Integer(89));
		htblColNameValue6.put("name", new String("gogo"));
		htblColNameValue6.put("gpa", new Double(3));
		app.insertIntoTable(strTableName, htblColNameValue6);

		Hashtable<String, Object> htblColNameValue7 = new Hashtable<String, Object>();
		htblColNameValue7.put("id", new Integer(6));
		htblColNameValue7.put("name", new String("Mohamed"));
		htblColNameValue7.put("gpa", new Double(1));
		app.insertIntoTable(strTableName, htblColNameValue7);

		Hashtable<String, Object> htblColNameValue8 = new Hashtable<String, Object>();
		htblColNameValue8.put("id", new Integer(70));
		htblColNameValue8.put("name", new String("Ragaie"));
		htblColNameValue8.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue8);

		Hashtable<String, Object> htblColNameValue9 = new Hashtable<String, Object>();
		htblColNameValue9.put("id", new Integer(80));
		htblColNameValue9.put("name", new String("Emad"));
		htblColNameValue9.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue9);

		Hashtable<String, Object> htblColNameValuee = new Hashtable<String, Object>();
		htblColNameValuee.put("id", new Integer(900));
		htblColNameValuee.put("name", new String("Zalata"));
		htblColNameValuee.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValuee);

		Hashtable<String, Object> htblColNameValueee = new Hashtable<String, Object>();
		htblColNameValueee.put("id", new Integer(9));
		htblColNameValueee.put("name", new String("Tomeya"));
		htblColNameValueee.put("gpa", new Double(3));
		app.insertIntoTable(strTableName, htblColNameValueee);
	}

	public static void main(String[] args) throws Exception {

		DBApp app = new DBApp();
		app.init();
		String strTableName = "Student";

		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		htblColNameValue.put("gpa", new Double(1.5));
		htblColNameValue.put("name", "Hoda");
		// app.updateTable(strTableName, "(1,66)(4,24)(13,15)(23,270)(14,9)",
		// htblColNameValue);
		app.updateTable(strTableName, "2", htblColNameValue);

//		Polygon polyy=new Polygon();
//		polyy.addPoint(1,88);
//		polyy.addPoint(5,32);
//		polyy.addPoint(14,20);
//		polyy.addPoint(24,360);
//		polyy.addPoint(15,10);
//		
//		Polygon polyy1=new Polygon();
//		polyy1.addPoint(1,88);
//		polyy1.addPoint(6,50);
//		polyy1.addPoint(14,20);
//		
//		//(gpa,1.5), (isHealthy,true), (name,ranoon), (polygon,(1,90)(6,50)(20,20)(20,36) total number of points: 4), (birthday,Wed Apr 10 00:00:00 EET 1968), (id,8), (TouchDate,Mon Apr 20 07:08:51 EET 2020)]
//		
//		Record r = new Record();
//		r.coloumnsOfData.add(new Data(new Double(1.5),"gpa"));
//		r.coloumnsOfData.add(new Data(new Boolean(true),"isHealthy"));
//		r.coloumnsOfData.add(new Data(polyy,"polygon"));
//		r.coloumnsOfData.add(new Data(UpdateDateformatter("1968-04-10"),"birthday"));
//		r.coloumnsOfData.add(new Data("ranoon","name"));
//		Record r2 = new Record();
//		r2.coloumnsOfData.add(new Data(new Double(1.5),"gpa"));
//		r2.coloumnsOfData.add(new Data(new Boolean(true),"isHealthy"));
//		r2.coloumnsOfData.add(new Data(polyy,"polygon"));
//		r2.coloumnsOfData.add(new Data(UpdateDateformatter("1968-04-10"),"birthday"));
//		r2.coloumnsOfData.add(new Data("ranoon","name"));
//		Record r3 = new Record();
//		r3.coloumnsOfData.add(new Data(new Double(1.5),"gpa"));
//		r3.coloumnsOfData.add(new Data(new Boolean(true),"isHealthy"));
//		r3.coloumnsOfData.add(new Data(polyy,"polygon"));
//		r3.coloumnsOfData.add(new Data(UpdateDateformatter("1968-04-10"),"birthday"));
//		r3.coloumnsOfData.add(new Data("ranoon","name"));
//		
//		Record r1 = new Record();
//		r1.coloumnsOfData.add(new Data(new Double(1.5),"gpa"));
//		r1.coloumnsOfData.add(new Data(new Boolean(true),"isHealthy"));
//		r1.coloumnsOfData.add(new Data(polyy1,"polygon"));
//		r1.coloumnsOfData.add(new Data(UpdateDateformatter("1968-04-10"),"birthday"));
//		r1.coloumnsOfData.add(new Data("hoda","name"));
//		
//		Set<Record> s = new HashSet<Record>();
//		s.add(r);
//		s.add(r2);
//		
//		Set<Record> s1 = new HashSet<Record>();
//		s1.add(r1);
//		s1.add(r3);
//		List<Set<Record>> sets = new ArrayList<Set<Record>>();
//		sets.add(s);
//		
//		ArrayList<String> strarrOperators = new ArrayList<String>();
//        strarrOperators.add("OR");
//		
//		Iterator<Record> resultSet = app.CombineSets(sets, strarrOperators, s);
//		
//		
//		while(resultSet.hasNext()) {
//			System.out.println(" Iteration over set in main method ");
//			Record rx = (Record)resultSet.next();
//			
//				System.out.println("SET RESULT MAIN METHOD: " + rx);
//		}
//		
//		Polygon polyy=new Polygon();
//		polyy.addPoint(1,88);
//		polyy.addPoint(5,32);
//		polyy.addPoint(14,20);
//		polyy.addPoint(24,360);
//		polyy.addPoint(15,10);
//		
//		Polygon polyy1 = new Polygon();
//		polyy1.addPoint(1, 88);
//		polyy1.addPoint(6, 50);
//		polyy1.addPoint(14, 20);
//		
//		
//		Polygon polyy2=new Polygon();
//		polyy2.addPoint(1,90);
//		polyy2.addPoint(6,50);
//		polyy2.addPoint(20,20);
//		polyy2.addPoint(20,36);
//		
//		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.double");
//		htblColNameType.put("isHealthy", "java.lang.Boolean");
//		htblColNameType.put("birthday", "java.util.Date");
//		htblColNameType.put("polygon", "java.awt.Polygon");
//		app.createTable(strTableName, "id", htblColNameType);
//		app.createBTreeIndex("Student", "name");
//		// app.createBTreeIndex("Student", "birthday");
//		app.createRTreeIndex("Student", "polygon");
//		inserter2(app, strTableName);

//		BPlusTree ClusterBtree = (BPlusTree) DBApp.deserialize(strTableName + "_" + "birthday" + "Index.txt");
//		System.out.println(ClusterBtree);
		// app.deleteFromTable(strTableName, htblColNameValue);
//		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("id","java.lang.Integer" );
//		htblColNameType.put("gpa","java.lang.Double" );
//		app.createTable(strTableName, "id", htblColNameType);
////		
//		inserter(app,strTableName);
//		app.createBTreeIndex("Student", "id");
//		app.createBTreeIndex("Student", "name");

		// String strTableName = "Student";
		// app.createBTreeIndex("Student", "id");

//		LeafNode trees = (LeafNode) DBApp.deserialize("index_On_name_StudentJohn NoorL.txt");
//		System.out.println(trees);
//		for(int i=0; i<trees.values.size();i++)
//			System.err.println((trees.values.get(i)));
		// System.err.println(Compare(new Boolean(false),new Boolean(true)));
//		SQLTerm[] arrSQLTerms = new SQLTerm[3];
//		arrSQLTerms[0] = new SQLTerm();
//		arrSQLTerms[0].setTableName("Student");
//		arrSQLTerms[0].setColName("id");
//		arrSQLTerms[0].setOperator("=");
//		arrSQLTerms[0].setObjValue(new Integer(1));
//		arrSQLTerms[1] = new SQLTerm();
//		arrSQLTerms[1].setTableName("Student");
//		arrSQLTerms[1].setColName("gpa");
//		arrSQLTerms[1].setOperator("=");
//		arrSQLTerms[1].setObjValue(new Double(1.5));
//		arrSQLTerms[2] = new SQLTerm();
//		arrSQLTerms[2].setTableName("Student");
//		arrSQLTerms[2].setColName("name");
//		arrSQLTerms[2].setOperator("=");
//		arrSQLTerms[2].setObjValue("Hoda");
//		String[] strarrOperators = new String[2];
//		strarrOperators[0] = "AND";
//		strarrOperators[1] = "OR";
//		long startTime = System.currentTimeMillis();
//		Iterator<Record> resultSet = app.selectFromTable(arrSQLTerms, strarrOperators);
//		long endTime = System.currentTimeMillis();
//		System.out.printf("Time for query = %d ms\n", endTime - startTime);
//		while (resultSet.hasNext()) {
//			System.out.println(" Iteration over set in main method ");
//			Record r = (Record) resultSet.next();
//
//			System.out.println("SET RESULT MAIN METHOD: " + r);
//		}
//		
//		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//		htblColNameType.put("coordinates", "java.awt.Polygon");
//		htblColNameType.put("name", "java.lang.String");
//		app.createTable(strTableName, "coordinates", htblColNameType);
//		

//		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//		htblColNameValue.put("isHealhty", new Boolean(true));
//		app.updateTable(strTableName, "3", htblColNameValue);
		Table t = (Table) DBApp.deserialize("Student.txt");
		System.err.println("--------------------------------------------------------");
		for (int i = 0; i < t.NumberOfPages; i++) {
			PagePrinter((Page) DBApp.deserialize("page_Student" + i + ".txt"));
		}

//		app.PagePrinter((Page) DBApp.deserialize("page_Student" + 1 + ".txt"));

//		
//		Page p = (Page) DBApp.deserialize("page_Student" + 1 + ".txt");
//		p.records.clear();
//		Record r1 = new Record();
//		r1.coloumnsOfData.add(new Data(new Integer(4),"id"));
//		r1.coloumnsOfData.add(new Data("Fady","name"));
//		r1.coloumnsOfData.add(new Data(new Double(1.5),"gpa"));
//		p.records.add(r1);
//		Record r2 = new Record();
//		r2.coloumnsOfData.add(new Data(new Integer(4),"id"));
//		r2.coloumnsOfData.add(new Data("3asfoor","name"));
//		r2.coloumnsOfData.add(new Data(new Double(1),"gpa"));
//		p.records.add(r2);
//		Record r3 = new Record();
//		r3.coloumnsOfData.add(new Data(new Integer(5),"id"));
//		r3.coloumnsOfData.add(new Data("Mohsen","name"));
//		r3.coloumnsOfData.add(new Data(new Double(2),"gpa"));
//		p.records.add(r3);
//		serialize("page_Student" + 1 + ".txt",p);
//		
//		Table t = (Table) DBApp.deserialize("Student.txt");
//		Hashtable<String, BPlusTree> test = t.colNameIndex;
//		Enumeration<String> enumeration = test.keys();
//		while (enumeration.hasMoreElements()) {
//
//			String key = enumeration.nextElement();
//
//			System.out.println(test.get(key) + " " + key);
//
//		}

//		app.createBTreeIndex("Student", "id");
//		app.createBTreeIndex("Student", "name");

	}

}
