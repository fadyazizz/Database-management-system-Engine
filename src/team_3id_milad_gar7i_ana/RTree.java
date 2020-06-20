package team_3id_milad_gar7i_ana;

import java.awt.Polygon;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Vector;



public class RTree<K extends Comparable<? super K>, V> implements Serializable{

	public static enum RangePolicy {
		EXCLUSIVE, INCLUSIVE
	}

	/**
	 * The branching factor used when none specified in constructor.
	 */
	private static final int DEFAULT_BRANCHING_FACTOR = 128;

	/**
	 * The branching factor for the B+ tree, that measures the capacity of nodes
	 * (i.e., the number of children nodes) for internal nodes in the tree.
	 */
	private int branchingFactor;

	/**
	 * The root node of the B+ tree.
	 */
	 static String lastSearchedPage="gjhbjhbjh";
	private Node root;
	private String TableName;
	private String ColName;
	  
public RTree() {
		
	}

	public RTree(int branchingFactor,String TableName,String ColName) throws IOException {
		if (branchingFactor <= 2)
			throw new IllegalArgumentException("Illegal branching factor: "
					+ branchingFactor);
		this.branchingFactor = branchingFactor;
		root = new LeafNode(TableName,ColName);
		this.TableName=TableName;
		this.ColName=ColName;
		root.path="index_On_"+ColName+"_"+TableName+"root.txt";
		DBApp.serialize("index_On_"+ColName+"_"+TableName+"root.txt", root);
		 Vector<String> allLeafs=new Vector<String>(1,1);
		 DBApp.serialize("allLeafs"+ColName+"_"+TableName+".txt", allLeafs);
		 Vector<String> allNodesNames=new Vector<String>(1,1);
		 DBApp.serialize("allNodes"+ColName+"_"+TableName+".txt", allNodesNames);
	}
	public Vector<String> search(K key) throws Exception {
		this.root=(Node)DBApp.deserialize("index_On_"+this.ColName+"_"+this.TableName+"root.txt");
		//System.out.println("In searchhhhhhhhh1");
		Vector<String> temp= root.getValue(key);
		
		DBApp.serialize(root.path, root);
		DBApp.serialize(this.TableName+"_"+this.ColName+"Index.txt", this);
		return temp;
	}
	/**
	 * Returns the value to which the specified key is associated, or
	 * {@code null} if this tree contains no association for the key.
	 *
	 * <p>
	 * A return value of {@code null} does not <i>necessarily</i> indicate that
	 * the tree contains no association for the key; it's also possible that the
	 * tree explicitly associates the key to {@code null}.
	 * 
	 * @param key
	 *            the key whose associated value is to be returned
	 * 
	 * @return the value to which the specified key is associated, or
	 *         {@code null} if this tree contains no association for the key
	 */
//	public V search(K key) {
//		return root.getValue(key);
//	}

	/**
	 * Returns the values associated with the keys specified by the range:
	 * {@code key1} and {@code key2}.
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	**/
	static void Delete(String path) {
		File f=new File("data/"+path);
		f.delete();
	}
	 public void checker(String tableName,String indexName) {
		 try {
			Serializable s=(Serializable)DBApp.deserialize(tableName+".txt");
			Table tb=(Table)s;
			
				
				Vector<String> allNodes=(Vector<String>)DBApp.deserialize("allNodes"+indexName+"_"+tableName+".txt");
				for(int i=0;i<allNodes.size();i++) {
					Delete(allNodes.elementAt(i));
				}
				createRTreeIndex(tableName, indexName);
			
			
			
			
			
			
			
			
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 catch(Exception e) {
			 
		 }
	 }
	 
	 
	 public static void createRTreeIndex(String strTableName, String strColName) throws Exception {
			Vector<DataWithType> metadata =DBApp.ReadMetadata(strTableName);
			String colType = null;
			int i;
			for (i = 0; i < metadata.size(); i++) {
				if (strColName.equals(metadata.get(i).colName)) {
					colType = metadata.get(i).colType.toLowerCase();
					break;
				}

			}
			//Vector<String> indexes =DBApp.allIndexes(strTableName);
			

			if (colType == null) {
				System.out.println("Coloumn not found!!!");
				return;

			}

			if (colType.equals("java.lang.boolean")||colType.equals("java.lang.integer")||colType.equals("java.lang.string")|| colType.equals("java.lang.double")||colType.equals("java.util.Date")) {
				System.out.println("can't create index on Boolean or polygon");
				return;
			}

		
//				
		
			
				// System.out.println("-------DateS-------");
				RTree<Polygonn, Vector<String>> index = new RTree<Polygonn, Vector<String>>(DBApp.NodeSize + 1,
						strTableName, strColName);
				DBApp.IndexPopulatorR(index, strTableName, strColName);
				DBApp.serialize(strTableName + "_" + strColName + "Index.txt", index);
			

		}
	 
	/**
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * @param key1
	 *            the start key of the range
	 * @param policy1
	 *            the range policy, {@link RangePolicy#EXCLUSIVE} or
	 *            {@link RangePolicy#INCLUSIVE}
	 * @param key2
	 *            the end end of the range
	 * @param policy2
	 *            the range policy, {@link RangePolicy#EXCLUSIVE} or
	 *            {@link RangePolicy#INCLUSIVE}
	 * @return the values associated with the keys specified by the range:
	 *         {@code key1} and {@code key2}
	 */
	public List<V> searchRange(K key1, RangePolicy policy1, K key2,
			RangePolicy policy2) {
		return root.getRange(key1, policy1, key2, policy2);
	}

	/**
	 * Associates the specified value with the specified key in this tree. If
	 * the tree previously contained a association for the key, the old value is
	 * replaced.
	 * 
	 * @param key
	 *            the key with which the specified value is to be associated
	 * @param value
	 *            the value to be associated with the specified key
	 * @throws Exception 
	 */
	public Vector<String> allNodes(){
		Queue<List<Node>> queue = new LinkedList<List<Node>>();
		Node n;
		try {
			n = (Node)DBApp.deserialize("index_On_"+this.ColName+"_"+this.TableName+"root.txt");
			queue.add(Arrays.asList(n));
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		StringBuilder sb = new StringBuilder();
		Vector<String> values=new Vector<String>(1,1);
		Vector<String> allNode=new Vector<String>(1,1);
		while (!queue.isEmpty()) {
			Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
			while (!queue.isEmpty()) {
				List<Node> nodes = queue.remove();
				sb.append('{');
				Iterator<Node> it = nodes.iterator();
				while (it.hasNext()) {
					//String s=it.next();
					Node node = it.next();
					allNode.add(node.path);
					sb.append(node.toString());
					if (it.hasNext())
						sb.append(", ");
					
					if (node instanceof RTree.InternalNode) {
						InternalNode Node=(InternalNode)node;
						List<Node> l=new LinkedList<Node>() ;
						
						for(int i=0;i<Node.children.size();i++) {
							Node temp;
							try {
								temp = (Node)DBApp.deserialize(Node.children.get(i));
								l.add(temp);
							} catch (ClassNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
							
						}
						nextQueue.add(l);
						
					
					}
					
					
					}
				sb.append('}');
				if (!queue.isEmpty())
					sb.append(", ");
				else
					sb.append('\n');
			}
			queue = nextQueue;
		}
		//System.out.println(values);

		return allNode;
	}
	public void insert(K key, V value) throws Exception {
		this.root=(Node)DBApp.deserialize("index_On_"+this.ColName+"_"+this.TableName+"root.txt");
		root.insertValue(key, value);
		DBApp.serialize(root.path, root);
		Vector<String> allNodes=allNodes();
		DBApp.serialize("allNodes"+this.ColName+"_"+this.TableName+".txt", allNodes);
		//DBApp.serialize(this.TableName+"_"+this.ColName+"Index.txt", this);
	}
	public void PageDeleter(String path,K key) throws IOException, ClassNotFoundException {
		this.root=(Node)DBApp.deserialize("index_On_"+this.ColName+"_"+this.TableName+"root.txt");
		this.root.PageDeleter(path, key);
	DBApp.serialize(root.path, root);
	//DBApp.serialize(this.TableName+"_"+this.ColName+"Index.txt", this);
	}
	/**
	 * Removes the association for the specified key from this tree if present.
	 * 
	 * @param key
	 *            the key whose association is to be removed from the tree
	 * @throws Exception 
	 */
//	public static void deleter(String tableName2, String colName2, Object key) {
//	try {
//		BPlusTree b=(BPlusTree)DBApp.deserialize(tableName2+"_"+colName2+"Index.txt");
//		
//		b.delete((Comparable)key);
//		
//		System.err.println("deleter"+b.allLeafs);
//		
//		
//		
//		
//		
//	} catch (ClassNotFoundException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	} catch (Exception e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
//		
//	}
	public void delete(K key) throws Exception {
		this.root=(Node)DBApp.deserialize("index_On_"+this.ColName+"_"+this.TableName+"root.txt");
		root.deleteValue(key);
		DBApp.serialize(root.path, root);
		DBApp.serialize(this.TableName+"_"+this.ColName+"Index.txt", this);
	}
	

	public String toString(){
		Queue<List<Node>> queue = new LinkedList<List<Node>>();
		Node n;
		try {
			n = (Node)DBApp.deserialize("index_On_"+this.ColName+"_"+this.TableName+"root.txt");
			queue.add(Arrays.asList(n));
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		StringBuilder sb = new StringBuilder();
		Vector<String> values=new Vector<String>(1,1);
		while (!queue.isEmpty()) {
			Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
			while (!queue.isEmpty()) {
				List<Node> nodes = queue.remove();
				sb.append('{');
				Iterator<Node> it = nodes.iterator();
				while (it.hasNext()) {
					//String s=it.next();
					Node node = it.next();
					
					sb.append(node.toString());
					if (it.hasNext())
						sb.append(", ");
					
					if (node instanceof RTree.InternalNode) {
						InternalNode Node=(InternalNode)node;
						List<Node> l=new LinkedList<Node>() ;
						
						for(int i=0;i<Node.children.size();i++) {
							Node temp;
							try {
								temp = (Node)DBApp.deserialize(Node.children.get(i));
								l.add(temp);
							} catch (ClassNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
							
						}
						nextQueue.add(l);
						
					
					}
					else {
						LeafNode Node=(LeafNode)node;
						for(int i=0;i<Node.values.size();i++) {
							for(int j=0;j<Node.values.get(i).size();j++) {
								values.add(Node.values.get(i).elementAt(j));
							}
							values.add("------end Of"+Node.keys.get(i)+ " -----"+"\n");
						}
					}
					
					}
				sb.append('}');
				if (!queue.isEmpty())
					sb.append(", ");
				else
					sb.append('\n');
			}
			queue = nextQueue;
		}
		System.out.println(values);

		return sb.toString();
	}

	 abstract class Node implements Serializable {
		List<K> keys;
		String ColName;
		String tableName;
		String path;
		
		int keyNumber() {
			return keys.size();
		}

		abstract Vector<String> getValue(K key) throws Exception;

		abstract void deleteValue(K key) throws Exception ;

		abstract void insertValue(K key, V value) throws Exception;

		abstract K getFirstLeafKey() throws Exception;

		abstract List<V> getRange(K key1, RangePolicy policy1, K key2,
				RangePolicy policy2);

		abstract void merge(Node sibling) throws Exception;

		abstract Node split();

		abstract boolean isOverflow();

		abstract boolean isUnderflow();
		abstract void Delete();
		abstract void PageDeleter(String path,K key);

		public String toString() {
			return keys.toString();
		}
	}

	 class InternalNode extends Node implements Serializable {
		List<String> children;

		InternalNode(String tableName, String colName) {
			this.keys = new ArrayList<K>();
			this.children = new ArrayList<String>();
			this.tableName=tableName;
			this.ColName=colName;
		}

		@Override
		Vector<String> getValue(K key) throws Exception  {
			//System.out.println("get ValueeeInternal");
			Node child=(Node)DBApp.deserialize(this.getChild(key));
			return child.getValue(key);
		}

		@Override
		void deleteValue(K key) throws Exception {
			//System.out.println("sdaghvahvfagh_______DSA_D_AD_D_AS_DAS_D_SAD_D_D_A_D");
			//Vector<String> allLeafs=(Vector<String>)DBApp.deserialize("allLeafs"+ColName+"_"+TableName+".txt");
			String path=getChild(key);
			Node child = (Node)DBApp.deserialize(path);
			child.deleteValue(key);
			if (child.isUnderflow()) {
				String ChildLeft= getChildLeftSibling(key);
				Node childLeftSibling=null;
				if(ChildLeft!=null) {
					childLeftSibling =(Node)DBApp.deserialize(ChildLeft);
				}
				String ChildRight= getChildRightSibling(key);
				Node childRightSibling =null;
				if(ChildRight!=null) {
					childRightSibling=(Node)DBApp.deserialize(ChildRight);
				}
				Node left = childLeftSibling != null ? childLeftSibling : child;
				Node right = childLeftSibling != null ? child: childRightSibling;
				left.merge(right);
				deleteChild(right.getFirstLeafKey());
				//right.Delete();
				//allLeafs.remove(right.path);
				if (left.isOverflow()) {
					Node sibling = left.split();
					if(sibling instanceof RTree.LeafNode) {
					sibling.path="index_On_"+sibling.ColName+"_"+sibling.tableName+sibling.keys.get(0)+"L.txt";
				//	allLeafs.add(sibling.path);
					}
					else {
						sibling.path="index_On_"+sibling.ColName+"_"+sibling.tableName+sibling.keys.get(0)+"I.txt";
					}
					insertChild(sibling.getFirstLeafKey(), sibling);
					DBApp.serialize(sibling.path, sibling);
				}
				//left.Delete();
				//allLeafs.remove(left.path);
				if(left instanceof RTree.LeafNode) {
					left.path="index_On_"+left.ColName+"_"+left.tableName+left.keys.get(0)+"L.txt";
				//	allLeafs.add(left.path);
					}
					else {
						left.path="index_On_"+left.ColName+"_"+left.tableName+left.keys.get(0)+"I.txt";
					}
				DBApp.serialize(left.path, left);
				
				if (root.keyNumber() == 0) {
					///left.Delete();
					System.out.println("leftkeys "+left.keys);
					//allLeafs.remove(left.path);
					
					left.path=root.path;
					root = left;
					
					DBApp.serialize(root.path, root);
				//	DBApp.serialize("allLeafs"+ColName+"_"+TableName+".txt", allLeafs);
					return;
				}
				
				
				}
			
			//child.Delete();
			//allLeafs.remove(child.path);
			if(child instanceof RTree.LeafNode) {
			child.path="index_On_"+child.ColName+"_"+child.tableName+child.keys.get(0)+"L.txt";
			//allLeafs.add(child.path);
			}
			else {
				child.path="index_On_"+child.ColName+"_"+child.tableName+child.keys.get(0)+"I.txt";
			}
			//DBApp.serialize(child.path, child);
			this.insertChild(child, child.keys.get(0));
			if(!this.path.equals(root.path)) {
				//this.Delete();
				this.path="index_On_"+this.ColName+"_"+this.tableName+this.keys.get(0)+"I.txt";
				//DBApp.serialize(this.path, this);
			}
			else {
				//DBApp.serialize(root.path, root);
			}
			
			
		//	DBApp.serialize("allLeafs"+ColName+"_"+TableName+".txt", allLeafs);
			
			
			
			
		}
		void Delete() {
			File f=new File("data/"+this.path);
			f.delete();
		}
		@Override
		void insertValue(K key, V value) throws Exception {
			String child = getChild(key);
			//Vector<String> allLeafs=(Vector<String>)DBApp.deserialize("allLeafs"+ColName+"_"+TableName+".txt");
			Node childNode=(Node)DBApp.deserialize(child);
			childNode.insertValue(key, value);
			//this.Delete();
			Vector<String> allNodes=(Vector<String>)DBApp.deserialize("allNodes"+ColName+"_"+TableName+".txt");
			if (childNode.isOverflow()) {   
				Node sibling = childNode.split();
				if(sibling instanceof RTree.LeafNode ) {
					sibling.path="index_On_"+sibling.ColName+"_"+sibling.tableName+sibling.keys.get(0)+"L"+".txt";
				
					
				}
				else {
					sibling.path="index_On_"+sibling.ColName+"_"+sibling.tableName+sibling.keys.get(0)+"I"+".txt";
				}
				//System.out.println(sibling.getFirstLeafKey()+" ------------------key------__");
				insertChild(sibling.getFirstLeafKey(), sibling);
				//this.insertChild(sibling,sibling.getFirstLeafKey() );
				if(!allNodes.contains(sibling.path)) {
					allNodes.add(sibling.path);
				}
				DBApp.serialize(sibling.path, sibling);
				
				
			}
			childNode.Delete();
			allNodes.remove(childNode.path);
			//allLeafs.remove(childNode.path);
			if(childNode instanceof RTree.InternalNode) {
				childNode.path="index_On_"+childNode.ColName+"_"+childNode.tableName+childNode.keys.get(0)+"I"+".txt";
			}
			else {
				
				
				childNode.path="index_On_"+childNode.ColName+"_"+childNode.tableName+childNode.keys.get(0)+"L"+".txt";
				
			}
			
			this.insertChild(childNode,childNode.keys.get(0));
			DBApp.serialize(childNode.path, childNode);
			if(!allNodes.contains(childNode.path)) {
				allNodes.add(childNode.path);
			}
			if (root.isOverflow()) {
				allNodes.remove(this.path);
				this.Delete();
				Node sibling = split();
				
				sibling.path="index_On_"+sibling.ColName+"_"+sibling.tableName+sibling.keys.get(0).toString()+"I.txt";
				InternalNode newRoot = new InternalNode(root.tableName,root.ColName);
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add("index_On_"+this.ColName+"_"+this.tableName+this.keys.get(0).toString()+"I"+".txt");
				newRoot.children.add("index_On_"+sibling.ColName+"_"+sibling.tableName+sibling.keys.get(0).toString()+"I.txt");
				
				newRoot.path=root.path;
				this.path="index_On_"+this.ColName+"_"+this.tableName+this.keys.get(0).toString()+"I"+".txt";
				root = newRoot;
				
				DBApp.serialize("index_On_"+root.ColName+"_"+root.tableName+"root.txt", newRoot);
				DBApp.serialize(sibling.path, sibling);
				DBApp.serialize(this.path, this);
				if(!allNodes.contains(sibling.path)) {
					allNodes.add(sibling.path);
				}
				if(!allNodes.contains(this.path)) {
					allNodes.add(this.path);
				}
				if(!allNodes.contains(root.path)) {
					allNodes.add(root.path);
				}
				return;
			}
			this.Delete();
			allNodes.remove(this.path);
			if(!this.path.equals(root.path)) {
				this.path="index_On_"+this.ColName+"_"+this.tableName+this.keys.get(0)+"I"+".txt";
				DBApp.serialize(this.path, this);	
				if(!allNodes.contains(this.path)) {
					allNodes.add(this.path);
				}
			}
			else {
				DBApp.serialize(root.path,root );
				if(!allNodes.contains(root.path)) {
					allNodes.add(root.path);
				}
			}
			
		//	DBApp.serialize("allLeafs"+ColName+"_"+TableName+".txt", allLeafs);
			DBApp.serialize("allNodes"+ColName+"_"+TableName+".txt", allNodes);
				
		}

		private void insertChild(RTree<K, V>.Node childNode,K key) {
			int index=0;
			for(int i=0;i<this.keys.size();i++) {
				if(key.compareTo(this.keys.get(i))>=0) {
					index=i+1;
				}
			}
			//System.out.println("Keys size: "+this.keys.size()+" Children size "+this.children.size());
			if(this.keys.size()<this.children.size()) {
			//	System.out.println("--------------- Child Path -----------------"+childNode.path);
				this.children.set(index, childNode.path);
			}
			
			
			
		}

		@Override
		K getFirstLeafKey() throws Exception {
			Node n=(Node)DBApp.deserialize(this.children.get(0));
			
			return n.getFirstLeafKey();
		}

		@Override
		List<V> getRange(K key1, RangePolicy policy1, K key2,
				RangePolicy policy2) {
			return null;
			//return getChild(key1).getRange(key1, policy1, key2, policy2);
		}

		@Override
		void merge(Node sibling) throws Exception {
			@SuppressWarnings("unchecked")
			InternalNode node = (InternalNode) sibling;
			keys.add(node.getFirstLeafKey());
			keys.addAll(node.keys);
			children.addAll(node.children);

		}

		@Override
		Node split() {
			int from = keyNumber() / 2 + 1, to = keyNumber();
			InternalNode sibling = new InternalNode(this.tableName,this.ColName);
			sibling.keys.addAll(keys.subList(from, to));
			sibling.children.addAll(children.subList(from, to + 1));

			keys.subList(from - 1, to).clear();
			children.subList(from, to + 1).clear();

			return sibling;
		}

		@Override
		boolean isOverflow() {
			return children.size() > branchingFactor;
		}

		@Override
		boolean isUnderflow() {
			return children.size() < (branchingFactor + 1) / 2;
		}

		String getChild(K key) {
			//System.out.println("PAGE PATH   "+this.path+" Keys  "+this.keys+" key "+key);
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			return children.get(childIndex);
		}

		void deleteChild(K key) {
			int loc = Collections.binarySearch(keys, key);
			if (loc >= 0) {
				keys.remove(loc);
				children.remove(loc + 1);
			}else {
				loc=-(loc)-2;
				if(loc<0) {
					return;
				}
				System.out.println("else"+keys.remove(loc));
				
				children.remove(loc + 1);
			}
		}

		void insertChild(K key, Node child) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (loc >= 0) {
				//System.out.println(childIndex+"   asds"+child.path);
				children.set(childIndex, child.path);
			} else {
				//System.out.println(childIndex+"   else "+child.path);
				keys.add(childIndex, key);
				children.add(childIndex + 1, child.path);
			}
			//crrcrcrc
		}

		String getChildLeftSibling(K key) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex > 0)
				return children.get(childIndex - 1);

			return null;
		}

		String getChildRightSibling(K key) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex < keyNumber())
				return children.get(childIndex + 1);

			return null;
		}

		@Override
		void PageDeleter(String path, K key) {
			String child=getChild(key);
			Node childNode;
			try {
				childNode = (Node)DBApp.deserialize(child);
				childNode.PageDeleter(path, key);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}

		
		
	
	}
	 class LeafNode extends Node implements Serializable  {
		List<Vector<String>> values;
		String next;

		public LeafNode(String tableName, String colName) {
			keys = new ArrayList<K>();
			values = new ArrayList<Vector<String>>();
			this.ColName=colName;
			this.tableName=tableName;
		}

		@Override
		Vector<String> getValue(K key) {
			int loc = Collections.binarySearch(keys, key);
			//System.out.println("get Valueeeleaf");
			//System.out.println("----------------------------Get Valueeee-------------------" + this.path);
			lastSearchedPage=this.path;
			//h.add("sadad");
			return loc >= 0 ? values.get(loc) : null;
		}

		@Override
		void deleteValue(K key) {
			try {
			int loc = Collections.binarySearch(keys, key);
			if (loc >= 0) {
				keys.remove(loc);
				values.remove(loc);
			}
			
			}
			catch(Exception e) {
				
			}
		}
		
		
		
		void PageDeleter(String path,K key) {
			int loc = Collections.binarySearch(keys, key);
			if(loc<0) {
				System.err.println("Yastaaaa hatbawzli el table yasta da5l value mawgouda!!!!");
				return;
			}
			System.err.println("Yastaaaa ana fe page deleter");
			Vector<String> values=this.values.get(loc);
			for(int i=0;i<values.size();i++) {
			//	System.out.println("=======Page Deleter000 ======== "+i+" "+path);
				if(values.elementAt(i).equals(path)) {
					System.out.println("=======Page Deleter ======== "+i+" "+path);
					System.out.println("______________________________");
					System.out.println(values+"     "+i);
					values.remove(i);
					System.out.println(values+"     "+i);
					break;
				}
			}
			
			try {
				DBApp.serialize(this.path, this);
				LeafNode n = (LeafNode)DBApp.deserialize(this.path);
				System.out.println(" AMMMMMMMMMMMMMMMMMMMMMMMMMMMMM " + n.values);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
		@Override
		void insertValue(K key, V value) throws Exception {
			//Vector<String> allLeafs=(Vector<String>)DBApp.deserialize("allLeafs"+ColName+"_"+TableName+".txt");
			Vector<String> allNodes=(Vector<String>)DBApp.deserialize("allNodes"+ColName+"_"+TableName+".txt");
			int loc = Collections.binarySearch(keys, key);
			int valueIndex = loc >= 0 ? loc : -loc - 1;
			if (loc >= 0) {
				//System.out.println("hopa kilo baboooooooooooooooooo");
				Vector<String> temp=values.get(valueIndex);
				int i=0;
				for(i=0;i<temp.size();i++) {
					String s=temp.elementAt(i);
					int valuetogetinserted=DBApp.getPageNumber((String)value);
					int valueAtI=DBApp.getPageNumber(s);
					if(valueAtI>=valuetogetinserted) {
						break;
					}
					
				}
				if(i==temp.size()) {
					values.get(valueIndex).add((String)value);
				}
				else {
					values.get(valueIndex).add(i, (String)value);
				}
					
					
					
					//temp.sort(null);
					//sort so that null values always come last?
				
			} else {
				keys.add(valueIndex, key);
				Vector<String> data=new Vector<String>(1,1);
				data.add((String)value);
				values.add(valueIndex, data);
			}
			//this.Delete();
			if (root.isOverflow()) {
				//System.out.println("asndbjab");
				//allLeafs.remove(root.path);
				Node sibling = split();
				InternalNode newRoot = new InternalNode(this.tableName,this.ColName);
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add("index_On_"+this.ColName+"_"+this.tableName+this.keys.get(0)+"L.txt");
				newRoot.path=root.path;
				// this.Delete();
				this.path="index_On_"+this.ColName+"_"+this.tableName+this.keys.get(0)+"L.txt";
				newRoot.children.add("index_On_"+sibling.ColName+"_"+sibling.tableName+sibling.keys.get(0)+"L.txt");
				sibling.path="index_On_"+sibling.ColName+"_"+sibling.tableName+sibling.keys.get(0)+"L.txt";
			//	allLeafs.add(this.path);
			//	allLeafs.add(sibling.path);
				root = newRoot;
				
				DBApp.serialize(root.path, root);
				DBApp.serialize("index_On_"+sibling.ColName+"_"+sibling.tableName+sibling.keys.get(0)+"L.txt", sibling);
				DBApp.serialize(this.path, this);
				
				if(!allNodes.contains(sibling.path)) {
					allNodes.add(sibling.path);
				}
				if(!allNodes.contains(this.path)) {
					allNodes.add(this.path);
				}
				if(!allNodes.contains(root.path)) {
					allNodes.add(root.path);
				}
				//DBApp.serialize("allLeafs"+ColName+"_"+TableName+".txt", allLeafs);
				DBApp.serialize("allNodes"+ColName+"_"+TableName+".txt", allNodes);
				return;
			}
			if(root instanceof RTree.LeafNode  ) {
				if(!allNodes.contains(root.path)) {
					allNodes.add(root.path);
				}
				
			}
			//DBApp.serialize("allLeafs"+ColName+"_"+TableName+".txt", allLeafs);
			DBApp.serialize("allNodes"+ColName+"_"+TableName+".txt", allNodes);
			
		}

		@Override
		K getFirstLeafKey() {
			return keys.get(0);
		}

		@Override
		List<V> getRange(K key1, RangePolicy policy1, K key2,
				RangePolicy policy2) {
			List<V> result = new LinkedList<V>();
//			LeafNode node = this;
//			while (node != null) {
//				Iterator<K> kIt = node.keys.iterator();
//				Iterator<V> vIt = node.values.iterator();
//				while (kIt.hasNext()) {
//					K key = kIt.next();
//					V value = vIt.next();
//					int cmp1 = key.compareTo(key1);
//					int cmp2 = key.compareTo(key2);
//					if (((policy1 == RangePolicy.EXCLUSIVE && cmp1 > 0) || (policy1 == RangePolicy.INCLUSIVE && cmp1 >= 0))
//							&& ((policy2 == RangePolicy.EXCLUSIVE && cmp2 < 0) || (policy2 == RangePolicy.INCLUSIVE && cmp2 <= 0)))
//						result.add(value);
//					else if ((policy2 == RangePolicy.EXCLUSIVE && cmp2 >= 0)
//							|| (policy2 == RangePolicy.INCLUSIVE && cmp2 > 0))
//						return result;
//				}
//				node = node.next;
//			}
			return result;
		}

		@Override
		void merge(Node sibling) {
			@SuppressWarnings("unchecked")
			LeafNode node = (LeafNode) sibling;
			keys.addAll(node.keys);
			values.addAll(node.values);
			next = node.next;
		}

		@Override
		Node split() {
			LeafNode sibling = new LeafNode(this.tableName,this.ColName);
			int from = (keyNumber() + 1) / 2, to = keyNumber();
			sibling.keys.addAll(keys.subList(from, to));
			sibling.values.addAll(values.subList(from, to));

			keys.subList(from, to).clear();
			values.subList(from, to).clear();

			sibling.next = next;
			next = sibling.path;
			return sibling;
		}

		@Override
		boolean isOverflow() {
			return values.size() > branchingFactor - 1;
		}

		@Override
		boolean isUnderflow() {
			return values.size() < branchingFactor / 2;
		}

		@Override
		void Delete() {
			// TODO Auto-generated method stub
			File f=new File("data/"+this.path);
			//System.out.println(this.path+"------");
			f.delete();
		}
	}
	 
	 public static void main(String[] args) throws Exception {
		 
		 RTree r = new RTree(4,"Poly","coordinates");
		 int[] xpoints = new int[2];
		 int[] ypoints = new int[2];
		 xpoints[0]=1;
		 xpoints[1]=2;
		 ypoints[0]=1;
		 ypoints[1]=2;
		 
		 r.insert(new Polygonn(xpoints, ypoints, 2), "kitty");
		 System.out.println("HHHHHHH");
		 System.out.println(r);
	 }
	
	
}
