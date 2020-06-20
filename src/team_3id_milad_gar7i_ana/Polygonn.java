package team_3id_milad_gar7i_ana;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Polygon;
import java.util.ArrayList;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Polygonn extends Polygon implements Comparable<Polygonn> {

	// static boolean isArea = false;

	public Polygonn(int[] xpoints, int[] ypoints, int npoints) {
		super(xpoints, ypoints, npoints);

	}

	public Polygonn(Polygon poly) {
		super(poly.xpoints, poly.ypoints, poly.npoints);
	}

	public int getArea() {
		Dimension dim = this.getBounds().getSize();
		return dim.width * dim.height;
	}

	public int compareTo(Polygonn dataToBeInsertedPolygon) {
		// TODO Auto-generated method stub

		Dimension dim = this.getBounds().getSize();
		Polygonn p = (Polygonn) dataToBeInsertedPolygon;
		Dimension dim2 = p.getBounds().getSize();
		if ((dim.width * dim.height) > (dim2.width * dim2.height)) {
			return 1;
		}
		if (dim.width * dim.height < dim2.width * dim2.height) {
			return -1;
		}
		return 0;

	}

	public boolean equals(Polygonn p) {
		Point point = new Point();
		if (p.npoints != this.npoints) {
			return false;
		}
		Vector<Point> Ppoints = new Vector<Point>(1, 1);
		Vector<Point> thisPoints = new Vector<Point>(1, 1);
		for (int i = 0; i < this.npoints; i++) {
			Point tempP = new Point(p.xpoints[i], p.ypoints[i]);
			Ppoints.add(tempP);
			tempP = new Point(this.xpoints[i], this.ypoints[i]);
			thisPoints.add(tempP);
		}
		for (int i = 0; i < p.npoints; i++) {
			if (!thisPoints.contains(Ppoints.elementAt(i))) {
				return false;
			}
		}
		return true;
	}

	public static Polygonn getPolygon(String poly) {

		Pattern p = Pattern.compile("\\d+");
		Matcher m = p.matcher(poly);

		ArrayList<Integer> values = new ArrayList<>();
		while (m.find())
			values.add(Integer.parseInt(m.group()));

		ArrayList<Point> points = new ArrayList<>();
		if (values.size() % 2 == 0)
			for (int i = 0; i < values.size(); i += 2)
				points.add(new Point(values.get(i), values.get(i + 1)));

		ArrayList<Integer> xpoints = new ArrayList<Integer>();
		ArrayList<Integer> ypoints = new ArrayList<Integer>();
		int npoints = points.size();
		for (Point point : points) {// 1,1

			xpoints.add(point.x);
			ypoints.add(point.y);

			System.out.println(point.x + ", " + point.y);
		}

		int[] x = new int[xpoints.size()];
		int[] y = new int[ypoints.size()];
		for (int i = 0; i < xpoints.size(); i++) {
			x[i] = xpoints.get(i);
			y[i] = ypoints.get(i);
		}
		Polygonn po = new Polygonn(new Polygon(x, y, npoints));
		return po;
	}

	public String toString() {
		String coordinate = "";
		for (int i = 0; i < this.xpoints.length; i++) {
			coordinate += "(" + this.xpoints[i] + "," + this.ypoints[i] + ")";
		}
		coordinate += " total number of points: " + this.npoints;
		return coordinate;
	}
	
	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}

}
