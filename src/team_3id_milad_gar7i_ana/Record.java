package team_3id_milad_gar7i_ana;

import java.awt.Polygon;
import java.io.Serializable;
import java.util.Vector;

public class Record implements Serializable {

	public Vector<Data> coloumnsOfData;

	public Record() {
		coloumnsOfData = new Vector<Data>(1, 1);
	}

	@Override

	public boolean equals(Object obj) {
		boolean check = true;

		if (obj == null || !(obj instanceof Record)) {
			check = false;
		}
		for (int i = 0; i < this.coloumnsOfData.size(); i++) {
			Data data1 = ((Record) obj).coloumnsOfData.get(i);
			Data data2 = this.coloumnsOfData.get(i);

			if (data1.value instanceof Polygon && data2.value instanceof Polygon) {
				Polygonn value1 = new Polygonn((Polygon) data1.value);
				Polygonn value2 = new Polygonn((Polygon) data2.value);

				if (!((data1.name).equals(data2.name)) || !((value1).equals(value2))) {
					return false;
				}

			} else {
				Object value1 = data1.value;
				Object value2 = data2.value;

				if (!((data1.name).equals(data2.name)) || !((value1).equals(value2))) {
					return false;
				}
			}
		}

		return check;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;

		for (int i = 0; i < this.coloumnsOfData.size(); i++) {
			Data data1 = this.coloumnsOfData.get(i);
			if (data1.value instanceof Polygon) {
				Polygonn value1 = new Polygonn((Polygon) data1.value);
				result = prime * result + ((data1.name == null) ? 0 : data1.name.hashCode());
				result = prime * result + ((value1 == null) ? 0 : value1.hashCode());

			} else {
				Object value1 = data1.value;
				result = prime * result + ((data1.name == null) ? 0 : data1.name.hashCode());
				result = prime * result + ((value1 == null) ? 0 : value1.hashCode());
			}

		}
		return result;
	}

	@Override
	public String toString() {
		String s = "";
		s += "" + this.coloumnsOfData;

		return s;
	}

}
