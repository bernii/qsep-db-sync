package pl.extensa.qsep;

/**
 * Represents a column inside the table (type, value, size etc.)
 * QSEP DB has 3 types: integer, string and double 
 * @author berni
 *
 */
public class TableColumn implements Cloneable{
	String name;
	int type;
	int size;
	int index;
	long intValue;
	double doubleValue;
	String stringValue;
	
	protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	public String getValue() {
		switch (type) {
			case FieldType.INTEGER:
				return  "i_" + intValue;
			case FieldType.STRING:
				return stringValue;
			case FieldType.DOUBLE:
				return "d_" + doubleValue;
		}
		return null;
	}
	public double getDoubleValue() throws Exception{
		if(type == FieldType.DOUBLE){
			return doubleValue;
		}else{
			throw new Exception("Not a double");
		}
	}
}