package pl.extensa.qsep;

import java.util.ArrayList;

/**
 * Represents a single row of the table and a table definition
 * @author berni
 *
 */
public class TableDefinition implements Cloneable{
	public int index; // rawTaableIndex of the table
	int fieldSize = 0;
	ArrayList<TableColumn>columns = new ArrayList<TableColumn>();
	int count = -1;
	private int id_index = -1;
	
	protected Object clone() throws CloneNotSupportedException {
        TableDefinition a = (TableDefinition) super.clone();
        ArrayList<TableColumn> cols = new ArrayList<TableColumn>(this.columns.size());
        for (TableColumn tableColumn : this.columns) {
			cols.add((TableColumn) tableColumn.clone());
		}
        a.columns = cols;
        return a;
    }
	
	/**
	 * Parse data from buffer accordingly with each column definition
	 * @param buff with data in byte buffer
	 * @return ArrayList of TableColumns containing data
	 */
	public ArrayList<TableColumn> parseData(byte[] buff){
		int i = 0;
		for (TableColumn cd : columns) {
			switch (cd.type) {
				case FieldType.INTEGER:
					long value = 0;
					for (int a = 0; a < cd.size; a++)
					{
					   value += ((long) buff[a + i] & 0xffL) << (8 * a);
					}
					cd.intValue = value;
					break;
					
				case FieldType.DOUBLE:
					value = 0;
					for (int a = 0; a < cd.size; a++)
					{
					   value += ((long) buff[a + i] & 0xffL) << (8 * a);
					}
					cd.doubleValue = java.lang.Double.longBitsToDouble(value);
					break;
					
				case FieldType.STRING:
					cd.stringValue = new String(buff, i, cd.size);
					break;
			}
			i += cd.size;					
		}
		return columns;
	}
	
	/**
	 * Check if there is an ID column
	 * @return ID column index or -1 if it doesn't exist 
	 */
	public int getIdIndex() {
        if(this.id_index == -1){
	        for (TableColumn cd : columns) {
				if(cd.name.equalsIgnoreCase("ID")){
					id_index = cd.index;
				}
			}
        }
        return id_index;
	}
	
	/**
	 * Get particular column size
	 * @param index of column to check
	 * @return column size
	 */
	public int columnSize(int index) {
		TableColumn elem = columns.get(index);
		return elem.size;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("I: " + index + " fieldsize " + fieldSize + " count " + count + " ");
		for (TableColumn col : columns) {
			sb.append(col.index + ": " + col.name + " size:" + col.size + " " + col.getValue()+ " ");
		}
		return sb.toString();
		
	}

	public TableColumn getField(String name) {
		for (TableColumn cd : columns) {
			if(cd.name.equalsIgnoreCase(name)){
				return cd;
			}
		}
		return null;
	}
}