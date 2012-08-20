package pl.extensa.qsep;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Observable;


/**
 * QSEP server integration lib. Allows the retrieval of database data from 
 * custom QSEP database used in products of QBS company (http://www.qbs.com.pl/).
 * This lib was created using tech docs delivered by QBS. Took some time to implement in Java ;)
 * @author berni
 *
 */

public class QSepIntegration extends Observable{
	
	public static class EVENT{
		public static final int ROW_READ = 1;
	}
	
	/**
	 * Blocking function to read cnt bytes from the socket. 
	 * Buffer size must be smaller or equal to the number of chars to get.
	 * @param s socket to read from
	 * @param b characters buffer
	 * @param cnt number of characters to get
	 * @throws IOException on communication error when socket is prematurely closed (without reading all the bytes)
	 */
	static void readBytes(Socket s, byte b[], int cnt) throws IOException 
	{
		int bytesRead = 0;
		int c = 0;
		while(bytesRead < cnt) {
			c = s.getInputStream().read(b, bytesRead, cnt-bytesRead);
			if (c > 0) bytesRead += c;
			else if (c == -1) {
				throw new IOException("Socket closed during readBytes");
			}
		}
	}
	
	/**
	 * Read int number in Little-Endian format
	 * @param s - socket with opened connection
	 * @return integer number
	 * @throws IOException
	 */
	static int readInt(Socket s) throws IOException
	{
		byte b[] = new byte[4];
		readBytes(s, b, 4);
		return ((b[3] & 0xff) << 24) | ((b[2] & 0xff) << 16) | ((b[1] & 0xff) << 8) | (b[0] & 0xff);
	}

	/**
	 * Write int number to the socket (L-E format)
	 * @param s opened connection
	 * @param x number to write
	 * @throws IOException
	 */
	static void writeInt(Socket s, int x) throws IOException
	{
		byte b[] = new byte[4];
		b[0] = (byte)(x & 255);
		b[1] = (byte)((x >> 8) & 255);
		b[2] = (byte)((x >> 16) & 255);
		b[3] = (byte)((x >> 24) & 255);
		writeBytes(s, b, 4);
	}
	
	static void writeBytes(Socket s, byte b[], int size) throws IOException
	{
		s.getOutputStream().write(b, 0, size);
	}
	
	/**
	 * Read string from the socket.
	 * Strings are formatted along with the Delphi convention. First we read a number telling what 
	 * is the length of string and then we read appropriate number of characters.
	 * @param s scoket we read from
	 * @return string read from socket
	 * @throws IOException
	 */
	static String readString(Socket s) throws IOException
	{
		int len = readInt(s);
		byte str[] = new byte[len];
		readBytes(s, str, len);
		return new String(str);
	}
	
	/**
	 * Read all the records from the DB table
	 * @param tableHandle handle to the table you want to read records from
	 * @param sock active server connection socket
	 * @param td table definition (fetched with readTableStruncture)
	 * @throws Exception
	 */
	ArrayList<TableDefinition> readRecords(int tableHandle, Socket sock, TableDefinition td) throws Exception{
		ArrayList<TableDefinition> records = new ArrayList<TableDefinition>();
		/* Read first record from the table */
		writeInt(sock, 10);					/* cc_TableSetStatus */
		writeInt(sock, tableHandle);		/* Table_Handle */
		writeInt(sock, 2);					/* Table_Status = 2, ts_Bof */
		writeInt(sock, 2);					/* cc_Flush */
		
		int tableStatus = readInt(sock);
		
		if (tableStatus != 2)
			throw new Exception("Error setting table status: Table_Status = " + tableStatus);

		System.out.println("Cursor set at the begining of the table.");
		
		/** Loop through all rows in table **/
		tableStatus = 0;
		int elementsCount = 0;
		final int elementReadCount = 2000;
		
		// Move the cursor to first table element
		writeInt(sock, 16);					/* cc_TableMoveBy */
		writeInt(sock, tableHandle);		/* Table_Handle */
		writeInt(sock, 1);					/* move by 1 */
		writeInt(sock, 2);					/* cc_Flush */
		
		tableStatus = readInt(sock);
		int count = readInt(sock);
					
		while(tableStatus == 0){
			writeInt(sock, 17);					/* cc_TableMultiGetForward */
			writeInt(sock, tableHandle);		/* Table_Handle */
			writeInt(sock, elementReadCount);	/* Read elementReadCount records packs / Count */
			writeInt(sock, 2);					/* cc_Flush */
						
			for (int i = 0; i < elementReadCount; i++) {
				tableStatus = readInt(sock);
				if (tableStatus != 0){
					// We reached the end of the table
					if (tableStatus == 4){
						System.out.println("EOF: Table_Status = " + tableStatus);
						break;
					}
					throw new Exception("Error during table read: Table_Status = " + tableStatus);
				}
				td = (TableDefinition) td.clone();
				int record_size = td.fieldSize + (td.count % 8 != 0 ? td.count/8 + 1 : td.count/8);
				byte REC[] = new byte[record_size];          /* REC buffor size: 32 + 8 + 1 */
				readBytes(sock, REC, record_size);			/* read recird */			
				td.parseData(REC);

				records.add(td);
				elementsCount++;
			}
			// Notify observers that table part has been read
			setChanged();
			notifyObservers(EVENT.ROW_READ);
			if (tableStatus == 4)
				break;
			tableStatus = readInt(sock);
			if (tableStatus != 0){
				throw new Exception("End: Error during table read: Table_Status = " + tableStatus);
			}
		}
		System.out.println("Total elements count: "+ elementsCount);
		return records;
	}
	
	/** Read records row by row (not a bulk read). Really really reallllyyy slowww!
	 * Read all the records from a DB table
	 * @param tableHandle handle to the table you want to read records from
	 * @param sock active server connection socket
	 * @param td table definition (fetched with readTableStruncture)
	 * @throws Exception
	 */
	ArrayList<TableDefinition> readRecordsSingle(int tableHandle, Socket sock, TableDefinition td) throws Exception{
		ArrayList<TableDefinition> records = new ArrayList<TableDefinition>();
		/* Read first record from the table */
		
		writeInt(sock, 10);					/* cc_TableSetStatus */
		writeInt(sock, tableHandle);		/* Table_Handle */
		writeInt(sock, 2);					/* Table_Status = 2, ts_Bof */
		writeInt(sock, 2);					/* cc_Flush */
		
		int tableStatus = readInt(sock);
		
		if (tableStatus != 2)
			throw new Exception("Error setting table status: Table_Status = " + tableStatus);

		System.out.println("Cursor set at the begining of the table.");
		
		/** Loop through all rows in table **/
		tableStatus = 0;
		int elementsCount = 0;
		while(tableStatus == 0){
			// Move the cursor by one element
			writeInt(sock, 16);					/* cc_TableMoveBy */
			writeInt(sock, tableHandle);		/* Table_Handle */
			writeInt(sock, 1);					/* move by 1 */
			writeInt(sock, 2);					/* cc_Flush */
			
			tableStatus = readInt(sock);
			int count = readInt(sock);

			if(tableStatus == 4){ // EOF - the end of the table
				System.out.println("TABLE STATUS = 4 ");
				break;
			}
			if (tableStatus != 0)
				throw new Exception("Error during cursor move: Table_Status = " + tableStatus);

			if (count != 1)
				throw new Exception("Error duringcursor move, "
					+ "Table_Status = 0, count = " + count);
			
			/* Set cursor to the first row of the table. */
			
			writeInt(sock, 19);					/* cc_TableGet */
			writeInt(sock, tableHandle);		/* Table_Handle */
			writeInt(sock, 2);			 		/* Flush */
			
			tableStatus = readInt(sock);
			
			if (tableStatus != 0)
				throw new Exception("Error during table read: Table_Status = " + tableStatus);

			int record_size = td.fieldSize + (td.count % 8 != 0 ? td.count/8 + 1 : td.count/8);
			byte REC[] = new byte[record_size];          /* REC buffor size: 32 + 8 + 1 */
			readBytes(sock, REC, record_size);			/* read recird */
			
			StringBuffer sb = new StringBuffer();
			for (byte b : REC) {
				sb.append(b);
			}
			System.out.println(sb);
			
			td.parseData(REC);
			records.add(td);

			elementsCount++;
			// Notify observers that row has been read
			setChanged();
			notifyObservers(EVENT.ROW_READ);
		}
		System.out.println("Total elements count: "+ elementsCount);
		return records;
	}
	
	/**
	 * Read whole table from QSEP database
	 * @param rawTableIndex table index
	 * @param Transaction_Handle
	 * @param sock
	 * @return all the table records
	 * @throws Exception
	 */
	ArrayList<TableDefinition> readTable(int rawTableIndex,int Transaction_Handle, Socket sock) throws Exception{
		TableDefinition td = readTableStruncture(rawTableIndex, sock);
		int id_index = td.getIdIndex();
		if(id_index == -1){
			for (TableColumn elem : td.columns) {
				if(elem.type == FieldType.INTEGER){
					id_index = elem.index;
					break;
				}
			}
		}
		
		System.out.println("Transaction_Handle " + Transaction_Handle + " id_index " + id_index + " rawtable index " + rawTableIndex);
		/* *** Open Table (cc_TableCreate) *** */
        writeInt(sock, 7);              /* cc_TableCreate */
        writeInt(sock, rawTableIndex);  /* RawTable_Index. you can get is using cc_QueryTableParam. */
        
        /* Select fields from RawTable */
        writeInt(sock, td.count);
        for(int i=1; i<=td.count ;i++){
        	writeInt(sock, i);      /* Field indexes: we want all of them */
        }
        writeInt(sock, Transaction_Handle);     /* transaction in which we open the table */

        /* Sort definition */
        writeInt(sock, 1);          /* Number of sort params */
        writeInt(sock, id_index);   /* index of id field */
        writeInt(sock, 1);          /* sort function (INTEGERCOMPFUNC) */
        
        int err = readInt(sock);
        if (err != 0)
            throw new Exception("cc_TableCreate: ERR = " + err);
        
        /* new table handle */
        int Table_Handle = readInt(sock);
        // read all the records from table
        ArrayList<TableDefinition> records = readRecords(Table_Handle, sock, td);

        /* close the table */
        writeInt(sock, 8);                  /* cc_TableDestroy */
        writeInt(sock, Table_Handle);
        writeInt(sock, 2);                  /* cc_Flush */
        err = readInt(sock);
        if (err != 0)
			throw new Exception("ERR = " + err);
        return records;
	}
	
	/**
	 * Get structure of the table identified with rawTableIndex
	 * @param rawTableIndex - index of the table
	 * @param sock - socket with db connection
	 * @return TableDefinition - containing field of the tables with their attributes
	 * @throws Exception
	 */
	static TableDefinition readTableStruncture(int rawTableIndex, Socket sock) throws Exception{
		TableDefinition td = new TableDefinition();
		td.index = rawTableIndex;
		/* Reading alle the table fields*/
		writeInt(sock, 22);             /* cc_QueryTableName */
		writeInt(sock,  rawTableIndex); /* RawTable_Index, tutaj na stale 1 */
		writeInt(sock,  2);             /* cc_Flush */

		int err = readInt(sock);
		if (err != 0)
			throw new Exception("ERR = " + err);
		int count = readInt(sock);

		for(int i = 0; i < count; i++)
		{
			int size;
			String name = readString(sock);
			int type = readInt(sock);
			TableColumn cd = new TableColumn();
			cd.name = name;
			cd.index = i+1;

			switch(type)
			{
			case 1: /* INTEGERFIELDTYPE */
				size = readInt(sock);
				cd.type = FieldType.INTEGER;
				cd.size = size;
				td.fieldSize += size;
				break;

			case 2: /* STRINGFIELDTYPE */
				size = readInt(sock);
				cd.type = FieldType.STRING;
				cd.size = size;
				td.fieldSize += size;
				break;
			case 3: /* DOUBLEFIELDTYPE */
				cd.type = FieldType.DOUBLE;
				cd.size = 8;
				td.fieldSize += 8;
				break;
			default:
				throw new Exception("Unexpected data type");
			}
			td.columns.add(cd);
		}
		td.count = count;
		return td;
	}

	private Socket sock;
	private int trans; // transaction handle

	public ConnectionData connect(String host, int port) throws Exception {
		ConnectionData cData = new ConnectionData();
		SocketAddress sockaddr = new InetSocketAddress(host, port);
		sock = new Socket();
		sock.connect(sockaddr, 5000);
		cData.dbname = readString(sock);
		cData.dbversion = readString(sock);
		
		int err;
		/* Create transaction */
        writeInt(sock, 3);          /* cc_TransactionCreate */
        writeInt(sock, 2);          /* cc_Flush */
        err = readInt(sock);        /* ERR */
        trans = readInt(sock);      /* Transaction_Handle */
		
		return cData;		
	}

	public ArrayList<TableData> getTablesList() throws Exception {
		ArrayList<TableData> tables = new ArrayList<TableData>();

		/* Load tables list */
		writeInt(sock, 20);			/* cc_QueryTableName */
		writeInt(sock, 2);			/* cc_Flush */
		
		int err = readInt(sock);
		
		System.out.println("ERR = " + err);
		if (err != 0)
			throw new Exception("ERR = " + err);
		int count = -1;
		count = readInt(sock);

		System.out.println("Database has " + count + " tables.");
		
		for(int i = 0; i < Math.abs(count); i++)
		{
			String name = readString(sock);
			int index = readInt(sock);
			System.out.println("Table #" + i + ": RawTable_Name = " + name
				+ ", RawTable_Index =  " + index);
			TableData tableData = new TableData();
			tableData.id = index;
			tableData.name = name;
			tables.add(tableData);
		}
		return tables;
	}

	public int finish() throws IOException {
		/* Accept transaction and finish */
		writeInt(sock, 5);			/* cc_TransactionAccept */
		writeInt(sock, trans);		/* Transaction_Handle */
		writeInt(sock, 2);			/* cc_Flush */
		int err = readInt(sock);		/* ERR */
		System.out.println("TransactionAccept: ERR = " + err);

		writeInt(sock, 1);			/* cc_Quit */
		return readInt(sock);       /* Return Quit Status */
	}

	public ArrayList<TableDefinition> getTableData(int tableId) {
		ArrayList<TableDefinition> tableData = null;
		try {
			tableData = readTable(tableId, trans, sock);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tableData;
	}	
}

class ConnectionData{
	String dbname;
	String dbversion;
}

class FieldType{
	static final int INTEGER = 1;
	static final int STRING = 2;
	static final int DOUBLE = 3;
}