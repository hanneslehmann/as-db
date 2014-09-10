package com.tibco.as.db;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;

import org.h2.Driver;
import org.junit.Assert;
import org.junit.Test;

import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.browser.Browser;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.Tuple;

public class TestDatabaseImport extends TestBase {

	private final static String DRIVER = Driver.class.getName();

	private final static String URL = "jdbc:h2:mem:test";

	private static final int SIZE = 1000;

	@Test
	public void testDatabaseImporter() throws Exception {
		Class.forName(DRIVER);
		Connection conn = DriverManager.getConnection(URL);
		// populate table "TEST"
		Statement statement = conn.createStatement();
		statement.execute("DROP TABLE IF EXISTS \"MySpace\"");
		statement
				.execute("CREATE TABLE \"MySpace\" (\"field1\" BIGINT not null, \"field2\" VARCHAR not null, \"field3\" TIMESTAMP null, \"field4\" BLOB null, \"field5\" BOOLEAN null, \"field6\" CHAR null, \"field7\" DOUBLE PRECISION null, \"field8\" FLOAT null, \"field9\" INTEGER null, \"field10\" SMALLINT null, Primary Key (\"field1\",\"field2\"))");
		statement.close();
		PreparedStatement preparedStatement = conn
				.prepareStatement("INSERT INTO \"MySpace\" (\"field1\", \"field2\", \"field3\", \"field4\", \"field5\", \"field6\", \"field7\", \"field8\", \"field9\", \"field10\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		for (int index = 0; index < SIZE; index++) {
			preparedStatement.setLong(1, index);
			preparedStatement.setString(2, String.valueOf(index));
			preparedStatement.setTimestamp(3, new Timestamp(Calendar
					.getInstance().getTime().getTime()));
			byte[] bytes = new byte[index];
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = (byte) i;
			}
			Blob blob = preparedStatement.getConnection().createBlob();
			blob.setBytes(1, bytes);
			preparedStatement.setBlob(4, blob);
			preparedStatement.setBoolean(5, index % 2 == 0);
			preparedStatement.setString(6, "c");
			preparedStatement.setDouble(7, index / 1000);
			preparedStatement.setFloat(8, index / 1000);
			preparedStatement.setInt(9, index);
			preparedStatement.setShort(10, (short) index);
			preparedStatement.execute();
		}
		conn.commit();
		preparedStatement.close();
		Metaspace metaspace = getMetaspace();
		Database db = new Database();
		db.setDriver(DRIVER);
		db.setUrl(URL);
		DatabaseImporter importer = new DatabaseImporter(metaspace, db);
		DatabaseImport import1 = new DatabaseImport();
		import1.setDistributionRole(DistributionRole.SEEDER);
		importer.setDefaultTransfer(import1);
		importer.execute();
		Space space1 = metaspace.getSpace("MySpace");
		Assert.assertEquals(SIZE, space1.size());
		Browser browser = space1.browse(BrowserType.GET);
		try {
			Tuple tuple;
			while ((tuple = browser.next()) != null) {
				Long index = tuple.getLong(TestDatabaseExport.FIELD_NAME1);
				Assert.assertEquals(String.valueOf(index),
						tuple.getString(TestDatabaseExport.FIELD_NAME2));
			}
		} finally {
			browser.stop();
		}
		space1.close();
	}
}
