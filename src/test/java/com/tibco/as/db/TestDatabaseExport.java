package com.tibco.as.db;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Calendar;

import org.h2.Driver;
import org.junit.Assert;
import org.junit.Test;

import com.tibco.as.space.ASException;
import com.tibco.as.space.DateTime;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class TestDatabaseExport extends TestBase {

	private final static String DRIVER = Driver.class.getName();

	private final static String URL = "jdbc:h2:mem:test";

	private static final int SIZE = 100;

	protected static final String SPACE_NAME = "MySpace";

	protected static final String FIELD_NAME1 = "field1";

	protected static final String FIELD_NAME2 = "field2";

	protected static final String FIELD_NAME3 = "field3";

	protected static final String FIELD_NAME4 = "field4";

	protected static final String FIELD_NAME5 = "field5";

	protected static final String FIELD_NAME6 = "field6";

	protected static final String FIELD_NAME7 = "field7";

	protected static final String FIELD_NAME8 = "field8";

	protected static final String FIELD_NAME9 = "field9";

	protected static final String FIELD_NAME10 = "field10";

	protected static final FieldDef FIELD1 = FieldDef.create(FIELD_NAME1,
			FieldType.LONG).setNullable(false);
	protected static final FieldDef FIELD2 = FieldDef.create(FIELD_NAME2,
			FieldType.STRING).setNullable(false);
	protected static final FieldDef FIELD3 = FieldDef.create(FIELD_NAME3,
			FieldType.DATETIME).setNullable(true);
	protected static final FieldDef FIELD4 = FieldDef.create(FIELD_NAME4,
			FieldType.BLOB).setNullable(true);
	protected static final FieldDef FIELD5 = FieldDef.create(FIELD_NAME5,
			FieldType.BOOLEAN).setNullable(true);
	protected static final FieldDef FIELD6 = FieldDef.create(FIELD_NAME6,
			FieldType.CHAR).setNullable(true);
	protected static final FieldDef FIELD7 = FieldDef.create(FIELD_NAME7,
			FieldType.DOUBLE).setNullable(true);
	protected static final FieldDef FIELD8 = FieldDef.create(FIELD_NAME8,
			FieldType.FLOAT).setNullable(true);
	protected static final FieldDef FIELD9 = FieldDef.create(FIELD_NAME9,
			FieldType.INTEGER).setNullable(true);
	protected static final FieldDef FIELD10 = FieldDef.create(FIELD_NAME10,
			FieldType.SHORT).setNullable(true);

	private Space createSpace() throws ASException {
		SpaceDef spaceDef = SpaceDef.create(SPACE_NAME, 0, Arrays.asList(
				FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8,
				FIELD9, FIELD10));
		spaceDef.setKey(FIELD1.getName(), FIELD2.getName());
		getMetaspace().defineSpace(spaceDef);
		Space space = getMetaspace().getSpace(spaceDef.getName(),
				DistributionRole.SEEDER);
		space.waitForReady();
		for (int index = 1; index <= SIZE; index++) {
			space.put(createTuple(index));
		}
		return space;
	}

	@Test
	public void testExportDatabase() throws Exception {
		Space space = createSpace();
		Database db = new Database();
		db.setDriver(DRIVER);
		db.setUrl(URL);
		Metaspace metaspace = getMetaspace();
		DatabaseExporter exporter = new DatabaseExporter(metaspace, db);
		exporter.setDoNotCloseConnection(true);
		exporter.execute();
		Connection conn = getConnection();
		Statement stat = conn.createStatement();
		ResultSet resultSet = stat.executeQuery(MessageFormat.format(
				"select * from \"{0}\".\"{1}\" order by \"{2}\"",
				metaspace.getName(), SPACE_NAME, FIELD_NAME1));
		long index = 1;
		while (resultSet.next()) {
			Assert.assertEquals(space.getSpaceDef().getFieldDefs().size(),
					resultSet.getMetaData().getColumnCount());
			Assert.assertEquals(index, resultSet.getLong(1));
			Assert.assertEquals(String.valueOf(index), resultSet.getString(2));
			Assert.assertNotNull(resultSet.getTimestamp(3));
			Blob blob = resultSet.getBlob(4);
			byte[] bytes = blob.getBytes(1, (int) blob.length());
			Assert.assertEquals(index, bytes.length);
			for (int arrayIndex = 0; arrayIndex < bytes.length; arrayIndex++) {
				Assert.assertEquals(arrayIndex, bytes[arrayIndex]);
			}
			Assert.assertEquals(index % 2 == 0, resultSet.getBoolean(5));
			Assert.assertEquals('c', resultSet.getString(6).charAt(0));
			Assert.assertEquals(index / 1000, resultSet.getDouble(7), 0);
			Assert.assertEquals(index / 1000, resultSet.getFloat(8), 0);
			Assert.assertEquals(index, resultSet.getInt(9));
			Assert.assertEquals(index, resultSet.getShort(10));
			index++;
		}
		Assert.assertEquals(SIZE, index - 1);
		conn.close();
		exporter.getConnection().close();
	}

	private Connection getConnection() throws SQLException,
			ClassNotFoundException {
		Class.forName(DRIVER);
		return DriverManager.getConnection(URL);
	}

	@Test
	public void testExportDatabaseConvert() throws Exception {
		Connection connection = getConnection();
		Statement statement = connection.createStatement();
		statement.execute("CREATE SCHEMA ms");
		String sql = "CREATE TABLE ms.MySpace (field1 BIGINT not null, field2 VARCHAR not null, field3 TIMESTAMP null, field4 BLOB null, field5 BOOLEAN null, field6 CHAR null, field7 DOUBLE PRECISION null, field8 FLOAT null, field9 INTEGER null, field10 SMALLINT null, Primary Key (field1,field2))";
		statement.execute(sql);
		testExportDatabase();
		connection.close();
	}

	protected Tuple createTuple(int id) {
		Tuple tuple = Tuple.create();
		tuple.putLong(FIELD_NAME1, id);
		tuple.putString(FIELD_NAME2, String.valueOf(id));
		Calendar calendar = Calendar.getInstance();
		tuple.putDateTime(FIELD_NAME3, DateTime.create(calendar));
		byte[] bytes = new byte[id];
		for (int index = 0; index < bytes.length; index++) {
			bytes[index] = (byte) index;
		}
		tuple.putBlob(FIELD_NAME4, bytes);
		tuple.putBoolean(FIELD_NAME5, id % 2 == 0);
		tuple.putChar(FIELD_NAME6, 'c');
		tuple.putDouble(FIELD_NAME7, id / 1000);
		tuple.putFloat(FIELD_NAME8, id / 1000);
		tuple.putInt(FIELD_NAME9, id);
		tuple.putShort(FIELD_NAME10, (short) id);
		return tuple;
	}

}
