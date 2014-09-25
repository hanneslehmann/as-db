package com.tibco.as.db;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;

import org.junit.Assert;
import org.junit.Test;

import com.tibco.as.io.Direction;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;
import com.tibco.as.space.browser.Browser;
import com.tibco.as.space.browser.BrowserDef.BrowserType;

public class TestDatabaseImport extends TestBase {

	private static final int SIZE = 1000;

	@Test
	public void testImportDatabase() throws Exception {
		java.sql.Connection conn = getConnection();
		// populate table "TEST"
		Statement statement = conn.createStatement();
		statement.execute("DROP TABLE IF EXISTS \"MySpace\"");
		statement
				.execute("CREATE TABLE \"MySpace\" (\"field1\" BIGINT not null, \"field2\" VARCHAR not null, \"field3\" TIMESTAMP null, \"field4\" BLOB null, \"field5\" BOOLEAN null, \"field6\" CHAR(1) null, \"field7\" DOUBLE PRECISION null, \"field8\" REAL null, \"field9\" INTEGER null, \"field10\" SMALLINT null, Primary Key (\"field1\",\"field2\"))");
		statement.close();
		PreparedStatement preparedStatement = conn
				.prepareStatement("INSERT INTO \"MySpace\" (\"field1\", \"field2\", \"field3\", \"field4\", \"field5\", \"field6\", \"field7\", \"field8\", \"field9\", \"field10\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		for (int index = 0; index < SIZE; index++) {
			preparedStatement.setLong(1, index);
			preparedStatement.setString(2, getString(index));
			preparedStatement.setTimestamp(3, new Timestamp(getCalendar(index)
					.getTime().getTime()));
			Blob blob = preparedStatement.getConnection().createBlob();
			blob.setBytes(1, getBytes(index));
			preparedStatement.setBlob(4, blob);
			preparedStatement.setBoolean(5, getBoolean(index));
			preparedStatement.setString(6, String.valueOf(getCharacter(index)));
			preparedStatement.setDouble(7, getDouble(index));
			preparedStatement.setFloat(8, getFloat(index));
			preparedStatement.setInt(9, index);
			preparedStatement.setShort(10, (short) index);
			preparedStatement.execute();
		}
		conn.commit();
		preparedStatement.close();
		Metaspace metaspace = getMetaspace();
		Database database = createDatabase();
		DatabaseChannel channel = new DatabaseChannel(metaspace, database);
		TableConfig config = new TableConfig();
		config.setDirection(Direction.IMPORT);
		config.getTable().setName("MySpace");
		config.setDistributionRole(DistributionRole.SEEDER);
		channel.addConfig(config);
		channel.open();
		channel.close();
		SpaceDef spaceDef = metaspace.getSpaceDef(SPACE_NAME);
		Assert.assertEquals(FieldType.LONG, spaceDef.getFieldDef(FIELD_NAME1)
				.getType());
		Assert.assertEquals(FieldType.STRING, spaceDef.getFieldDef(FIELD_NAME2)
				.getType());
		Assert.assertEquals(FieldType.DATETIME,
				spaceDef.getFieldDef(FIELD_NAME3).getType());
		Assert.assertEquals(FieldType.BLOB, spaceDef.getFieldDef(FIELD_NAME4)
				.getType());
		Assert.assertEquals(FieldType.BOOLEAN, spaceDef
				.getFieldDef(FIELD_NAME5).getType());
		Assert.assertEquals(FieldType.STRING, spaceDef.getFieldDef(FIELD_NAME6)
				.getType());
		Assert.assertEquals(FieldType.DOUBLE, spaceDef.getFieldDef(FIELD_NAME7)
				.getType());
		Assert.assertEquals(FieldType.FLOAT, spaceDef.getFieldDef(FIELD_NAME8)
				.getType());
		Assert.assertEquals(FieldType.INTEGER, spaceDef
				.getFieldDef(FIELD_NAME9).getType());
		Browser browser = metaspace.browse(SPACE_NAME, BrowserType.GET);
		Assert.assertEquals(SIZE, browser.size());
		try {
			Tuple tuple;
			while ((tuple = browser.next()) != null) {
				Long index = tuple.getLong(FIELD_NAME1);
				int id = index.intValue();
				Assert.assertEquals(getString(id), tuple.getString(FIELD_NAME2));
				Assert.assertEquals(getCalendar(id).getTimeInMillis(), tuple
						.getDateTime(FIELD_NAME3).getTime().getTimeInMillis());
				Assert.assertArrayEquals(getBytes(id),
						tuple.getBlob(FIELD_NAME4));
				Assert.assertEquals(getBoolean(id),
						tuple.getBoolean(FIELD_NAME5));
				Assert.assertEquals(String.valueOf(getCharacter(id)),
						tuple.getString(FIELD_NAME6));
				Assert.assertEquals(getDouble(id), tuple.getDouble(FIELD_NAME7)
						.doubleValue(), 0);
				Assert.assertEquals(getFloat(id), tuple.getFloat(FIELD_NAME8)
						.floatValue(), 0);
				Assert.assertEquals(id, tuple.getInt(FIELD_NAME9).intValue());
				Assert.assertEquals(id, tuple.getInt(FIELD_NAME10).intValue());
			}
		} finally {
			browser.stop();
		}
		channel.close();
	}

}
