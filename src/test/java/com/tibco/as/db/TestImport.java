package com.tibco.as.db;

import java.io.File;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;

import org.junit.Assert;
import org.junit.Test;

import com.tibco.as.io.Utils;
import com.tibco.as.space.ASException;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;
import com.tibco.as.space.browser.Browser;
import com.tibco.as.space.browser.BrowserDef.BrowserType;

public class TestImport extends TestBase {

	private static final int SIZE = 1000;

	@Test
	public void testImportDatabase() throws Exception {
		Statement statement = getConnection().createStatement();
		statement.execute("DROP TABLE IF EXISTS \"MySpace\"");
		statement.execute("DROP TABLE IF EXISTS \"MySpace2\"");
		statement
				.execute("CREATE TABLE \"MySpace\" (\"field1\" BIGINT not null, \"field2\" VARCHAR not null, \"field3\" TIMESTAMP null, \"field4\" BLOB null, \"field5\" BOOLEAN null, \"field6\" CHAR(1) null, \"field7\" DOUBLE PRECISION null, \"field8\" REAL null, \"field9\" INTEGER null, \"field10\" SMALLINT null, Primary Key (\"field1\",\"field2\"))");
		statement
				.execute("CREATE TABLE \"MySpace2\" (\"test1\" BIGINT not null, \"test2\" VARCHAR not null, \"test3\" TIMESTAMP null, Primary Key (\"test1\",\"test2\"))");
		statement.close();
		PreparedStatement preparedStatement = getConnection()
				.prepareStatement(
						"INSERT INTO \"MySpace\" (\"field1\", \"field2\", \"field3\", \"field4\", \"field5\", \"field6\", \"field7\", \"field8\", \"field9\", \"field10\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
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
		getConnection().commit();
		preparedStatement.close();
		execute("-discovery tcp -driver org.h2.Driver -url jdbc:h2:mem:test import -distribution_role seeder MySpace");
		assertSpace();
	}

	private void assertSpace() throws ASException {
		SpaceDef spaceDef = getMetaspace().getSpaceDef(SPACE_NAME);
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
		Browser browser = getMetaspace().browse(SPACE_NAME, BrowserType.GET);
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
	}

	@Test
	public void testImportConfigFile() throws Exception {
		Statement statement = getConnection().createStatement();
		statement.execute("DROP TABLE IF EXISTS \"MyTable\"");
		statement.execute("DROP TABLE IF EXISTS \"MySpace2\"");
		statement
				.execute("CREATE TABLE \"MyTable\" (\"column1\" BIGINT not null, \"column2\" VARCHAR not null, \"column3\" TIMESTAMP null, \"column4\" BLOB null, \"column5\" BOOLEAN null, \"column6\" CHAR(1) null, \"column7\" DOUBLE PRECISION null, \"column8\" REAL null, \"column9\" INTEGER null, \"column10\" SMALLINT null, Primary Key (\"column1\",\"column2\"))");
		statement
				.execute("CREATE TABLE \"MySpace2\" (\"test1\" BIGINT not null, \"test2\" VARCHAR not null, \"test3\" TIMESTAMP null, Primary Key (\"test1\",\"test2\"))");
		statement.close();
		PreparedStatement preparedStatement = getConnection()
				.prepareStatement(
						"INSERT INTO \"MyTable\" (\"column1\", \"column2\", \"column3\", \"column4\", \"column5\", \"column6\", \"column7\", \"column8\", \"column9\", \"column10\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
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
		getConnection().commit();
		preparedStatement.close();
		File file = Utils.copy("db.xml", Utils.createTempDirectory());
		execute("-config "
				+ file.getAbsolutePath()
				+ " -discovery tcp -driver org.h2.Driver -url jdbc:h2:mem:test import -distribution_role seeder");
		assertSpace();
	}

}
