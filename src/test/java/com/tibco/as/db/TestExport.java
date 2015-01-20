package com.tibco.as.db;

import java.io.File;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;

import org.junit.Assert;
import org.junit.Test;

import com.tibco.as.space.Space;
import com.tibco.as.util.file.FileUtils;

public class TestExport extends TestBase {

	@Test
	public void testExport() throws Exception {
		Space space = createSpace();
		execute("-discovery tcp -driver org.h2.Driver -url jdbc:h2:mem:test export "
				+ SPACE_NAME);
		Statement stat = getConnection().createStatement();
		ResultSet resultSet = stat.executeQuery(MessageFormat.format(
				"select * from \"{0}\" order by \"{1}\"", SPACE_NAME,
				FIELD_NAME1));
		int index = 1;
		while (resultSet.next()) {
			Assert.assertEquals(space.getSpaceDef().getFieldDefs().size(),
					resultSet.getMetaData().getColumnCount());
			Assert.assertEquals(index, resultSet.getLong(1));
			Assert.assertEquals(getString(index), resultSet.getString(2));
			Assert.assertEquals(getCalendar(index).getTime(),
					resultSet.getTimestamp(3));
			Blob blob = resultSet.getBlob(4);
			byte[] bytes = blob.getBytes(1, (int) blob.length());
			Assert.assertArrayEquals(getBytes(index), bytes);
			Assert.assertEquals(getBoolean(index), resultSet.getBoolean(5));
			Assert.assertEquals(getCharacter(index), resultSet.getString(6)
					.charAt(0));
			Assert.assertEquals(getDouble(index), resultSet.getDouble(7), 0);
			Assert.assertEquals(getFloat(index), resultSet.getFloat(8), 0);
			Assert.assertEquals(index, resultSet.getInt(9));
			Assert.assertEquals(index, resultSet.getShort(10));
			index++;
		}
		Assert.assertEquals(SIZE, index - 1);
	}

	@Test
	public void testConvert() throws Exception {
		java.sql.Connection connection = getConnection();
		Statement statement = connection.createStatement();
		statement.execute("CREATE SCHEMA ms");
		String sql = "CREATE TABLE \"MySpace\" (\"field1\" BIGINT not null, \"field2\" VARCHAR not null, \"field3\" TIMESTAMP null, \"field4\" BLOB null, \"field5\" BOOLEAN null, \"field6\" CHAR null, \"field7\" DOUBLE PRECISION null, \"field8\" FLOAT null, \"field9\" INTEGER null, \"field10\" SMALLINT null, Primary Key (\"field1\",\"field2\"))";
		statement.execute(sql);
		testExport();
		connection.close();
	}

	@Test
	public void testConfigFile() throws Exception {
		File file = FileUtils.copy("db.xml", FileUtils.createTempDirectory());
		Space space = createSpace();
		execute("-config "
				+ file.getAbsolutePath()
				+ " -discovery tcp -driver org.h2.Driver -url jdbc:h2:mem:test export");
		Statement stat = getConnection().createStatement();
		ResultSet resultSet = stat
				.executeQuery("select * from \"MyTable\" order by \"column1\"");
		int index = 1;
		while (resultSet.next()) {
			Assert.assertEquals(space.getSpaceDef().getFieldDefs().size(),
					resultSet.getMetaData().getColumnCount());
			Assert.assertEquals(index, resultSet.getLong(1));
			Assert.assertEquals(getString(index), resultSet.getString(2));
			Assert.assertEquals(getCalendar(index).getTime(),
					resultSet.getTimestamp(3));
			Blob blob = resultSet.getBlob(4);
			byte[] bytes = blob.getBytes(1, (int) blob.length());
			Assert.assertArrayEquals(getBytes(index), bytes);
			Assert.assertEquals(getBoolean(index), resultSet.getBoolean(5));
			Assert.assertEquals(getCharacter(index), resultSet.getString(6)
					.charAt(0));
			Assert.assertEquals(getDouble(index), resultSet.getDouble(7), 0);
			Assert.assertEquals(getFloat(index), resultSet.getFloat(8), 0);
			Assert.assertEquals(index, resultSet.getInt(9));
			Assert.assertEquals(index, resultSet.getShort(10));
			index++;
		}
		Assert.assertEquals(SIZE, index - 1);
	}
}
