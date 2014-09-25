package com.tibco.as.db;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;

import org.junit.Assert;
import org.junit.Test;

import com.tibco.as.io.Direction;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;

public class TestDatabaseExport extends TestBase {

	@Test
	public void testExportToDatabase() throws Exception {
		Space space = createSpace();
		Database database = createDatabase();
		Metaspace metaspace = getMetaspace();
		DatabaseChannel channel = new DatabaseChannel(metaspace, database);
		TableConfig config = new TableConfig();
		config.setDirection(Direction.EXPORT);
		config.setSpaceName(SPACE_NAME);
		channel.addConfig(config);
		channel.setKeepOpen(true);
		channel.open();
		channel.close();
		java.sql.Connection conn = getConnection();
		Statement stat = conn.createStatement();
		ResultSet resultSet = stat.executeQuery(MessageFormat.format(
				"select * from \"{0}\".\"{1}\" order by \"{2}\"",
				metaspace.getName(), SPACE_NAME, FIELD_NAME1));
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
		conn.close();
		channel.getConnection().close();
	}

	@Test
	public void testExportDatabaseConvert() throws Exception {
		java.sql.Connection connection = getConnection();
		Statement statement = connection.createStatement();
		statement.execute("CREATE SCHEMA ms");
		String sql = "CREATE TABLE ms.MySpace (field1 BIGINT not null, field2 VARCHAR not null, field3 TIMESTAMP null, field4 BLOB null, field5 BOOLEAN null, field6 CHAR null, field7 DOUBLE PRECISION null, field8 FLOAT null, field9 INTEGER null, field10 SMALLINT null, Primary Key (field1,field2))";
		statement.execute(sql);
		testExportToDatabase();
		connection.close();
	}

}
