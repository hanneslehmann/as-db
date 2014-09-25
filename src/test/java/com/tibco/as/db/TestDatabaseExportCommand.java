package com.tibco.as.db;

import java.io.File;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Test;

import com.tibco.as.io.Utils;
import com.tibco.as.space.Space;

public class TestDatabaseExportCommand extends TestBase {

	@Test
	public void testExportDatabase() throws Exception {
		File file = Utils.copy("db.xml", Utils.createTempDirectory());
		Space space = createSpace();
		String[] args = new String[] { "-config", file.getAbsolutePath(),
				"-discovery", "tcp", "-keep_open", "export" };
		DatabaseApplication.main(args);
		java.sql.Connection conn = getConnection();
		Statement stat = conn.createStatement();
		ResultSet resultSet = stat
				.executeQuery("select * from \"ms\".\"MyTable\" order by \"column1\"");
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
	}

}
