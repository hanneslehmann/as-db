package com.tibco.as.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import org.h2.Driver;
import org.junit.Assert;
import org.junit.Test;

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

	private static final int SIZE = 1000;

	private static final String SPACE = "space1";

	private static final String FIELD1 = "field1";

	private static final String FIELD2 = "field2";

	@Test
	public void testDatabaseExporter() throws Exception {
		Metaspace metaspace = getMetaspace();
		SpaceDef spaceDef = SpaceDef.create(
				SPACE,
				0,
				Arrays.asList(FieldDef.create(FIELD1, FieldType.LONG),
						FieldDef.create(FIELD2, FieldType.STRING)));
		spaceDef.setKey(FIELD1);
		metaspace.defineSpace(spaceDef);
		Space space = metaspace.getSpace(SPACE, DistributionRole.SEEDER);
		space.waitForReady();
		for (int index = 0; index < SIZE; index++) {
			Tuple tuple = Tuple.create();
			tuple.putLong(FIELD1, index + 1);
			tuple.putString(FIELD2, String.valueOf(index + 1));
			space.put(tuple);
		}
		Database db = new Database();
		db.setDriver(DRIVER);
		db.setUrl(URL);
		DatabaseExporter exporter = new DatabaseExporter(metaspace, db);
		exporter.setDoNotCloseConnection(true);
		exporter.execute();
		Class.forName(DRIVER);
		Connection conn = DriverManager.getConnection(URL);
		Statement stat = conn.createStatement();
		ResultSet resultSet = stat.executeQuery("select * from ms." + SPACE);
		int size = 0;
		while (resultSet.next()) {
			size++;
		}
		Assert.assertEquals(SIZE, size);
	}

}
