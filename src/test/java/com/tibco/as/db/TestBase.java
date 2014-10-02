package com.tibco.as.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Calendar;
import java.util.logging.Logger;

import org.h2.Driver;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.tibco.as.log.LogFactory;
import com.tibco.as.log.LogLevel;
import com.tibco.as.space.ASException;
import com.tibco.as.space.DateTime;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.MemberDef;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class TestBase {

	private Logger log = LogFactory.getLog(TestBase.class);
	private final static String DRIVER = Driver.class.getName();
	private final static String URL = "jdbc:h2:mem:test";
	public static final String SPACE_NAME = "MySpace";
	public static final String FIELD_NAME1 = "field1";
	public static final String FIELD_NAME2 = "field2";
	public static final String FIELD_NAME3 = "field3";
	public static final String FIELD_NAME4 = "field4";
	public static final String FIELD_NAME5 = "field5";
	public static final String FIELD_NAME6 = "field6";
	public static final String FIELD_NAME7 = "field7";
	public static final String FIELD_NAME8 = "field8";
	public static final String FIELD_NAME9 = "field9";
	public static final String FIELD_NAME10 = "field10";
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

	protected static final int SIZE = 100;
	private Metaspace metaspace;
	private Connection connection;

	@BeforeClass
	public static void setup() {
		LogFactory.getRootLogger(LogLevel.DEBUG);
	}

	@Before
	public void connectMetaspace() throws Exception {
		metaspace = Metaspace.connect(null, getMemberDef());
		log.info("TestBase - Connecting " + DRIVER);
		Class.forName(DRIVER);
		connection = DriverManager.getConnection(URL);
		connection.setAutoCommit(true);
	}

	protected MemberDef getMemberDef() {
		return MemberDef.create(null, "tcp", null).setConnectTimeout(10000);

	}

	@After
	public void closeMetaspace() throws Exception {
		metaspace.closeAll();
		metaspace = null;
		connection.close();
		connection = null;
		log.info("TestBase - Disconnected");
	}

	public Connection getConnection() {
		return connection;
	}

	protected Metaspace getMetaspace() {
		return metaspace;
	}

	protected SpaceDef createSpaceDef() {
		SpaceDef spaceDef = SpaceDef.create(SPACE_NAME, 0, Arrays.asList(
				FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8,
				FIELD9, FIELD10));
		spaceDef.setKey(FIELD1.getName(), FIELD2.getName());
		return spaceDef;
	}

	protected DatabaseConfig createDatabaseConfig() {
		DatabaseConfig database = new DatabaseConfig();
		database.setDriver(DRIVER);
		database.setURL(URL);
		return database;
	}

	protected Tuple createTuple(int id) {
		Tuple tuple = Tuple.create();
		tuple.putLong(FIELD_NAME1, id);
		tuple.putString(FIELD_NAME2, getString(id));
		tuple.putDateTime(FIELD_NAME3, DateTime.create(getCalendar(id)));
		tuple.putBlob(FIELD_NAME4, getBytes(id));
		tuple.putBoolean(FIELD_NAME5, getBoolean(id));
		tuple.putChar(FIELD_NAME6, getCharacter(id));
		tuple.putDouble(FIELD_NAME7, id / 1000);
		tuple.putFloat(FIELD_NAME8, id / 1000);
		tuple.putInt(FIELD_NAME9, (int) id);
		tuple.putShort(FIELD_NAME10, (short) id);
		return tuple;
	}

	protected Calendar getCalendar(int id) {
		Calendar calendar = Calendar.getInstance();
		calendar.clear();
		calendar.set(Calendar.YEAR, 2000 + id);
		calendar.set(Calendar.MONTH, id % 12);
		calendar.set(Calendar.DAY_OF_MONTH, id & 28);
		calendar.set(Calendar.HOUR_OF_DAY, id % 24);
		calendar.set(Calendar.MINUTE, id % 60);
		calendar.set(Calendar.SECOND, id % 60);
		calendar.set(Calendar.MILLISECOND, id % 1000);
		return calendar;
	}

	protected byte[] getBytes(int value) {
		byte[] bytes = new byte[value];
		for (int index = 0; index < bytes.length; index++) {
			bytes[index] = (byte) index;
		}
		return bytes;
	}

	protected boolean getBoolean(int index) {
		return index % 2 == 0;
	}

	protected char getCharacter(int index) {
		return 'c';
	}

	protected String getString(int index) {
		return String.valueOf(index);
	}

	protected double getDouble(int index) {
		return index / 1000;
	}

	protected float getFloat(int index) {
		return index / 1000;
	}

	protected Space createSpace() throws ASException {
		SpaceDef spaceDef = createSpaceDef();
		getMetaspace().defineSpace(spaceDef);
		Space space = getMetaspace().getSpace(spaceDef.getName(),
				DistributionRole.SEEDER);
		space.waitForReady();
		for (int index = 1; index <= SIZE; index++) {
			space.put(createTuple(index));
		}
		return space;
	}
}
