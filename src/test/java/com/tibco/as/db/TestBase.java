package com.tibco.as.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;

import org.h2.Driver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.tibco.as.io.EventManager;
import com.tibco.as.io.IEvent;
import com.tibco.as.io.IEvent.Severity;
import com.tibco.as.io.IEventListener;
import com.tibco.as.space.ASException;
import com.tibco.as.space.DateTime;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.MemberDef;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;

public class TestBase {

	protected final static String DRIVER = Driver.class.getName();

	protected final static String URL = "jdbc:h2:mem:test";

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

	@Before
	public void connectMetaspace() throws ASException {
		MemberDef memberDef = MemberDef.create(null, "tcp", null);
		memberDef.setConnectTimeout(10000);
		metaspace = Metaspace.connect(null, memberDef);
		EventManager.addListener(new IEventListener() {
			@Override
			public void onEvent(IEvent event) {
				if (event.getSeverity() == Severity.ERROR) {
					event.getException().printStackTrace();
					Assert.fail(event.getMessage());
				}
			}
		});
	}

	@After
	public void closeMetaspace() throws ASException {
		metaspace.closeAll();
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

	protected Connection getConnection() throws SQLException,
			ClassNotFoundException {
		Class.forName(DRIVER);
		return DriverManager.getConnection(URL);
	}

	protected Database createDatabase() {
		Database database = new Database();
		database.setDriver(DRIVER);
		database.setUrl(URL);
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
