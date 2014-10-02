package com.tibco.as.db;

public class Adapter {

	public static <T extends Enum<?>> String print(T value) {
		return value == null ? null : value.name().toLowerCase();
	}

	public static String parse(String name) {
		return name.toUpperCase();
	}

	public static String printDataType(JDBCType type) {
		return print(type);
	}

	public static JDBCType parseDataType(String name) {
		return JDBCType.valueOf(parse(name));
	}

}
