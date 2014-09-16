package com.tibco.as.db;

import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;

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

	public static String printDistributionRole(DistributionRole type) {
		return print(type);
	}

	public static DistributionRole parseDistributionRole(String name) {
		return DistributionRole.valueOf(parse(name));
	}
	
	public static String printFieldType(FieldType type) {
		return print(type);
	}

	public static FieldType parseFieldType(String name) {
		return FieldType.valueOf(parse(name));
	}


}
