package com.tibco.as.db.cli;

import com.tibco.as.db.TableType;
import com.tibco.as.io.cli.converters.AbstractEnumConverter;

public class TableTypeConverter extends AbstractEnumConverter<TableType> {

	@Override
	protected TableType valueOf(String name) {
		return TableType.valueOf(name);
	}

	@Override
	protected TableType[] getValues() {
		return TableType.values();
	}

}
