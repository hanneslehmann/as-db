package com.tibco.as.db.cli;

import com.beust.jcommander.Parameter;
import com.tibco.as.db.Table;
import com.tibco.as.db.TableDestination;
import com.tibco.as.db.TableType;

public class DatabaseImportParameters extends DatabaseParameters {

	@Parameter(names = { "-fetch_size" }, description = "Fetch size")
	private Integer fetchSize;
	@Parameter(names = { "-select_sql" }, description = "Select query")
	private String selectSQL;
	@Parameter(names = { "-count_sql" }, description = "Select count query")
	private String countSQL;
	@Parameter(names = { "-type" }, description = "Table type", converter = TableTypeConverter.class, validateWith = TableTypeConverter.class)
	private TableType type;

	@Override
	public void configure(TableDestination destination) {
		Table table = destination.getTable();
		if (fetchSize != null) {
			table.setFetchSize(fetchSize);
		}
		if (selectSQL != null) {
			table.setSelectSQL(selectSQL);
		}
		if (countSQL != null) {
			table.setCountSQL(countSQL);
		}
		if (type != null) {
			table.setType(type);
		}
		super.configure(destination);
	}

	public TableType getType() {
		return type;
	}

}
