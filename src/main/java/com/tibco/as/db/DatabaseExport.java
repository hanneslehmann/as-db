package com.tibco.as.db;

import com.tibco.as.io.Export;

public class DatabaseExport extends Export {

	private Table table;

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	@Override
	public DatabaseExport clone() {
		DatabaseExport export = new DatabaseExport();
		copyTo(export);
		return export;
	}

	public void copyTo(DatabaseExport export) {
		export.table = table;
		super.copyTo(export);
	}

}
