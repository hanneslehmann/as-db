package com.tibco.as.db;

import com.tibco.as.io.AbstractExport;

public class DatabaseExport extends AbstractExport {

	private Table table = new Table();

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
