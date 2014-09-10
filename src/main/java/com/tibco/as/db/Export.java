package com.tibco.as.db;

import com.tibco.as.io.AbstractExport;

public class Export extends AbstractExport {

	private Table table = new Table();

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	@Override
	public Export clone() {
		Export export = new Export();
		copyTo(export);
		return export;
	}

	public void copyTo(Export export) {
		export.table = table;
		super.copyTo(export);
	}

}
