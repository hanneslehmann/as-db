package com.tibco.as.db;

import com.tibco.as.io.DestinationConfig;
import com.tibco.as.space.Member.DistributionRole;

public class TableConfig extends DestinationConfig {

	private Table table = new Table();

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	@Override
	public String getSpaceName() {
		if (table.getSpace() == null) {
			if (table.getName() == null) {
				return super.getSpaceName();
			}
			return table.getName();
		}
		return table.getSpace();
	}

	@Override
	public Integer getBatchSize() {
		if (table.getBatchSize() == null) {
			return super.getBatchSize();
		}
		return table.getBatchSize();
	}

	@Override
	public DistributionRole getDistributionRole() {
		if (table.getDistributionRole() == null) {
			return super.getDistributionRole();
		}
		return table.getDistributionRole();
	}

	@Override
	public TableConfig clone() {
		TableConfig export = new TableConfig();
		copyTo(export);
		return export;
	}

	public void copyTo(TableConfig export) {
		export.table = table;
		super.copyTo(export);
	}

}
