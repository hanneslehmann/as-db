package com.tibco.as.db;

import com.tibco.as.io.AbstractImport;
import com.tibco.as.space.Member.DistributionRole;

public class DatabaseImport extends AbstractImport {

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
	public DatabaseImport clone() {
		DatabaseImport result = new DatabaseImport();
		copyTo(result);
		return result;
	}

	public void copyTo(DatabaseImport target) {
		target.table = table;
		super.copyTo(target);
	}

}
