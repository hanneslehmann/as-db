package com.tibco.as.db;

import com.tibco.as.io.Import;
import com.tibco.as.space.Member.DistributionRole;

public class DatabaseImport extends Import {

	private Table table;

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	@Override
	public String getSpaceName() {
		String spaceName = super.getSpaceName();
		if (spaceName == null) {
			spaceName = table.getSpace();
			if (spaceName == null) {
				spaceName = table.getName();
			}
		}
		return spaceName;
	}

	@Override
	public Integer getBatchSize() {
		Integer batchSize = super.getBatchSize();
		if (batchSize == null) {
			batchSize = table.getBatchSize();
		}
		return batchSize;
	}

	@Override
	public DistributionRole getDistributionRole() {
		DistributionRole distributionRole = super.getDistributionRole();
		if (distributionRole == null) {
			distributionRole = table.getDistributionRole();
		}
		return distributionRole;
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
