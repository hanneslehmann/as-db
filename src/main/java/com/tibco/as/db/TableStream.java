package com.tibco.as.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TableStream {

	private TableConfig config;

	protected TableStream(TableConfig config) {
		this.config = config;
	}

	protected IPreparedStatementAccessor[] getAccessors() {
		Collection<IPreparedStatementAccessor> result = new ArrayList<IPreparedStatementAccessor>();
		List<ColumnConfig> columns = config.getColumns();
		for (int index = 0; index < columns.size(); index++) {
			ColumnConfig column = columns.get(index);
			result.add(getAccessor(index + 1, column.getColumnType()));
		}
		return result.toArray(new IPreparedStatementAccessor[result.size()]);
	}

	private IPreparedStatementAccessor getAccessor(int index, JDBCType type) {
		switch (type) {
		case BLOB:
			return new BlobAccessor(index);
		default:
			return new DefaultAccessor(index, type.getType());
		}
	}
}
