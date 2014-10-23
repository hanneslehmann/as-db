package com.tibco.as.db;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;

import com.tibco.as.io.AbstractDestination;
import com.tibco.as.io.IOutputStream;

public class TableDestination extends AbstractDestination {

	private static final String SELECT = "SELECT {0} FROM {1}";

	private DatabaseChannel channel;
	private TableConfig config;

	public TableDestination(DatabaseChannel channel, TableConfig config) {
		super(channel, config);
		this.channel = channel;
		this.config = config;
	}

	@Override
	protected IOutputStream getExportOutputStream() throws SQLException {
		if (config.getInsertSQL() == null) {
			String tableName = config.getFullyQualifiedName();
			String[] columnNames = config.getColumnNames();
			String[] questionMarks = new String[columnNames.length];
			Arrays.fill(questionMarks, "?");
			String sql = MessageFormat.format(
					"INSERT INTO {0} ({1}) VALUES ({2})", tableName,
					getCommaSeparated(columnNames),
					getCommaSeparated(questionMarks));
			config.setInsertSQL(sql);
		}
		int batchSize = config.getTableBatchSize();
		if (batchSize > 1) {
			return new BatchTableOutputStream(channel, config, batchSize);
		}
		return new TableOutputStream(channel, config);
	}

	@Override
	protected TableInputStream getImportInputStream() throws Exception {
		if (config.getSelectSQL() == null) {
			config.setSelectSQL(MessageFormat.format(SELECT,
					getCommaSeparated(config.getColumnNames()),
					config.getFullyQualifiedName()));
			if (config.getCountSQL() == null) {
				config.setCountSQL(MessageFormat.format(SELECT, "COUNT(*)",
						config.getFullyQualifiedName()));
			}
		}
		return new TableInputStream(channel, config);
	}

	@Override
	public String getName() {
		return config.getTable();
	}

	private String getCommaSeparated(String[] elements) {
		String result = "";
		for (int index = 0; index < elements.length; index++) {
			if (index > 0) {
				result += ", ";
			}
			result += elements[index];
		}
		return result;
	}
	
	@Override
	protected Class<?> getComponentType() {
		return Object.class;
	}

}
