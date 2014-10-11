package com.tibco.as.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.IOutputStream;
import com.tibco.as.log.LogFactory;

public class TableOutputStream extends TableStream implements IOutputStream {

	private Logger log = LogFactory.getLog(TableOutputStream.class);
	private DatabaseChannel channel;
	private TableConfig config;
	private PreparedStatement statement;
	private IPreparedStatementAccessor[] accessors;

	public TableOutputStream(DatabaseChannel channel, TableConfig config) {
		super(config);
		this.channel = channel;
		this.config = config;
	}

	@Override
	public void open() throws Exception {
		Collection<TableConfig> tables = channel.getTables(config);
		if (tables.isEmpty()) {
			// create table
			channel.execute(getCreateSQL());
		} else {
			for (TableConfig table : tables) {
				table.copyTo(config);
			}
		}
		String sql = config.getInsertSQL();
		log.log(Level.FINE, "Preparing statement: {0}", sql);
		statement = channel.prepareStatement(sql);
		accessors = getAccessors();
	}

	@Override
	public void close() throws Exception {
		close(statement);
	}

	protected void close(PreparedStatement statement) throws SQLException {
		statement.close();
	}

	@Override
	public void write(Object element) throws Exception {
		set((Object[]) element);
		execute(statement);
	}

	protected void execute(PreparedStatement statement) throws SQLException {
		statement.execute();
	}

	private void set(Object[] element) throws SQLException {
		for (int index = 0; index < element.length; index++) {
			accessors[index].set(statement, element[index]);
		}
	}

	private String getCreateSQL() throws SQLException {
		String query = "";
		query += "CREATE TABLE " + config.getFullyQualifiedName() + " (";
		for (ColumnConfig column : config.getColumns()) {
			String columnName = config.quote(column.getColumnName());
			JDBCType type = column.getColumnType();
			String typeName = type.getName();
			query += columnName + " " + typeName;
			Integer size = column.getColumnSize();
			if (size != null) {
				query += "(";
				query += size;
				if (column.getDecimalDigits() != null) {
					query += "," + column.getDecimalDigits();
				}
				query += ")";
			}
			if (!Boolean.TRUE.equals(column.getColumnNullable())) {
				query += " not";
			}
			query += " null, ";
		}
		query += "Primary Key (";
		int index = 0;
		for (String key : config.getPrimaryKeys()) {
			if (index > 0) {
				query += ",";
			}
			query += config.quote(key);
			index++;
		}
		query += ")"; // close primary key constraint
		query += ")"; // close table def
		return query;
	}
}
