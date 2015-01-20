package com.tibco.as.db.accessors;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BlobAccessor implements IColumnAccessor {

	private int index;

	public BlobAccessor(int index) {
		this.index = index;
	}

	@Override
	public void set(PreparedStatement statement, Object value)
			throws SQLException {
		Blob blob = statement.getConnection().createBlob();
		blob.setBytes(1, (byte[]) value);
		statement.setBlob(index, blob);
	}

	@Override
	public byte[] get(ResultSet resultSet) throws SQLException {
		Blob blob = resultSet.getBlob(index);
		if (blob == null) {
			return null;
		}
		return blob.getBytes(1, (int) blob.length());
	}

}
