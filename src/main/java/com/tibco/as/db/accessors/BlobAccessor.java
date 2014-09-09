package com.tibco.as.db.accessors;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.tibco.as.db.IPreparedStatementAccessor;

public class BlobAccessor implements IPreparedStatementAccessor {

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

}
