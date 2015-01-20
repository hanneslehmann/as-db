package com.tibco.as.db;

import com.tibco.as.io.AbstractArrayStreamAdapter;
import com.tibco.as.util.convert.IConverter;

public class ObjectArrayStreamAdapter extends
		AbstractArrayStreamAdapter<Object> {

	private TableDestination destination;

	public ObjectArrayStreamAdapter(TableDestination destination) {
		super(destination);
		this.destination = destination;
	}

	@Override
	protected IConverter[] getConverters() {
		return destination.getInputConverters();
	}
}
