package com.tibco.as.db;

import com.tibco.as.io.AbstractTupleStreamAdapter;
import com.tibco.as.util.convert.IConverter;

public class TupleStreamAdapter extends AbstractTupleStreamAdapter<Object[]> {

	private TableDestination destination;

	public TupleStreamAdapter(TableDestination destination) {
		super(destination);
		this.destination = destination;
	}

	@Override
	protected void setValue(Object[] target, Object converted, int index) {
		target[index] = converted;
	}

	@Override
	protected IConverter[] getConverters() {
		return destination.getOutputConverters();
	}

}
