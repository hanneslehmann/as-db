package com.tibco.as.db;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.tibco.as.io.EventManager;
import com.tibco.as.io.IEvent;
import com.tibco.as.io.IEvent.Severity;
import com.tibco.as.io.IEventListener;
import com.tibco.as.space.ASException;
import com.tibco.as.space.MemberDef;
import com.tibco.as.space.Metaspace;

public class TestBase {

	private Metaspace metaspace;

	@Before
	public void connectMetaspace() throws ASException {
		MemberDef memberDef = MemberDef.create(null, "tcp", null);
		memberDef.setConnectTimeout(10000);
		metaspace = Metaspace.connect(null, memberDef);
		EventManager.addListener(new IEventListener() {
			@Override
			public void onEvent(IEvent event) {
				if (event.getSeverity() == Severity.ERROR) {
					event.getException().printStackTrace();
					Assert.fail(event.getMessage());
				}
			}
		});
	}

	@After
	public void closeMetaspace() throws ASException {
		metaspace.closeAll();
	}

	protected Metaspace getMetaspace() {
		return metaspace;
	}

}
