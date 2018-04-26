package com.paytm.code.log_analyzer;

import junit.framework.TestCase;

public class UtilsTest extends TestCase {
	
	public void testGetSessionId() {
		assertEquals("2015-07-22-36", Utils.getSessionId("2015-07-22T09:00:28.019143Z", 15));
		assertEquals("2015-07-22-0", Utils.getSessionId("2015-07-22T00:14:28.019143Z", 15));
		assertEquals("2015-07-22-1", Utils.getSessionId("2015-07-22T00:16:28.019143Z", 15));
	}

	public void testGetSession() {
		assertEquals("2015-07-22T09:00-2015-07-22T09:15", Utils.getSession("2015-07-22-36", 15));
		assertEquals("2015-07-22T00:00-2015-07-22T00:15", Utils.getSession("2015-07-22-0", 15));
		assertEquals("2015-07-22T00:15-2015-07-22T00:30", Utils.getSession("2015-07-22-1", 15));
	}
}
