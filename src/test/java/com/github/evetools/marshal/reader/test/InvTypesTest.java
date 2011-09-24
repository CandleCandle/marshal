package com.github.evetools.marshal.reader.test;

import com.github.evetools.marshal.reader.InvTypes;
import java.io.InputStream;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Copyright (C)2011 by Gregor Anders
 * All rights reserved.
 *
 * This code is free software; you can redistribute it and/or modify
 * it under the terms of the BSD license (see the file LICENSE.txt
 * included with the distribution).
 */
public class InvTypesTest {

	@Test
	public void testRead_Cache() throws Exception {
		InputStream in = MarketOrdersBestsTest.class.getResourceAsStream("/ce99.cache");
		assertNotNull(in);
		InvTypes invTypes = new InvTypes(in);
		invTypes.read();
	}

	@Test
	public void testRead_Bulkdata() throws Exception {
				
		InputStream in = MarketOrdersBestsTest.class.getResourceAsStream("/600004.cache2");
		
		if (in != null) {
			InvTypes invTypes = new InvTypes(in);
			invTypes.read();
		}
	}
}
