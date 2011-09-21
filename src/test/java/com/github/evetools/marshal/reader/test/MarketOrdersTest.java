package com.github.evetools.marshal.reader.test;

import java.io.File;
import java.net.URL;

import org.junit.Assert;
import org.junit.Test;

import com.github.evetools.marshal.reader.MarketOrders;

/**
 * Copyright (C)2011 by Gregor Anders
 * All rights reserved.
 *
 * This code is free software; you can redistribute it and/or modify
 * it under the terms of the BSD license (see the file LICENSE.txt
 * included with the distribution).
 */
public class MarketOrdersTest {

	@Test
	public void testRead() throws Exception {
		
		URL url = this.getClass().getResource("/9128.cache");
		File file = new File(url.getFile());
		
		Assert.assertTrue(file.isFile());
		
		MarketOrders marketOrders = new MarketOrders(file);		
		marketOrders.read();
	}

}
