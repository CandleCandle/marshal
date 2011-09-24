package com.github.evetools.marshal.python;

/**
 * Copyright (C)2011 by Gregor Anders All rights reserved.
 * 
 * This code is free software; you can redistribute it and/or modify it under
 * the terms of the BSD license (see the file LICENSE.txt included with the
 * distribution).
 */
public class PyList extends PyContainer {

    /**
     * PyList.
     */
    public PyList() {
        super(PyType.LIST);
    }

    @Override
    public final boolean visit(final PyVisitor visitor) {
        return (visitor.visit(this));
    }
}
