package com.github.evetools.marshal.python;

import java.io.IOException;

import com.github.evetools.marshal.python.PyBase.PyType;

/**
* The PyInt class wraps a value of the primitive type
* <code>int</code> in an object. An object of type
* <code>PyInt</code> contains a single field whose type is
* <code>int</code>.
* <p>
* In addition, this class provides methods for converting
* a <code>int</code> to a <code>String</code>.
* <br>
* <br>
* Copyright (C)2011 by Gregor Anders All rights reserved.
* <br>
* <br>
* This code is free software; you can redistribute it and/or modify it under
* the terms of the BSD license (see the file LICENSE.txt included with the
* distribution).
*
* @since   0.0.1
*/
public class PyInt extends PyBase {

    /**
     * The value of the PyInt.
     */
    private int value;

    /**
     * Allocates a <code>PyInt</code> object representing the
     * <code>val</code> argument.
     *
     * @param val the value of the <code>PyInt</code>.
     * @since   0.0.1
     */
        public PyInt(final int val) {
        super(PyType.INT32);
        this.value = val;
    }

    /**
     * Returns <code>true</code> if and only if the argument is not
     * <code>null</code> and is a <code>PyInt</code> object that
     * represents the same <code>int</code> value as this object.
     *
     * @param   obj   the object to compare with.
     * @return  <code>true</code> if the <code>PyInt</code> objects represent
     *          the same value; <code>false</code> otherwise.
     * @since   0.0.1
     */
    @Override
    public final boolean equals(final Object obj) {
        if (obj instanceof PyInt) {
            return value == ((PyInt) obj).intValue();
        }
        return false;
    }

    /**
     * Returns the value of this <tt>PyInt</tt> object as a
     * <code>int</code> primitive.
     *
     * @return  the primitive <code>int</code> value of this object.
     * @since   0.0.1
     */
    public final int intValue() {
        return this.value;
    }

    /**
     * Returns a hash code for this <tt>PyInt</tt> object.
     *
     * @return  the integer this object represents.
     * @see     Integer#hashCode()
     * @see     PyBase#getType()
     * @see     PyType#hashCode()
     * @since   0.0.1
     */
    @Override
    public final int hashCode() {
        final int prime = 31;
        return (prime * super.getType().hashCode())
                + Integer.valueOf(this.value).hashCode();
    }

    /**
     * Returns a <code>String</code> object representing this
     * <code>PyInt</code>'s value.  The value is converted to an
     * <code>Integer</code> object and returned as a string, exactly as if
     * the <code>int</code> value were given as an argument to the
     * {@link java.lang.Integer#toString(short)} method.
     *
     * @return  a string representation of this object.
     * @see     Integer#toString()
     * @since   0.0.1
     */
    @Override
    public final String toString() {
        return Integer.toString(this.value);
    }

    /**
     * Visits this <tt>PyInt</tt> instance with an object
     * implementing <code>PyVisitor</code> interface.
     *
     * @param   visitor visitor used
     * @return  <code>true</code> if the visit was successfull;
     *          <code>false</code> otherwise.
     * @throws  IOException if an error occurred
     * @see     PyVisitable
     * @since   0.0.1
     */
    @Override
    public final boolean visit(final PyVisitor visitor) throws IOException {
        return (visitor.visit(this));
    }

    /**
     * Compares this <tt>PyInt</tt> instance with another <tt>PyBase</tt>
     * instance.
     *
     * @param   other the <tt>PyBase</tt> instance to be compared
     * @return  zero if both objects share the same <tt>PyType</tt> and
     *          this object represents the same <code>int</code> value as the
     *          argument; a positive value if this object's value is greater
     *          then the argument's value; and a negative value if
     *          this object's value is smaller then the argument's value;
     *          if both objects are of different <tt>PyType</tt> their
     *          <tt>PyType</tt>'s are compared.
     * @see     Comparable
     * @see     PyType
     * @see     Integer#compareTo(Integer)
     * @since   0.0.1
     */
    @Override
    public final int compareTo(final PyBase other) {
        if (other.getType() == this.getType()) {
            return Integer.valueOf(this.value).compareTo(
                    Integer.valueOf(other.asInt().intValue()));
        } else {
            return other.getType().compareTo(this.getType());
        }
    }

}
