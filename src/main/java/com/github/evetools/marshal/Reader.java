package com.github.evetools.marshal;

import com.github.evetools.marshal.python.PyBase;
import com.github.evetools.marshal.python.PyBool;
import com.github.evetools.marshal.python.PyBuffer;
import com.github.evetools.marshal.python.PyByte;
import com.github.evetools.marshal.python.PyDBRowDescriptor;
import com.github.evetools.marshal.python.PyDict;
import com.github.evetools.marshal.python.PyDouble;
import com.github.evetools.marshal.python.PyGlobal;
import com.github.evetools.marshal.python.PyInt;
import com.github.evetools.marshal.python.PyList;
import com.github.evetools.marshal.python.PyLong;
import com.github.evetools.marshal.python.PyMarker;
import com.github.evetools.marshal.python.PyNone;
import com.github.evetools.marshal.python.PyObject;
import com.github.evetools.marshal.python.PyObjectEx;
import com.github.evetools.marshal.python.PyPackedRow;
import com.github.evetools.marshal.python.PyShort;
import com.github.evetools.marshal.python.PyString;
import com.github.evetools.marshal.python.PyTuple;
import com.google.common.collect.Maps;
import com.jcraft.jzlib.JZlib;
import com.jcraft.jzlib.ZStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Map;

/**
 * Copyright (C)2011 by Gregor Anders
 * All rights reserved.
 *
 * This code is free software; you can redistribute it and/or modify
 * it under the terms of the BSD license (see the file LICENSE.txt
 * included with the distribution).
 */
public class Reader {

	private static class Buffer {

		private final ByteBuffer buffer;
		private final Map<Integer, PyBase> shared;
		private ByteBuffer sharedBuffer;
		PyBase latest;

		Buffer(byte[] bytes) {
			this.buffer = ByteBuffer.wrap(bytes);
			this.buffer.order(ByteOrder.LITTLE_ENDIAN);
			shared = Maps.newHashMap();
		}

		public PyBase putReference(int key, PyBase value) {
			return shared.put(key, value);
		}

		public PyBase getReference(int key) {
			return shared.get(key);
		}

		public PyBase getLatest() {
			return latest;
		}

		public void setLatest(PyBase latest) {
			this.latest = latest;
		}

		public final int length() {
			return this.buffer.array().length;
		}

		public final byte peekByte() {
			final byte b = this.buffer.get();
			this.buffer.position(this.buffer.position() - 1);
			return b;
		}

		public final byte[] peekBytes(int offset, int size) {
			byte[] bytes = null;
			final int position = this.buffer.position();
			this.buffer.position(offset);
			bytes = this.readBytes(size);
			this.buffer.position(position);
			return bytes;
		}

		public final int position() {
			return this.buffer.position();
		}

		public final byte readByte() {
			return this.buffer.get();
		}

		public final byte[] readBytes(int size) {
			final byte[] bytes = new byte[size];
			this.buffer.get(bytes);
			return bytes;
		}

		public final double readDouble() {
			return this.buffer.getDouble();
		}

		public final int readInt() {
			return this.buffer.getInt();
		}

		public final long readLong() {
			return this.buffer.getLong();
		}

		public final short readShort() {
			return this.buffer.getShort();
		}
		public final int readLength() {
			int length = 0;
			length = readByte() & 0xFF;
			if (length == 255) {
				length = readInt();
			}
			return length;
		}

		private void initSharedVector() {
			final int size = readInt();
			final int offset = length() - (size * 4);
			sharedBuffer = ByteBuffer.wrap(peekBytes(offset, (size * 4)));
			sharedBuffer.order(ByteOrder.LITTLE_ENDIAN);
		}

		private int readSharedInt() {
			return sharedBuffer.getInt();
		}
	}

	interface Provider<T> {
		T read(Buffer buffer) throws IOException;
	}

	private static byte fromBitSet(BitSet bitSet) {
		byte b = 0;

		for (int i = 0; i < bitSet.length(); i++) {
			if (bitSet.get(i)) {
				b |= 1 << i;
			}
		}
		return b;
	}

	static enum ParseProvider implements Provider<PyBase> {
		ERROR(0x00) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				throw new IOException("ERROR");
			}
		},
		NOT_IMPLEMENTED(0x0c, 0x0d) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadNotImplemented(buffer);
			}
		},
		NONE(0x01) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return new PyNone();
			}
		},
		GLOBAL(new int[]{0x02}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				final byte[] bytes = buffer.readBytes(buffer.readLength());
				return new PyGlobal(new String(bytes));
			}
		},
		LONG(new int[]{0x03}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadLong(buffer);
			}
		},
		INT(new int[]{0x04}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadInt(buffer);
			}
		},
		SHORT(new int[]{0x05}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadShort(buffer);
			}
		},
		BYTE(new int[]{0x06}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadByte(buffer);
			}
		},
		INT_MINUS_ONE(new int[]{0x07}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadIntMinus1(buffer);
			}
		},
		INT_ZERO(new int[]{0x08}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadInt0(buffer);
			}
		},
		INT_ONE(new int[]{0x09}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadInt1(buffer);
			}
		},
		DOUBLE(new int[]{0x0a}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadDouble(buffer);
			}
		},
		DOUBLE_ZERO(new int[]{0x0a}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadDouble0(buffer);
			}
		},
		STRING_ZERO(new int[]{0x0e}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadString0(buffer);
			}
		},
		STRING_ONE(new int[]{0x0f}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadString1(buffer);
			}
		},
		STRING(new int[]{0x10}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadString(buffer);
			}
		},
		STRING_REF(new int[]{0x11}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadStringRef(buffer);
			}
		},
		STRING_UNICODE(new int[]{0x12}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadUnicode(buffer);
			}
		},
		BUFFER(new int[]{0x13}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadBuffer(buffer);
			}
		},
		TUPLE(new int[]{0x14}) {
			@Override public PyBase read(Buffer buffer) throws IOException {
				return Reader.loadTuple(buffer);
			}
		},

		;

		int[] supported;
		private ParseProvider(int... supported) {
			this.supported = new int[supported.length];
			System.arraycopy(supported, 0, this.supported, 0, supported.length);
		}

		private static final Map<Integer, ParseProvider> cache = Maps.newHashMap();
		static {
			for (ParseProvider pp : values()) {
				for (int i : pp.supported) {
					if (!cache.containsKey(i)) {
						cache.put(i, pp);
					} else {
						throw new AssertionError("Duplicate entries for the opcode: "
								+ i
								+ " first: " + cache.get(i).name()
								+ " second: " + pp.name()
								);
					}
				}
			}
		}
		public static ParseProvider from(int marker) {
			if (cache.containsKey(marker)) {
				return cache.get(marker);
			}
			throw new IllegalArgumentException("There is no available parser "
					+ "for the marker: 0x"
					+ Integer.toHexString(0xFF & marker)
					+ " [actual: " + marker + "]"
					);
		}
	}

	private final Buffer buffer;

//	private final Provider[] loadMethods = new Provider[] {
//
//
//	/* 0x15 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadList();
//		}
//	},
//	/* 0x16 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadDict();
//		}
//	},
//	/* 0x17 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadInstance();
//		}
//	},
//	/* 0x18 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x19 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x1a */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x1b */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadReference();
//		}
//	},
//	/* 0x1c */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x1d */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x1e */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x1f */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadTrue();
//		}
//	},
//	/* 0x20 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadFalse();
//		}
//	},
//	/* 0x21 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x22 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadObjectEx();
//		}
//	},
//	/* 0x23 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadObjectEx();
//		}
//	},
//	/* 0x24 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x25 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadTuple1();
//		}
//	},
//	/* 0x26 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadList0();
//		}
//	},
//	/* 0x27 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadList1();
//		}
//	},
//	/* 0x28 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadUnicode0();
//		}
//	},
//	/* 0x29 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadUnicode1();
//		}
//	},
//	/* 0x2a */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadPacked();
//		}
//	},
//	/* 0x2b */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadSubStream();
//		}
//	},
//	/* 0x2c */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadTuple2();
//		}
//	},
//	/* 0x2d */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadMarker();
//		}
//	},
//	/* 0x2e */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadBuffer();
//		}
//	},
//	/* 0x2f */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadVarInt();
//		}
//	},
//	/* 0x30 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x31 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x32 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x33 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x34 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x35 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x36 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x37 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x38 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x39 */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	},
//	/* 0x3a */new Provider() {
//		@Override
//		public PyBase read() throws IOException {
//			return Reader.this.loadNotImplemented();
//		}
//	}
//	};

	private Reader(Buffer buffer) throws IOException {
		this.buffer = buffer;
	}

	public Reader(File file) throws IOException {
		this(new FileInputStream(file));
	}

	public Reader(InputStream stream) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		final byte[] bytes = new byte[4096];

		int read = -1;
		while (0 <= (read = stream.read(bytes))) {
			baos.write(bytes, 0, read);
		}

		stream.close();

		this.buffer = new Buffer(baos.toByteArray());
	}

	private PyDBRowDescriptor toDBRowDescriptor(PyBase base)
			throws IOException {

		if (!(base instanceof PyObjectEx)) {
			throw new IOException("Invalid Packed Row header: "
					+ base.getType());
		}

		final PyObjectEx object = (PyObjectEx) base;

		return new PyDBRowDescriptor(object);
	}


	private static PyBase loadBuffer(Buffer buffer) throws IOException {
		final int size = buffer.readLength();
		final byte[] bytes = buffer.readBytes(size);

		if (bytes[0] == 0x78) {

			final byte[] zlibbytes = new byte[bytes.length + 1];
			System.arraycopy(bytes, 0, zlibbytes, 0, bytes.length);
			zlibbytes[zlibbytes.length - 1] = 0;

			int zlen = zlibbytes.length * 2;
			byte[] zout;

			boolean success = false;
			final ZStream zstream = new ZStream();
			int res = 0;

			while (!success) {

				zout = new byte[zlen];

				zstream.next_in = zlibbytes;
				zstream.next_in_index = 0;
				zstream.next_out = zout;
				zstream.next_out_index = 0;

				if (zstream.inflateInit() != JZlib.Z_OK) {
					throw new IOException("Error uncompressing zlib buffer");
				}

				while ((zstream.total_out < zlen)
						&& (zstream.total_in < zlibbytes.length)) {
					zstream.avail_in = 1;
					zstream.avail_out = 1;
					res = zstream.inflate(JZlib.Z_NO_FLUSH);
					if (res == JZlib.Z_STREAM_END) {
						success = true;
						break;
					}

					if (res == JZlib.Z_DATA_ERROR) {
						return new PyBuffer(bytes);
					}
				}

				if (zstream.total_out < zlen) {
					break;
				}

				if (!success) {
					zout = null;
					zlen = zlen * 2;
				} else {
					zstream.inflateEnd();

					/*
					 * for debugging byte[] uncom = new byte[(int)
					 * zstream.total_out]; for (int loop = 0; loop <
					 * uncom.length; loop++) { uncom[loop] = zout[loop]; }
					 */

					final Buffer buf = new Buffer(zout);
					final Reader reader = new Reader(buf);

					return reader.read();
				}
			}
		}
		return new PyBuffer(bytes);
	}

	private static PyBase loadByte(Buffer buffer) throws IOException {
		final byte valueByte = buffer.readByte();
		return new PyByte(valueByte);
	}

	private PyBase loadDict() throws IOException {
		final int size = buffer.readLength();

		PyBase key = null;
		PyBase value = null;

		final PyDict dict = new PyDict();

		for (int loop = 0; loop < size; loop++) {
			value = this.loadPy(buffer);
			key = this.loadPy(buffer);
			dict.put(key, value);
		}

		return dict;
	}

	private static PyBase loadDouble(Buffer buffer) throws IOException {
		return new PyDouble(buffer.readDouble());
	}

	private static PyBase loadDouble0(Buffer buffer) throws IOException {
		return new PyDouble(0);
	}

	private static PyBase loadFalse(Buffer buffer) throws IOException {
		return new PyBool(false);
	}

	private PyBase loadInstance(Buffer buffer) throws IOException {
		return new PyObject(loadPy(buffer), loadPy(buffer));
	}

	private static PyBase loadInt(Buffer buffer) throws IOException {
		return new PyInt(buffer.readInt());
	}

	private static PyBase loadInt0(Buffer buffer) throws IOException {
		return new PyInt(0);
	}

	private static PyBase loadInt1(Buffer buffer) throws IOException {
		return new PyInt(1);
	}

	private static PyBase loadIntMinus1(Buffer buffer) throws IOException {
		return new PyInt(-1);
	}

	private PyBase loadList() throws IOException {
		return this.loadList(buffer.readLength());
	}

	private PyBase loadList(int size) throws IOException {
		final PyList tuple = new PyList();
		PyBase base = null;
		int curSize = size;
		while (curSize > 0) {
			base = this.loadPy(buffer);
			if (base == null) {
				throw new IOException("null element in list found");
			}
			tuple.add(base);
			curSize--;
		}
		return tuple;
	}

	private PyBase loadList0() throws IOException {
		return this.loadList(0);
	}

	private PyBase loadList1() throws IOException {
		return this.loadList(1);
	}

	private PyBase loadMarker() throws IOException {
		return new PyMarker();
	}

	private static PyBase loadNotImplemented(Buffer buffer) throws IOException {
		byte[] type = buffer.peekBytes(buffer.position()-1, 1);

		throw new IOException("Not implemented: "
				+ Integer.toHexString(type[0]) + " at: " + buffer.position());
	}

	private static PyBase loadObjectEx(Buffer buffer) throws IOException {

		final PyObjectEx objectex = new PyObjectEx();

		buffer.setLatest(objectex);

		objectex.setHead(loadPy(buffer));

		while (buffer.peekByte() != 0x2d) {
			objectex.getList().add(loadPy(buffer));
		}
		buffer.readByte();

		PyBase key = null;
		PyBase value = null;

		while (buffer.peekByte() != 0x2d) {
			value = loadPy(buffer);
			key = loadPy(buffer);
			objectex.getDict().put(key, value);
		}
		buffer.readByte();

		return objectex;
	}

	private PyBase loadPacked() throws IOException {

		final PyBase head = this.loadPy(buffer);
		int size = buffer.readLength();
		final byte[] bytes = this.buffer.readBytes(size);

		final PyPackedRow base = new PyPackedRow(head, new PyBuffer(bytes));

		final PyDBRowDescriptor desc = this.toDBRowDescriptor(head);

		size = desc.size();

		final byte[] out = this.zerouncompress(bytes, size);

		final Buffer outbuf = new Buffer(out);

		ArrayList<PyBase> list = desc.getTypeMap().get(Integer.valueOf(0));

		for (final PyBase pyBase : list) {
			final PyTuple tuple = pyBase.asTuple();
			if (((PyByte) tuple.get(1)).getValue() == 5) {
				base.put(tuple.get(0), new PyDouble(outbuf.readDouble()));
			} else {
				base.put(tuple.get(0), new PyLong(outbuf.readLong()));
			}
		}

		list = desc.getTypeMap().get(Integer.valueOf(1));

		for (final PyBase pyBase : list) {
			final PyTuple tuple = pyBase.asTuple();
			base.put(tuple.get(0), new PyInt(outbuf.readInt()));
		}

		list = desc.getTypeMap().get(Integer.valueOf(2));

		for (final PyBase pyBase : list) {
			final PyTuple tuple = pyBase.asTuple();
			base.put(tuple.get(0), new PyShort(outbuf.readShort()));
		}

		list = desc.getTypeMap().get(Integer.valueOf(3));

		for (final PyBase pyBase : list) {
			final PyTuple tuple = pyBase.asTuple();
			base.put(tuple.get(0), new PyByte(outbuf.readByte()));
		}

		list = desc.getTypeMap().get(Integer.valueOf(4));

		int boolcount = 0;
		int boolvalue = 0;

		for (final PyBase pyBase : list) {
			final PyTuple tuple = pyBase.asTuple();

			if (boolcount == 0) {
				boolvalue = outbuf.readByte();
			}

			final boolean val = ((boolvalue >> boolcount++) & 0x01) > 0 ? true
					: false;

			base.put(tuple.get(0), new PyBool(val));

			if (boolcount == 8) {
				boolcount = 0;
			}
		}

		list = desc.getTypeMap().get(Integer.valueOf(5));

		for (final PyBase pyBase : list) {
			final PyTuple tuple = pyBase.asTuple();
			base.put(tuple.get(0), loadPy(buffer));
		}
		return base;
	}

	private static PyBase loadPy(Buffer buffer) throws IOException {

		final byte magic = buffer.readByte();
		final boolean sharedPy = (magic & 0x40) != 0;
		int type = magic;
		type = (type & 0x3f);

		final PyBase pyBase = ParseProvider.from(type).read(buffer);

		if (sharedPy) {
			// this is a dirty hack and maybe leads to errors
			if ((pyBase.isGlobal())
					&& (pyBase.asGlobal().getValue().endsWith(
							"blue.DBRowDescriptor"))) {
				buffer.putReference(buffer.readSharedInt(), buffer.getLatest());
			} else {
				buffer.putReference(buffer.readSharedInt(), pyBase);
			}
		}

		return pyBase;
	}

	private PyBase loadReference(Buffer buffer) throws IOException {
		return buffer.getReference(Integer.valueOf(buffer.readLength()));
	}

	private static PyBase loadLong(Buffer buffer) {
		return new PyLong(buffer.readLong());
	}

	private static PyBase loadShort(Buffer buffer) throws IOException {
		return new PyShort(buffer.readShort());
	}

	private static PyBase loadString(Buffer buffer) throws IOException {
		return new PyString(new String(buffer.readBytes(buffer.readLength())));
	}

	private static PyBase loadString0(Buffer buffer) throws IOException {
		return new PyString("");
	}

	private static PyBase loadString1(Buffer buffer) throws IOException {
		return new PyString(new String(buffer.readBytes(1)));
	}

	private static PyBase loadStringRef(Buffer buffer) throws IOException {
		return new PyString(Strings.get(buffer.readLength()));
	}

	private PyBase loadSubStream() throws IOException {
		final int size = buffer.readLength();
		final Buffer buf = new Buffer(this.buffer.readBytes(size));
		final Reader reader = new Reader(buf);
		return reader.read();
	}

	private PyBase loadTrue() throws IOException {
		return new PyBool(true);
	}

	private static PyBase loadTuple(Buffer buffer) throws IOException {
		return loadTuple(buffer, buffer.readLength());
	}

	private static PyBase loadTuple(Buffer buffer, int size) throws IOException {
		final PyTuple tuple = new PyTuple();
		PyBase base = null;
		int curSize = size;
		while (curSize > 0) {
			base = loadPy(buffer);
			if (base == null) {
				throw new IOException("null element in tuple found");
			}
			tuple.add(base);
			curSize--;
		}
		return tuple;
	}

	private PyBase loadTuple1(Buffer buffer) throws IOException {
		return loadTuple(buffer, 1);
	}

	private PyBase loadTuple2(Buffer buffer) throws IOException {
		return loadTuple(buffer, 2);
	}

	private static PyBase loadUnicode(Buffer buffer) throws IOException {
		return new PyString(new String(buffer.readBytes(buffer.readLength() * 2)));
	}

	private PyBase loadUnicode0() throws IOException {
		return new PyString("");
	}

	private PyBase loadUnicode1() throws IOException {
		return new PyString(new String(this.buffer.readBytes(2)));
	}

	private PyBase loadVarInt() throws IOException {

		final int size = buffer.readLength();

		switch (size) {
		case 0:
			return new PyLong(0);
		case 2:
			return loadShort(buffer);
		case 4:
			return loadInt(buffer);
		case 8:
			return loadLong(buffer);
		default:
			final byte[] bytes = this.buffer.readBytes(size);

			final BigInteger bi = new BigInteger(bytes);

			return new PyLong(bi.longValue());
		}
	}

	public PyBase read() throws IOException {

		this.buffer.readByte(); // throw the first byte away. it's the protocol marker
		buffer.initSharedVector();

		PyBase base = null;

		base = this.loadPy(buffer);

		return base;
	}

	private byte[] zerouncompress(byte[] bytes, int size) throws IOException {

		final byte[] out = new byte[size + 16];
		int outpos = 0;
		byte current = 0;
		int length = 0;
		int pos = 0;

		for (int loop = 0; loop < out.length; loop++) {
			out[loop] = 0;
		}

		while (pos < bytes.length) {

			current = bytes[pos++];

			final BitSet bitSet = new BitSet(8);
			for (int i = 0; i < 8; i++) {
				if ((current & (1 << i)) > 0) {
					bitSet.set(i);
				}
			}

			if (bitSet.get(3)) {
				length = Reader.fromBitSet(bitSet.get(0, 3)) + 1;
				for (int i = 0; i < length; i++) {
					out[outpos++] = 0;
				}
			} else {
				length = 8 - Reader.fromBitSet(bitSet.get(0, 3));
				for (int i = 0; i < length; i++) {
					out[outpos++] = bytes[pos++];
				}
			}

			if (bitSet.get(7)) {
				length = Reader.fromBitSet(bitSet.get(4, 7)) + 1;
				for (int i = 0; i < length; i++) {
					out[outpos++] = 0;
				}
			} else {
				length = 8 - Reader.fromBitSet(bitSet.get(4, 7));
				for (int i = 0; (i < length) && (pos < bytes.length); i++) {
					out[outpos++] = bytes[pos++];
				}
			}
		}

		return out;
	}
}
