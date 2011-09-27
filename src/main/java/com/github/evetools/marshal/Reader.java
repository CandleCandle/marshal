package com.github.evetools.marshal;

import com.github.evetools.marshal.python.PyBase;
import com.github.evetools.marshal.python.PyBool;
import com.github.evetools.marshal.python.PyBuffer;
import com.github.evetools.marshal.python.PyByte;
import com.github.evetools.marshal.python.PyDBRowDescriptor;
import com.github.evetools.marshal.python.PyDBColumn;
import com.github.evetools.marshal.python.PyDBColumn.DBColumnType;
import com.github.evetools.marshal.python.PyDict;
import com.github.evetools.marshal.python.PyDouble;
import com.github.evetools.marshal.python.PyGlobal;
import com.github.evetools.marshal.python.PyInt;
import com.github.evetools.marshal.python.PyList;
import com.github.evetools.marshal.python.PyLong;
import com.github.evetools.marshal.python.PyNone;
import com.github.evetools.marshal.python.PyObject;
import com.github.evetools.marshal.python.PyObjectEx;
import com.github.evetools.marshal.python.PyDBRow;
import com.github.evetools.marshal.python.PyShort;
import com.github.evetools.marshal.python.PyTuple;
import com.google.common.collect.Lists;
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
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * Copyright (C)2011 by Gregor Anders All rights reserved.
 *
 * This code is free software; you can redistribute it and/or modify it under
 * the terms of the BSD license (see the file LICENSE.txt included with the
 * distribution).
 */
public class Reader {

	public static class WrappedBuffer {

		private final ByteBuffer buffer;
		private final Map<Integer, PyBase> shared;
		private final Stack<PyBase> objects;
		private final Map<PyBase, PyDBRowDescriptor> descriptors;
		private ByteBuffer sharedBuffer;
		private PyBase latest;
		private int depth = 0;
		private final List<ParsePosition> tokensRead;

		WrappedBuffer(byte[] bytes) {
			this.buffer = ByteBuffer.wrap(bytes);
			this.buffer.order(ByteOrder.LITTLE_ENDIAN);
			shared = Maps.newHashMap();
			descriptors = Maps.newHashMap();
			tokensRead = Lists.newArrayList();
			objects = new Stack<PyBase>();
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
			int size = readInt();
			size *= (Integer.SIZE / Byte.SIZE);
			final int offset = length() - size;

			sharedBuffer = ByteBuffer.wrap(peekBytes(offset, size));
			sharedBuffer.order(ByteOrder.LITTLE_ENDIAN);
		}

		private int readSharedInt() {
			return sharedBuffer.getInt();
		}

		private int getPostion() {
			return buffer.position();
		}

		public PyBase push(PyBase item) {
			return objects.push(item);
		}
		public PyBase pop() {
			return objects.pop();
		}
		public PyBase peek() {
			return objects.peek();
		}

		public PyDBRowDescriptor putDescriptor(PyBase key, PyDBRowDescriptor value) {
			return descriptors.put(key, value);
		}
		public PyDBRowDescriptor getDescriptor(PyBase key) {
			return descriptors.get(key);
		}
		public boolean containDescriptor(PyBase key) {
			return descriptors.containsKey(key);
		}

		int getDepth() { return depth; }
		void incrementDepth() { depth++; }
		void decrementDepth() { depth--; }
		void tokenFound(Provider<PyBase> provider) {
			tokensRead.add(new ParsePosition(provider, getPostion(), getDepth()));
		}
	}
	static class ParsePosition {
		private final Provider<PyBase> provider;
		private final int position;
		private final int depth;
		ParsePosition(Provider<PyBase> provider, int position, int depth) {
			this.provider = provider;
			this.position = position;
			this.depth = depth;
		}
		int getDepth() {
			return depth;
		}
		int getPosition() {
			return position;
		}
		Provider<PyBase> getProvider() {
			return provider;
		}
		@Override public String toString() {
			return getProvider() + " at " + getPosition() + " at " + getDepth() + " deep.";
		}
	}

	interface Provider<T> {
		T read(WrappedBuffer buffer) throws IOException;
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
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				throw new IOException("ERROR");
			}
		},
		NOT_IMPLEMENTED(
				0x0c, 0x0d, 0x18, 0x19,
				0x1a, 0x1c, 0x1d, 0x1e,
				0x21, 0x24, 0x30, 0x31,
				0x32, 0x33, 0x34, 0x35,
				0x36, 0x37, 0x38, 0x39,
				0x3a, 0x2d
				) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadNotImplemented(buffer);
			}
		},
		NONE(0x01) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return new PyNone();
			}
		},
		GLOBAL(0x02) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				final byte[] bytes = buffer.readBytes(buffer.readLength());
				return new PyGlobal(bytes);
			}
		},
		LONG(0x03) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadLong(buffer);
			}
		},
		INT(0x04) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadInt(buffer);
			}
		},
		SHORT(0x05) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadShort(buffer);
			}
		},
		BYTE(0x06) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadByte(buffer);
			}
		},
		INT_MINUS_ONE(0x07) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadIntMinus1(buffer);
			}
		},
		INT_ZERO(0x08) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadInt0(buffer);
			}
		},
		INT_ONE(0x09) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadInt1(buffer);
			}
		},
		DOUBLE(0x0a) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadDouble(buffer);
			}
		},
		DOUBLE_ZERO(0x0b) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadDouble0(buffer);
			}
		},
		STRING_ZERO(0x0e) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadString0(buffer);
			}
		},
		STRING_ONE(0x0f) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadString1(buffer);
			}
		},
		STRING(0x10) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadString(buffer);
			}
		},
		STRING_REF(0x11) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadStringRef(buffer);
			}
		},
		STRING_UNICODE(0x12) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadUnicode(buffer);
			}
		},
		BUFFER(0x13, 0x2e) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadBuffer(buffer);
			}
		},
		TUPLE(0x14) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadTuple(buffer);
			}
		},
		LIST(0x15) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadList(buffer);
			}
		},
		DICT(0x16) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadDict(buffer);
			}
		},
		INSTANCE(0x17) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadInstance(buffer);
			}
		},
		REFERENCE(0x1b) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadReference(buffer);
			}
		},
		TRUE(0x1f) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadTrue(buffer);
			}
		},
		FALSE(0x20) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadFalse(buffer);
			}
		},
		OBJECT_REDUCE(0x22) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadObjectReduce(buffer);
			}
		},
		OBJECT_EX(0x23) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadObjectEx(buffer);
			}
		},
		TUPLE_ONE(0x25) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadTuple1(buffer);
			}
		},
		LIST_ZERO(0x26) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadList0(buffer);
			}
		},
		LIST_ONE(0x27) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadList1(buffer);
			}
		},
		UNICODE_ZERO(0x28) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadUnicode0(buffer);
			}
		},
		UNICODE_ONE(0x29) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadUnicode1(buffer);
			}
		},
		PACKED(0x2a) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadPacked(buffer);
			}
		},
		SUB_STREAM(0x2b) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadPacked(buffer);
			}
		},
		TUPLE_TWO(0x2c) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadTuple2(buffer);
			}
		},
		VAR_INT(0x2f) {
			@Override public PyBase read(WrappedBuffer buffer) throws IOException {
				return Reader.loadVarInt(buffer);
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
//			Map<Integer, ParseProvider> list = Maps.newTreeMap();
			for (ParseProvider pp : values()) {
				for (int i : pp.supported) {
					if (!cache.containsKey(i)) {
						cache.put(i, pp);
//						list.put(i, pp);
					} else {
						throw new AssertionError("Duplicate entries for the opcode: "
								+ i
								+ " first: " + cache.get(i).name()
								+ " second: " + pp.name()
								);
					}
				}
			}
//			for (Map.Entry<Integer, ParseProvider> e : list.entrySet()) {
//				System.out.println(e.getKey() + " [" + Integer.toHexString(e.getKey()) + "] --> " + e.getValue());
//			}
		}
		public static ParseProvider from(int marker) throws NoSuchProviderException {
			if (cache.containsKey(marker)) {
				return cache.get(marker);
			}
			throw new NoSuchProviderException("There is no available parser "
					+ "for the marker: 0x"
					+ Integer.toHexString(0xFF & marker)
					+ " [actual: " + marker + "]"
					);
		}
	}


	private final WrappedBuffer buffer;

	private Reader(WrappedBuffer buffer) throws IOException {
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

		this.buffer = new WrappedBuffer(baos.toByteArray());
	}

	private static PyDBRowDescriptor toDBRowDescriptor(PyBase base)
			throws IOException {

		if (!(base instanceof PyObjectEx)) {
			throw new IOException("Invalid Packed Row header: "
					+ base.getType());
		}

		final PyObjectEx object = (PyObjectEx) base;

		return new PyDBRowDescriptor(object);
	}


	private static PyBase loadBuffer(WrappedBuffer buffer) throws IOException {
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

					if (res == JZlib.Z_DATA_ERROR || res == JZlib.Z_NEED_DICT) {
						return new PyBuffer(bytes);
					}
				}

				if (!success) {
					zout = null;
					zlen = zlen * 2;
				} else {
					zstream.inflateEnd();

					byte[] uncom = new byte[(int) zstream.total_out];
					System.arraycopy(zout, 0, uncom, 0, uncom.length);

					final WrappedBuffer buf = new WrappedBuffer(uncom);
					final Reader reader = new Reader(buf);

					return reader.read();
				}
			}
		}
		return new PyBuffer(bytes);
	}

	private static PyBase loadByte(WrappedBuffer buffer) throws IOException {
		final byte valueByte = buffer.readByte();
		return new PyByte(valueByte);
	}

	private static PyBase loadDict(WrappedBuffer buffer) throws IOException {
		final int size = buffer.readLength();

		PyBase key = null;
		PyBase value = null;

		final PyDict dict = new PyDict();

		for (int loop = 0; loop < size; loop++) {
			value = loadPy(buffer);
			key = loadPy(buffer);
			dict.put(key, value);
		}

		return dict;
	}

	private static PyBase loadDouble(WrappedBuffer buffer) throws IOException {
		return new PyDouble(buffer.readDouble());
	}

	private static PyBase loadDouble0(WrappedBuffer buffer) throws IOException {
		return new PyDouble(0);
	}

	private static PyBase loadFalse(WrappedBuffer buffer) throws IOException {
		return new PyBool(false);
	}

	private PyBase loadGlobal() throws IOException {
		final byte[] bytes = buffer.readBytes(buffer.readLength());
		return new PyGlobal(bytes);
	}

	private static PyBase loadInstance(WrappedBuffer buffer) throws IOException {
		PyObject object = new PyObject();
		buffer.push(object);

		PyBase head = loadPy(buffer);
		object.setHead(head);
		PyBase content = loadPy(buffer);
		object.setContent(content);
		buffer.pop();
		return object;
	}

	private static PyBase loadInt(WrappedBuffer buffer) throws IOException {
		return new PyInt(buffer.readInt());
	}

	private static PyBase loadInt0(WrappedBuffer buffer) throws IOException {
		return new PyInt(0);
	}

	private static PyBase loadInt1(WrappedBuffer buffer) throws IOException {
		return new PyInt(1);
	}

	private static PyBase loadIntMinus1(WrappedBuffer buffer) throws IOException {
		return new PyInt(-1);
	}

	private static PyBase loadList(WrappedBuffer buffer) throws IOException {
		return loadList(buffer, buffer.readLength());
	}

	private static PyBase loadList(WrappedBuffer buffer, int size) throws IOException {
		final PyList tuple = new PyList();
		PyBase base = null;
		int curSize = size;
		while (curSize > 0) {
			base = loadPy(buffer);
			if (base == null) {
				throw new IOException("null element in list found");
			}
			tuple.add(base);
			curSize--;
		}
		return tuple;
	}

	private static PyBase loadList0(WrappedBuffer buffer) throws IOException {
		return loadList(buffer, 0);
	}

	private static PyBase loadList1(WrappedBuffer buffer) throws IOException {
		return loadList(buffer, 1);
	}

	private static PyBase loadNotImplemented(WrappedBuffer buffer) throws IOException {
		byte[] type = buffer.peekBytes(buffer.position()-1, 1);

		throw new IOException("Not implemented: "
				+ Integer.toHexString(type[0]) + " at: " + buffer.position());
	}

	private static PyBase loadObjectReduce(WrappedBuffer buffer) throws IOException {
		return loadObjectEx(buffer, true);
	}

	private static PyBase loadObjectEx(WrappedBuffer buffer) throws IOException {
		return loadObjectEx(buffer, false);
	}


	private static PyBase loadObjectEx(WrappedBuffer buffer, boolean reduce) throws IOException {

		final PyObjectEx objectex = new PyObjectEx(reduce);

		buffer.push(objectex);
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

		buffer.pop();

		return objectex;
	}

	private static PyBase loadPacked(WrappedBuffer buffer) throws IOException {

		final PyBase head = loadPy(buffer);
		int size = buffer.readLength();
		final byte[] bytes = buffer.readBytes(size);

		if (head == null) {
			throw new IOException("Invalid PackedRow header");
		}
		if (!buffer.containDescriptor(head)) {
			buffer.putDescriptor(head, toDBRowDescriptor(head));
		}

		PyDBRowDescriptor desc = buffer.getDescriptor(head);
		final PyDBRow base = new PyDBRow(desc);

		size = desc.size();

		final byte[] out = zerouncompress(bytes, size);

		final WrappedBuffer outbuf = new WrappedBuffer(out);

		List<PyDBColumn> list = desc.getColumns();

		int boolcount = 0;
		int boolvalue = 0;

		for (PyDBColumn pyDBColumn : list) {

			if (pyDBColumn.getDBType() == DBColumnType.BOOL) {

				if (boolcount == 0) {
					boolvalue = outbuf.readByte();
				}

				final boolean val = ((boolvalue >> boolcount++) & 0x01) > 0 ? true
						: false;

				base.put(pyDBColumn.getName(), new PyBool(val));

				if (boolcount == 8) {
					boolcount = 0;
				}

			} else if (pyDBColumn.getDBType() == DBColumnType.STRING
					|| pyDBColumn.getDBType() == DBColumnType.USTRING) {
				base.put(pyDBColumn.getName(), loadPy(buffer));
			} else {
				base.put(pyDBColumn.getName(), pyDBColumn.getDBType().read(outbuf));
			}

		}

		return base;
	}

//	static PyBase root = null;

	private static PyBase loadPy(WrappedBuffer buffer) throws IOException {
//		if (root != null) {
//			System.out.println("----------root ---------");
//			root.visit(new PyDumpVisitor());
//		}
		final byte magic = buffer.readByte();
		final boolean sharedPy = (magic & 0x40) != 0;
		final int type = (magic & 0x3f);

		final PyBase pyBase;
		try {
			buffer.incrementDepth();
			ParseProvider provider = ParseProvider.from(type);
			buffer.tokenFound(provider);
			pyBase = provider.read(buffer);
			buffer.decrementDepth();
		} catch (NoSuchProviderException nspe) {
			for (ParsePosition entry : buffer.tokensRead) {
				for (int i = 0; i < entry.getDepth(); ++i) {
					System.err.print(".\t");
				}
				System.err.print(entry.getProvider());
				System.err.print(" [");
				System.err.print(entry.getPosition());
				System.err.println("]");
			}
			throw new IllegalArgumentException("Failed to find"
				+ " a valid provider for position: " + buffer.getPostion()
				+ ". Original magic is " + Integer.toHexString(magic)
				+ ". Type is " + Integer.toHexString(type)
				+ ". Wrapped message is: " + nspe.getMessage(), nspe
				);
		}

		PyBase pyShared = null;
		if (sharedPy) {

			if (pyBase.isGlobal()) {
				pyShared = buffer.peek();
			} else {
				pyShared = pyBase;
			}
			buffer.putReference(buffer.readSharedInt(), pyShared);

		}

//		System.out.println("---------- return at position "+buffer.getPostion()+"---------");
//		pyBase.visit(new PyDumpVisitor());
		return pyBase;
	}

	private static PyBase loadReference(WrappedBuffer buffer) throws IOException {
		return buffer.getReference(Integer.valueOf(buffer.readLength()));
	}

	private static PyBase loadLong(WrappedBuffer buffer) {
		return new PyLong(buffer.readLong());
	}

	private static PyBase loadShort(WrappedBuffer buffer) throws IOException {
		return new PyShort(buffer.readShort());
	}

	private static PyBase loadString(WrappedBuffer buffer) throws IOException {
		return new PyBuffer(buffer.readBytes(buffer.readLength()));
	}

	private static PyBase loadString0(WrappedBuffer buffer) throws IOException {
		return new PyBuffer(new byte[]{});
	}

	private static PyBase loadString1(WrappedBuffer buffer) throws IOException {
		return new PyBuffer(buffer.readBytes(1));
	}

	private static PyBase loadStringRef(WrappedBuffer buffer) throws IOException {
		return new PyBuffer(Strings.get(buffer.readLength()));
	}

	private static PyBase loadTrue(WrappedBuffer buffer) throws IOException {
		return new PyBool(true);
	}

	private static PyBase loadTuple(WrappedBuffer buffer) throws IOException {
		return loadTuple(buffer, buffer.readLength());
	}

	private static PyBase loadTuple(WrappedBuffer buffer, int size) throws IOException {
		final PyTuple tuple = new PyTuple();
//		if (root == null) root = tuple;
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

	private static PyBase loadTuple1(WrappedBuffer buffer) throws IOException {
		return loadTuple(buffer, 1);
	}

	private static PyBase loadTuple2(WrappedBuffer buffer) throws IOException {
		return loadTuple(buffer, 2);
	}

	private static PyBase loadUnicode(WrappedBuffer buffer) throws IOException {
		return new PyBuffer(buffer.readBytes(buffer.readLength() * 2));
	}

	private static PyBase loadUnicode0(WrappedBuffer buffer) throws IOException {
		return new PyBuffer(new byte[]{});
	}

	private static PyBase loadUnicode1(WrappedBuffer buffer) throws IOException {
		return new PyBuffer(buffer.readBytes(2));
	}

	private static PyBase loadVarInt(WrappedBuffer buffer) throws IOException {
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
			final byte[] bytes = buffer.readBytes(size);
			final BigInteger bi = new BigInteger(bytes);
			return new PyLong(bi.longValue());
		}
	}

	public PyBase read() throws IOException {
		buffer.readByte(); // throw the first byte away. it's the protocol marker
		buffer.initSharedVector();

		PyBase base = null;

		base = loadPy(buffer);

		return base;
	}

	private static byte[] zerouncompress(byte[] bytes, int size) throws IOException {

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
