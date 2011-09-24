package com.github.evetools.marshal;

/**
 *
 * @author Andrew
 */
class NoSuchProviderException extends Exception {
	private static final long serialVersionUID = 1L;
	NoSuchProviderException(String message, Throwable cause) {
		super(message, cause);
	}
	NoSuchProviderException(String message) {
		super(message);
	}
}
