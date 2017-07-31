package com.thibaultleouay.asyncbigqueryclient;

public class BigQueryException extends Exception {

	public BigQueryException(final String message) {
		super(message);
	}
	
	public BigQueryException(final String message, final Throwable cause) {
		super(message,cause);
	}
}
