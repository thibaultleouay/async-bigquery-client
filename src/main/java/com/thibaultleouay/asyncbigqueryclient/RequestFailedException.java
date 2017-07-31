package com.thibaultleouay.asyncbigqueryclient;

public class RequestFailedException extends BigQueryException {

	private int statusCode;
	private String statusMessage;

	public RequestFailedException(final int statusCode, final String statusMessage) {
		super(statusCode + " " + statusMessage);
		this.statusCode = statusCode;
		this.statusMessage = statusMessage;
	}

	public int statusCode() {
		return statusCode;
	}

	public String statusMessage() {
		return statusMessage;
	}

}
