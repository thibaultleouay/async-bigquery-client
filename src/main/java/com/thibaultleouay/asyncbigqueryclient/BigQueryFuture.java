package com.thibaultleouay.asyncbigqueryclient;

import java.util.concurrent.CompletableFuture;

public class BigQueryFuture<T> extends CompletableFuture<T> {

	public boolean succeed(final T value) {
		return super.complete(value);
	}

	public boolean fail(final Throwable ex) {
		return super.completeExceptionally(ex);
	}
}
