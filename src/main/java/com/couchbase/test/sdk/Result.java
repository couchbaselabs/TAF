package com.couchbase.test.sdk;

import reactor.util.annotation.Nullable;

public class Result {
    private final String id;
    private final Object document;
    private final @Nullable Throwable err;
    private final boolean status;

    public Result(String id, Object document, Throwable err, boolean status) {
      this.id = id;
      this.document = document;
      this.err = err;
      this.status = status;
    }

    public String id() {
      return id;
    }

    public Object document() {
      return document;
    }

    public Throwable err() {
      return err;
    }

    public boolean status() {
      return status;
    }
  }