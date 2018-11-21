package com.blazingdb.calcite.plan.logical;

import java.io.Serializable;

public interface NodeBuilder extends Serializable {
  Node build() throws BuildingException;

  public static class BuildingException extends Exception {
    private static final long serialVersionUID = 8873783466414113910L;
    public BuildingException(final String message) { super(message); }
  }
}
