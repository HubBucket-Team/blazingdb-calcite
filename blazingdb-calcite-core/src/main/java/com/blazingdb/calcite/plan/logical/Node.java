package com.blazingdb.calcite.plan.logical;

import java.util.Collection;

import java.io.Serializable;

public interface Node extends Serializable {
  public void             addChild(final Node child);
  public Collection<Node> getChildren();
}
