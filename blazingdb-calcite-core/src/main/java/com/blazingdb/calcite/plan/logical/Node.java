package com.blazingdb.calcite.plan.logical;

import java.util.Collection;

import java.io.Serializable;

public interface Node extends Serializable {
  Node             addChild(final Node child);
  Collection<Node> getChildren();
}
