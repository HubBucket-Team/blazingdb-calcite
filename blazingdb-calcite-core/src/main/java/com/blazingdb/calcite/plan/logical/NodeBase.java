package com.blazingdb.calcite.plan.logical;

import java.util.ArrayList;
import java.util.Collection;

public abstract class NodeBase implements Node {

  private static final long serialVersionUID = -1272602310207264169L;

  private Collection<Node> children;

  public NodeBase() { children = new ArrayList<Node>(); }

  public void addChild(final Node child) { children.add(child); }

  public Collection<Node> getChildren() { return children; }

  public abstract String toString();
}
