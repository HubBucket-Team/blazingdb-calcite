package com.blazingdb.calcite.plan.logical;

public class FilterNode extends NodeBase {

  private static final long serialVersionUID = -3015864347938693253L;

  private final String label;

  public FilterNode(final String label) { this.label = label; }

  public String toString() { return "FilterNode : " + label; }
}
