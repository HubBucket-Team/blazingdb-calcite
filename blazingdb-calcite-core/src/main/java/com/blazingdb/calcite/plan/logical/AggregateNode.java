package com.blazingdb.calcite.plan.logical;

import java.util.List;

public class AggregateNode extends NodeBase {

  private static final long serialVersionUID = -4193184474436884856L;

  private final List<Integer> groups;

  public AggregateNode(final List<Integer> groups) { this.groups = groups; }

  public String toString() { return "AggregateNode : groups " + groups.get(0); }
}
