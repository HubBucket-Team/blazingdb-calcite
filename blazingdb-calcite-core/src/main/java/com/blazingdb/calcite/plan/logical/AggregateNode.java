package com.blazingdb.calcite.plan.logical;

import java.util.List;
import java.util.stream.Collectors;

final class AggregateNode extends NodeBase {

  private static final long serialVersionUID = -4193184474436884856L;

  private final List<Integer> groups;

  public AggregateNode(final List<Integer> groups) { this.groups = groups; }

  @Override
  public String toString() {
    return "Aggregate : groups = " +
        groups.stream().map(Object::toString).collect(Collectors.joining(", "));
  }
}
