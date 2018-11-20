package com.blazingdb.calcite.plan.logical;

public class ProjectNode extends NodeBase {

  private static final long serialVersionUID = -4767631526304421234L;

  private final String columns;

  public ProjectNode(final String columns) { this.columns = columns; }

  public String toString() { return "ProjectNode : " + columns; }
}
