package com.blazingdb.calcite.plan.logical;

import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;

final class ProjectNodeBuilder implements NodeBuilder {

  private static final long serialVersionUID = -6984359340171360303L;

  private final LogicalProject logicalProject;

  public ProjectNodeBuilder(final LogicalProject logicalProject) {
    this.logicalProject = logicalProject;
  }

  @Override
  public Node build() {
    final ArrayList<String> columnNames   = new ArrayList<String>();
    final ArrayList<Integer> columnValues = new ArrayList<Integer>();

    for (Pair<RexNode, String> pair : logicalProject.getNamedProjects()) {
      columnNames.add(pair.getValue());
      columnValues.add(((RexInputRef)pair.getKey()).getIndex());
    }

    ProjectNode logicalNode = new ProjectNode(columnNames, columnValues);

    return logicalNode;
  }
}
