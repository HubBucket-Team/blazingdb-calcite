package com.blazingdb.calcite.plan.logical;

import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;

final class ProjectNodeBuilder implements NodeBuilder {

  private static final long serialVersionUID = -6984359340171360303L;

  private final LogicalProject logicalProject;

  public ProjectNodeBuilder(final LogicalProject logicalProject) {
    this.logicalProject = logicalProject;
  }

  @Override
  public ProjectNode build() throws NoRexInputRefInLogicalFilterException {
    final ArrayList<String> columnNames   = new ArrayList<String>();
    final ArrayList<Integer> columnValues = new ArrayList<Integer>();

    for (Pair<RexNode, String> pair : logicalProject.getNamedProjects()) {
      final RexNode rexNode = pair.getKey();

      if (!rexNode.isA(SqlKind.INPUT_REF)) {
        throw new NoRexInputRefInLogicalFilterException("Bad rex node kind: " +
                                                        rexNode.getKind());
      }

      columnNames.add(pair.getValue());
      columnValues.add(((RexInputRef) rexNode).getIndex());
    }

    ProjectNode logicalNode = new ProjectNode(columnNames, columnValues);

    return logicalNode;
  }

  final static class NoRexInputRefInLogicalFilterException
      extends NodeBuilder.BuildingException {
    private static final long serialVersionUID = 9067377235514275389L;
    public NoRexInputRefInLogicalFilterException(final String message) {
      super(message);
    }
  }
}
