package com.blazingdb.calcite.plan.logical;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

final class PlanRelShuttle implements RelShuttle {

  protected final Deque<RelNode> relNodeStack = new ArrayDeque<>();
  protected final Deque<Node> nodeStack       = new ArrayDeque<>();
  private final Node rootNode                 = new NodeBase() {
    private static final long serialVersionUID = 2100115430813863826L;
    @Override
    public String toString() {
      return "Root";
    }
  };

  public PlanRelShuttle() { nodeStack.push(rootNode); }

  public RelNode visit(LogicalAggregate logicalAggregate) {
    AggregateNode aggregateNode =
        new AggregateNode(logicalAggregate.getGroupSet().asList());
    processCurrentNodeWith(aggregateNode);
    return applyToChild(logicalAggregate, 0, logicalAggregate.getInput());
  }

  public RelNode visit(LogicalMatch logicalMatch) {
    return applyToChild(logicalMatch, 0, logicalMatch.getInput());
  }

  public RelNode visit(TableScan tableScan) {
    TableScanNode tableScanNode =
        new TableScanNode(tableScan.getTable().getQualifiedName());
    processCurrentNodeWith(tableScanNode);
    return tableScan;
  }

  public RelNode visit(TableFunctionScan tableFunctionScan) {
    return traverseChildrenOf(tableFunctionScan);
  }

  public RelNode visit(LogicalValues logicalValues) { return logicalValues; }

  public RelNode visit(LogicalFilter logicalFilter) {
    FilterNode filterNode = new FilterNodeBuilder(logicalFilter).build();
    processCurrentNodeWith(filterNode);
    return applyToChild(logicalFilter, 0, logicalFilter.getInput());
  }

  public RelNode visit(LogicalProject logicalProject) {
    try {
      processCurrentNodeWith(new ProjectNodeBuilder(logicalProject).build());
    } catch (ProjectNodeBuilder.NoRexInputRefInLogicalFilterException e) {
      // TODO(gcca): insert bad node to check tree validity
    }
    return applyToChild(logicalProject, 0, logicalProject.getInput());
  }

  public RelNode visit(LogicalJoin logicalJoin) {
    return traverseChildrenOf(logicalJoin);
  }

  public RelNode visit(LogicalCorrelate logicalCorrelate) {
    return traverseChildrenOf(logicalCorrelate);
  }

  public RelNode visit(LogicalUnion logicalUnion) {
    UnionNode unionNode = new UnionNode(logicalUnion.all);
    processCurrentNodeWith(unionNode);
    return traverseChildrenOf(logicalUnion);
  }

  public RelNode visit(LogicalIntersect logicalIntersect) {
    return traverseChildrenOf(logicalIntersect);
  }

  public RelNode visit(LogicalMinus logicalMinus) {
    return traverseChildrenOf(logicalMinus);
  }

  public RelNode visit(LogicalSort logicalSort) {
    return traverseChildrenOf(logicalSort);
  }

  public RelNode visit(LogicalExchange logicalExchange) {
    return traverseChildrenOf(logicalExchange);
  }

  public RelNode visit(RelNode other) { return traverseChildrenOf(other); }

  public Node getRootNode() { return rootNode; }

  private void processCurrentNodeWith(final Node node) {
    nodeStack.peek().addChild(node);
    nodeStack.push(node);
  }

  protected RelNode applyToChild(final RelNode parentRelNode, final int i,
                                 final RelNode childRelNode) {
    relNodeStack.push(parentRelNode);
    try {
      RelNode otherChildRelNode = childRelNode.accept(this);
      nodeStack.pop();
      if (otherChildRelNode != childRelNode) {
        final List<RelNode> inputs = new ArrayList<>(parentRelNode.getInputs());
        inputs.set(i, otherChildRelNode);
        return parentRelNode.copy(parentRelNode.getTraitSet(), inputs);
      }
      return parentRelNode;
    } finally { relNodeStack.pop(); }
  }

  protected RelNode traverseChildrenOf(RelNode relNode) {
    for (final Ord<RelNode> input : Ord.zip(relNode.getInputs())) {
      relNode = applyToChild(relNode, input.i, input.e);
    }
    return relNode;
  }
}
