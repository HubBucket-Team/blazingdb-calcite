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
import org.apache.calcite.rex.RexNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

final class PlanRelShuttle implements RelShuttle {

  protected final Deque<RelNode> stack  = new ArrayDeque<>();
  protected final Deque<Node> nodeStack = new ArrayDeque<>();
  private Node                rootNode  = null;

  final static class RootNode extends NodeBase {
    private static final long serialVersionUID = 2100115430813863826L;
    public String             toString() { return "RootNode"; }
  };

  public PlanRelShuttle() {
    rootNode = new RootNode();
    nodeStack.push(rootNode);
  }

  public RelNode visit(LogicalAggregate aggregate) {
    AggregateNode aggregateNode =
        new AggregateNode(aggregate.getGroupSet().asList());
    processCurrentNodeWith(aggregateNode);
    return applyToChild(aggregate, 0, aggregate.getInput());
  }

  public RelNode visit(LogicalMatch match) {
    return applyToChild(match, 0, match.getInput());
  }

  public RelNode visit(TableScan scan) {
    TableScanNode tableScanNode =
        new TableScanNode(scan.getTable().getQualifiedName());
    processCurrentNodeWith(tableScanNode);
    return scan;
  }

  public RelNode visit(TableFunctionScan scan) {
    return traverseChildrenOf(scan);
  }

  public RelNode visit(LogicalValues values) { return values; }

  public RelNode visit(LogicalFilter filter) {
    FilterNode filterNode = new FilterNode(filter.getCondition().toString());
    processCurrentNodeWith(filterNode);
    return applyToChild(filter, 0, filter.getInput());
  }

  public RelNode visit(LogicalProject project) {
    StringBuilder b = new StringBuilder();
    b.append("name = ");
    for (RexNode r : project.getChildExps()) {
      b.append(r.toString());
      b.append("  ");
    }
    ProjectNode projectNode =
        new ProjectNode(project.getChildExps().get(0).toString());
    processCurrentNodeWith(projectNode);
    return applyToChild(project, 0, project.getInput());
  }

  public RelNode visit(LogicalJoin join) { return traverseChildrenOf(join); }

  public RelNode visit(LogicalCorrelate correlate) {
    return traverseChildrenOf(correlate);
  }

  public RelNode visit(LogicalUnion union) {
    UnionNode unionNode = new UnionNode(union.all);
    processCurrentNodeWith(unionNode);
    return traverseChildrenOf(union);
  }

  public RelNode visit(LogicalIntersect intersect) {
    return traverseChildrenOf(intersect);
  }

  public RelNode visit(LogicalMinus minus) { return traverseChildrenOf(minus); }

  public RelNode visit(LogicalSort sort) { return traverseChildrenOf(sort); }

  public RelNode visit(LogicalExchange exchange) {
    return traverseChildrenOf(exchange);
  }

  public RelNode visit(RelNode other) { return traverseChildrenOf(other); }

  public Node getRootNode() { return rootNode; }

  private void processCurrentNodeWith(final Node node) {
    nodeStack.peek().addChild(node);
    nodeStack.push(node);
  }

  protected RelNode
  applyToChild(RelNode parentRelNode, int i, RelNode childRelNode) {
    stack.push(parentRelNode);
    try {
      RelNode otherChildRelNode = childRelNode.accept(this);
      nodeStack.pop();
      if (otherChildRelNode != childRelNode) {
        final List<RelNode> inputs = new ArrayList<>(parentRelNode.getInputs());
        inputs.set(i, otherChildRelNode);
        return parentRelNode.copy(parentRelNode.getTraitSet(), inputs);
      }
      return parentRelNode;
    } finally { stack.pop(); }
  }

  protected RelNode traverseChildrenOf(RelNode relNode) {
    for (Ord<RelNode> input : Ord.zip(relNode.getInputs())) {
      relNode = applyToChild(relNode, input.i, input.e);
    }
    return relNode;
  }
}
