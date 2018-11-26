package com.blazingdb.calcite.plan.logical.expressions;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;

import java.util.ArrayDeque;
import java.util.Deque;

import java.math.BigDecimal;

public final class ExpressionRexShuttle extends RexShuttle {

  protected final Deque<RexNode> rexNodeStack       = new ArrayDeque<>();
  protected final Deque<Expression> expressionStack = new ArrayDeque<>();
  protected final Expression rootExpression         = new ExpressionBase() {
    private static final long serialVersionUID = -2624084985199375515L;
    @Override
    public String toString() {
      return "Root";
    }
  };

  public ExpressionRexShuttle() { expressionStack.push(rootExpression); }

  public RexNode visitCall(RexCall rexCall) {
    try {
      final Expression expression =
          new FunctionExpressionBuilder(rexCall).build().get();
      expressionStack.peek().addInput(expression);
      expressionStack.push(expression);
      traverseOperandsOf(rexCall);
      return rexCall;
    } catch (ExpressionBuilder.ExpressionBuildingException e) { return null; }
  }

  public RexNode visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
    return rexCorrelVariable;
  }

  public RexNode visitDynamicParam(RexDynamicParam rexDynamicParam) {
    return rexDynamicParam;
  }

  public RexNode visitFieldAccess(RexFieldAccess rexFieldAccess) {
    return rexFieldAccess;
  }

  public RexNode visitInputRef(RexInputRef rexInputRef) {
    expressionStack.peek().addInput(
        new ReferenceExpression(rexInputRef.getIndex()));
    return rexInputRef;
  }

  public RexNode visitLiteral(RexLiteral rexLiteral) {
    expressionStack.peek().addInput(
        new IntegerExpression(((BigDecimal) rexLiteral.getValue()).intValue()));
    return rexLiteral;
  }

  public RexNode visitLocalRef(RexLocalRef rexLocalRef) { return rexLocalRef; }

  public RexNode visitOver(RexOver rexOver) { return rexOver; }

  public RexNode visitRangeRef(RexRangeRef rexRangeRef) { return rexRangeRef; }

  public RexNode visitSubQuery(RexSubQuery rexSubQuery) { return rexSubQuery; }

  @Override
  public RexNode visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
    return rexPatternFieldRef;
  }

  @Override
  public RexNode visitTableInputRef(RexTableInputRef rexTableInputRef) {
    return rexTableInputRef;
  }

  public Expression getRootExpression() { return rootExpression; }

  protected RexNode applyToInput(final RexNode rexNode) {
    rexNodeStack.push(rexNode);
    try {
      RexNode otherChildRexNode = rexNode.accept(this);
      if (!otherChildRexNode.equals(rexNode)) { return otherChildRexNode; }
      return rexNode;
    } finally { rexNodeStack.pop(); }
  }

  protected RexNode traverseOperandsOf(RexNode rexNode) {
    for (final RexNode operand : ((RexCall) rexNode).getOperands()) {
      rexNode = applyToInput(operand);
    }
    expressionStack.pop();
    return rexNode;
  }
}
