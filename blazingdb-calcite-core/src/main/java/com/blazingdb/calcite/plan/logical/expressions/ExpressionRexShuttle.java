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
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayDeque;
import java.util.Deque;

import java.math.BigDecimal;

public final class ExpressionRexShuttle extends RexShuttle {

  protected final Deque<Expression> expressionStack = new ArrayDeque<>();
  protected final Expression rootExpressionNode     = new ExpressionBase() {
    private static final long serialVersionUID = -2624084985199375515L;
    @Override
    public String toString() {
      return "Root";
    }
  };

  public ExpressionRexShuttle() { expressionStack.push(rootExpressionNode); }

  public RexNode visitCall(RexCall rexCall) {
    if (SqlKind.EQUALS.equals(rexCall.getKind())) {
      EqualsExpression equalsNode = new EqualsExpression();
      expressionStack.peek().addChild(equalsNode);
      expressionStack.push(equalsNode);
    } else if (SqlKind.CAST.equals(rexCall.getKind())) {
      CastExpression castNode = new CastExpression();
      expressionStack.peek().addChild(castNode);
      expressionStack.push(castNode);
    } else {
      System.out.println("UNREACHEABLE");
    }

    for (final RexNode rexNode : rexCall.getOperands()) {
      rexNode.accept(this);
    }
    expressionStack.pop();
    return rexCall;
  }

  public RexNode visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
    return null;
  }

  public RexNode visitDynamicParam(RexDynamicParam rexDynamicParam) {
    return null;
  }

  public RexNode visitFieldAccess(RexFieldAccess rexFieldAccess) {
    return null;
  }

  public RexNode visitInputRef(RexInputRef rexInputRef) {
    expressionStack.peek().addChild(
        new IntegerExpression(rexInputRef.getIndex()));
    return rexInputRef;
  }

  public RexNode visitLiteral(RexLiteral rexLiteral) {
    expressionStack.peek().addChild(
        new IntegerExpression(((BigDecimal) rexLiteral.getValue()).intValue()));
    return rexLiteral;
  }

  public RexNode visitLocalRef(RexLocalRef rexLocalRef) { return null; }

  public RexNode visitOver(RexOver rexOver) { return null; }

  public RexNode visitRangeRef(RexRangeRef rexRangeRef) { return null; }

  public RexNode visitSubQuery(RexSubQuery rexSubQuery) { return null; }

  @Override
  public RexNode visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
    return null;
  }

  @Override
  public RexNode visitTableInputRef(RexTableInputRef rexTableInputRef) {
    return null;
  }

  public Expression getExpressionRootNode() { return rootExpressionNode; }
}
