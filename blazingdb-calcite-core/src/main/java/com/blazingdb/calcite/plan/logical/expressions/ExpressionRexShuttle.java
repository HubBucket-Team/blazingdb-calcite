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
    Expression expression = null;

    if (SqlKind.EQUALS.equals(rexCall.getKind())) {
      expression = new EqualsExpression();
    } else if (SqlKind.CAST.equals(rexCall.getKind())) {
      expression = new CastExpression();
    } else {
      // TODO(gcca): insert bad node to check tree validity
    }

    if (null == expression) {
      // TODO(gcca): idem and merge code
    }

    expressionStack.peek().addInput(expression);
    expressionStack.push(expression);
    for (final RexNode rexNode : rexCall.getOperands()) {
      rexNode.accept(this);
    }
    expressionStack.pop();
    return rexCall;
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
        new IntegerExpression(rexInputRef.getIndex()));
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

  public Expression getExpressionRootNode() { return rootExpressionNode; }
}
