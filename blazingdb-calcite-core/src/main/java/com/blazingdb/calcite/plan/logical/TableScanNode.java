package com.blazingdb.calcite.plan.logical;

import java.util.List;

final class TableScanNode extends NodeBase {

  private static final long serialVersionUID = -3038840524700102053L;

  private final List<String> qualifiedName;

  public TableScanNode(final List<String> qualifiedName) {
    this.qualifiedName = qualifiedName;
  }

  public List<String> getQualifiedName() { return qualifiedName; }

  public String getTableName() {
    final List<String> tableIdentifier = getQualifiedName();
    return tableIdentifier.get(tableIdentifier.size() - 1);
  }

  @Override
  public String toString() {
    return "TableScan : path = " + String.join(".", getQualifiedName());
  }
}
