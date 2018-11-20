package com.blazingdb.calcite.plan.logical;

import java.util.List;

public class TableScanNode extends NodeBase {

  private static final long serialVersionUID = -3038840524700102053L;

  private final List<String> tablePath;

  public TableScanNode(final List<String> tablePath) {
    this.tablePath = tablePath;
  }

  public List<String> getTablePath() { return tablePath; }

  public String getTableName() {
    final List<String> tableIdentifier = getTablePath();
    return tableIdentifier.get(tableIdentifier.size() - 1);
  }

  public String toString() {
    return "TableScanNode : path = " + String.join(".", getTablePath());
  }
}
