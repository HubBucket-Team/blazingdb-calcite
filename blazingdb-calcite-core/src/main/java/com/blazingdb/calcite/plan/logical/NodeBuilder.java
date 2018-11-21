package com.blazingdb.calcite.plan.logical;

import java.io.Serializable;

public interface NodeBuilder extends Serializable {
  Node build();
}
