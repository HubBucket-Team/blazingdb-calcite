package com.blazingdb.calcite.plan.logical;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

final class ProjectNode
    extends NodeBase implements Iterable<Pair<String, Integer>> {

  private static final long serialVersionUID = -4767631526304421234L;

  // index relation
  private final List<String> columnNames;
  private final List<Integer> columnValues;

  public ProjectNode(final List<String> columnNames,
                     final List<Integer> columnValues) {
    this.columnNames  = columnNames;
    this.columnValues = columnValues;
  }

  public Pair<String, Integer> get(final int index) {
    return Pair.of(columnNames.get(index), columnValues.get(index));
  }

  @Override
  public Iterator<Pair<String, Integer>> iterator() {
    // TODO(gcca): Don't build List. Go directly to make the iterator.
    List<Pair<String, Integer>> list = new ArrayList<Pair<String, Integer>>();
    for (int i = 0; i < columnNames.size(); i++) { list.add(get(i)); }
    return list.iterator();
  }

  @Override
  public String toString() {
    return "ProjectNode : " + IntStream.range(0, columnNames.size())
                                  .mapToObj(i
                                            -> new StringBuilder()
                                                   .append(columnNames.get(i))
                                                   .append('=')
                                                   .append(columnValues.get(i)))
                                  .collect(Collectors.joining(", "));
  }
}
