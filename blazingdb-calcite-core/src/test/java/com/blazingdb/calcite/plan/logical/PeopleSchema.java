package com.blazingdb.calcite.plan.logical;

public final class PeopleSchema {
  public final Person[] HEROES = {new Person("Ironman", 12),
                                  new Person("Batman", 10)};
  public static class Person {
    public final String NAME;
    public final Integer AGE;
    public Person(final String NAME, final Integer AGE) {
      this.NAME = NAME;
      this.AGE  = AGE;
    }
  }
}
