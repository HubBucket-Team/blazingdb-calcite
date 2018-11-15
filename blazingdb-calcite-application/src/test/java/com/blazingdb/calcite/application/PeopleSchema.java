package com.blazingdb.calcite.application;

public final class PeopleSchema {

  public final Person[] people = {new Person("Ironman", 12),
                                  new Person("Batman", 10)};

  public static class Person {
    public final String name;
    public final int age;

    public Person(final String name, final int age) {
      this.name = name;
      this.age = age;
    }
  }
}
