package com.ververica.field.dynamicrules.converters;

import java.io.Serializable;

public abstract class StringConverter<T> implements Serializable {
  public abstract T toValue(String input);

  public abstract String toString(T input);
}
