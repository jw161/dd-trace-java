package com.datadog.profiling.jfr;

import com.datadog.profiling.util.NonZeroHashCode;
import java.util.Collections;
import java.util.List;
import lombok.Generated;

/** A built-in type. Corresponds to a Java primitive type or {@link java.lang.String String} */
final class BuiltinType extends BaseType {
  private final Types.Builtin builtin;

  BuiltinType(long id, Types.Builtin type, ConstantPools constantPools, Metadata metadata) {
    super(id, type.getTypeName(), constantPools, metadata);
    this.builtin = type;
  }

  @Override
  public boolean isBuiltin() {
    return true;
  }

  @Override
  public List<TypedField> getFields() {
    return Collections.emptyList();
  }

  @Override
  public TypedField getField(String name) {
    throw new IllegalArgumentException();
  }

  @Override
  public List<Annotation> getAnnotations() {
    return Collections.emptyList();
  }

  @Override
  public boolean canAccept(Object value) {
    if (value == null) {
      // non-initialized built-ins will get the default value and String will be properly handled
      return true;
    }

    if (value instanceof TypedValue) {
      return this == ((TypedValue) value).getType();
    }
    switch (builtin) {
      case STRING:
        {
          return (value instanceof String);
        }
      case BYTE:
        {
          return value instanceof Byte;
        }
      case CHAR:
        {
          return value instanceof Character;
        }
      case SHORT:
        {
          return value instanceof Short;
        }
      case INT:
        {
          return value instanceof Integer;
        }
      case LONG:
        {
          return value instanceof Long;
        }
      case FLOAT:
        {
          return value instanceof Float;
        }
      case DOUBLE:
        {
          return value instanceof Double;
        }
      case BOOLEAN:
        {
          return value instanceof Boolean;
        }
    }
    return false;
  }

  // use Lombok @Generated to skip jacoco coverage verification
  @Generated
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    BuiltinType that = (BuiltinType) o;
    return builtin == that.builtin;
  }

  // use Lombok @Generated to skip jacoco coverage verification
  @Generated
  @Override
  public int hashCode() {
    return NonZeroHashCode.hash(super.hashCode(), builtin);
  }
}
