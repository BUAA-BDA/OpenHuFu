package group.bda.federate.driver.utils;

public class GeometryRow<T> {
  private T geometry;
  private String value;

  public GeometryRow(T geometry, String value) {
    this.geometry = geometry;
    this.value = value;
  }

  public T getGemotry() {
    return geometry;
  }

  public String getValue() {
    return value;
  }
}
