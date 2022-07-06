package com.hufudb.onedb.data.storage;

public class Point {
    private Double x;
    private Double y;

    public Point(Double x, Double y) {
        this.x = x;
        this.y = y;
    }

    public Double getX() {
        return this.x;
    }

    public Double getY() {
        return this.y;
    }

    @Override
    public String toString() {
        return String.format("POINT(%f %f)", getX(), getY());
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || (obj instanceof Point && ((Point) obj).getX() == x && ((Point) obj).getY() == y);
    }
}
