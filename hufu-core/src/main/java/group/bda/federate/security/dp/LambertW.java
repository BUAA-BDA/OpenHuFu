package group.bda.federate.security.dp;

public class LambertW {
  public static double lambertW1(double x) {
    if (x >= -0.00000000000001) return Double.NaN;
    if (x < -1.0) return Double.NaN;
    if (Math.abs(x + 1.0) <= 0.00000000000001) return -1;

    double M1 = 0.3361;
    double M2 = -0.0042;
    double M3 = -0.0201;
    double s = -1 - Math.log(-x);
    return -1.0 - s - (2.0/M1) * ( 1.0 - 1.0 / ( 1.0 + ( (M1 * Math.sqrt(s/2.0)) / (1.0 + M2 * s * Math.exp(M3 * Math.sqrt(s)) ) ) ) );
  }
}
