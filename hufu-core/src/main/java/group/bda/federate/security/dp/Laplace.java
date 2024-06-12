package group.bda.federate.security.dp;

import group.bda.federate.config.FedSpatialConfig;
import java.util.Random;
import org.apache.commons.math3.distribution.LaplaceDistribution;

public class Laplace {
  private final LaplaceDistribution ld;
  private final double sd;

  public Laplace(final double delta, final double epsilon, final double sd) {
    this.ld = new LaplaceDistribution(0, delta / epsilon);
    this.sd = sd;
  }

  public double sample() {
    double sample = this.ld.sample();
    while (Math.abs(sample) >= this.sd) {
      sample = this.ld.sample();
    }
    return sample;
  }

  public double getSD() {
    return sd;
  }

  private static Random random = new Random();

  public static double geoISampleRad(double epsilon, double prob) {
    double w0 = (prob - 1.0) / Math.exp(1.0);
    double w1 = LambertW.lambertW1(w0);
    return -1.0 * (w1 + 1.0) / epsilon;
  }

  public static double[] boundedPlanarLaplaceMechanism(double x, double y, double epsilon, double delta) {
    final double stepDelta = 1e-3;
    double theta = 2 * Math.PI * random.nextDouble();
    double prob = random.nextDouble();
    double bigDelta = 0;

    while (true) {
      bigDelta += stepDelta;
      double right = delta * Math.PI * Math.pow(geoISampleRad(epsilon, 1 - bigDelta), 2);
      if (bigDelta >= right)
        break;
    }

    double r;
    if (prob > 1 - bigDelta) {
      double rSquare = Math.pow(geoISampleRad(epsilon, 1 - bigDelta), 2);
      r = Math.sqrt(rSquare * random.nextDouble());
    } else {
      r = geoISampleRad(epsilon, prob);
    }

    double dx = r * Math.cos(theta);
    double dy = r * Math.sin(theta);

    return new double[]{x + dx, y + dy};
  }

  public static void main(String[] args) {
    double longitude = 121;
    double latitude = 40;
    double[] result = Laplace.boundedPlanarLaplaceMechanism(longitude, latitude,
        FedSpatialConfig.Planar_EPS_DP, FedSpatialConfig.Planar_DELTA_DP);
    System.out.println("(" + result[0] + "," + result[1] + ")");
  }
}
