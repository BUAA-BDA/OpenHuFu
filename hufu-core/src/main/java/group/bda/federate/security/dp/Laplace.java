package group.bda.federate.security.dp;

import org.apache.commons.math3.distribution.LaplaceDistribution;

public class Laplace {
  private final LaplaceDistribution ld;
  private final double sd;

  public Laplace(final double epsilon, final double sd) {
    this.ld = new LaplaceDistribution(0, 1 / epsilon);
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
}
