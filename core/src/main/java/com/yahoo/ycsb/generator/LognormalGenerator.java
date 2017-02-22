package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

public class LognormalGenerator extends IntegerGenerator{

  public static final String LOGNORMAL_MEAN="4";
  public static final String LOGNORMAL_SD="0.27";

  /**
   * The lognormal constant to use.
   */
  double _mu;
  double _sigma;

  /******************************* Constructors **************************************/

  /**
   * Create an lognormal generator with scale and shape parameters
   */
  public LognormalGenerator(double scale, double shape)
  {
    _mu = scale;
    _sigma = shape;
  }

  /****************************************************************************************/

  /**
   * Generate the next item.
   */
  @Override
  public int nextInt()
  {
    return (int)nextLong();
  }

  /**
   * Generate the next item as a long.
   */
  public long nextLong()
  {
    return (long) Math.exp(_mu + _sigma * Utils.random().nextGaussian());
  }

  @Override
  public double mean() {
    return Math.exp(_mu + (_sigma * _sigma / 2));
  }

  public static void main(String args[]) {
    LognormalGenerator l = new LognormalGenerator(4, 0.27);
    double total = 0;
    for(int i = 0; i < 1000000; i++) {
      total += l.nextInt();
    }
    System.out.println("Got this mean from a total of 1000000 entries: " + Integer.toString((int)(total/1000000))
      + " expect: " + Integer.toString((int)l.mean()));
  }

}