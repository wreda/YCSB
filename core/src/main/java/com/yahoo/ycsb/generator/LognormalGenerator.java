package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
    LognormalGenerator l = new LognormalGenerator(2.5, 1.25);
    double total = 0;
    List<Integer> values = new ArrayList<Integer>();
    for(int i = 0; i < 10000000; i++) {
      int value = l.nextInt();
      total += value;
      values.add(value);
    }

    System.out.println("Got this mean from a total of 1000000 entries: " + Integer.toString((int)(total/10000000))
      + " expect: " + Integer.toString((int)l.mean()));

    Collections.sort(values);
    System.out.printf("The median, 95%% and 99%%tile times are %,d / %,d / %,d %n",
      values.get(values.size()/2), values.get(values.size()*95/100), values.get(values.size()*99/100));
  }
}