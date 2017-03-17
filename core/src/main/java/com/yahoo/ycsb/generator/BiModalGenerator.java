/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.generator;

import java.util.Random;

import com.yahoo.ycsb.Utils;

/**
 * Generates integers based on a bimodal model
 */
public class BiModalGenerator extends IntegerGenerator
{
  int _lb,_ub;
  float _prob;

  /**
   * Creates a generator that will return a small value based on a fixed probability and otherwise generates a larger value
   *
   * @param lb the low value
   * @param ub the high value
   * @param prob is the probability of generating a lb value
   */
  public BiModalGenerator(int lb, int ub, float prob)
  {
    _lb=lb;
    _ub=ub;
    _prob=prob;
  }

  @Override
  public int nextInt()
  {
    float ret=Utils.random().nextFloat();
    if(ret <= _prob)
      return _lb;
    else
      return _ub;
  }

  @Override
  public double mean() {
    return ((double)(_lb*_prob+_ub*(1-_prob)));
  }
}
