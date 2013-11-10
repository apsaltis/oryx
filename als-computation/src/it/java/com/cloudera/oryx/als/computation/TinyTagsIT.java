/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.als.computation;

import java.io.File;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.common.IDValue;
import com.cloudera.oryx.als.serving.ServerRecommender;

/**
 * Runs a tiny test including tags to check its results. The results are checked against the same test where
 * real items/users are used instead of tags. The results are slightly different because tags translate to 
 * IDs that fall in a different order, resulting in a slightly different initial random starting vector.
 * 
 * @author Sean Owen
 */
public final class TinyTagsIT extends AbstractComputationIT {

  private static final Logger log = LoggerFactory.getLogger(TinyTagsIT.class);
  private static final double BIG_EPSILON = 0.02;

  @Override
  protected File getTestDataPath() {
    return getResourceAsFile("tiny-tags");
  }

  @Test
  public void testTinyRecommend() throws Exception {
    ServerRecommender client = getRecommender();
    List<IDValue> recs = client.recommend("5", 2);
    log.info("{}", recs);
    assertEquals("4", recs.get(0).getID());
    assertEquals("2", recs.get(1).getID());
    assertEquals(0.69684064f, recs.get(0).getValue(), BIG_EPSILON);
    assertEquals(0.62091565f, recs.get(1).getValue(), BIG_EPSILON);
  }
  
  @Test
  public void testTinySimilar() throws Exception {
    ServerRecommender client = getRecommender();
    List<IDValue> similar = client.mostSimilarItems("2", 1);
    log.info("{}", similar);
    assertEquals("foo", similar.get(0).getID());
    assertEquals(0.8351423f, similar.get(0).getValue());
  }
  
  @Test
  public void testTinyMostPopular() throws Exception {
    ServerRecommender client = getRecommender();
    List<IDValue> popular = client.mostPopularItems(5);
    log.info("{}", popular);
    assertEquals("foo", popular.get(0).getID());
    assertEquals(3.0f, popular.get(0).getValue(), BIG_EPSILON);
  }
  
  @Test
  public void testTinyAnonymous() throws Exception {
    ServerRecommender client = getRecommender();
    List<IDValue> recs = client.recommendToAnonymous(new String[] {"2", "3"}, 1);
    log.info("{}", recs);
    assertEquals("bar", recs.get(0).getID());
    assertEquals(0.18420106f, recs.get(0).getValue(), BIG_EPSILON);
  }

}
