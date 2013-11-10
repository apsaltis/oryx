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
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.common.IDValue;
import com.cloudera.oryx.als.common.NoSuchItemException;
import com.cloudera.oryx.als.common.NoSuchUserException;
import com.cloudera.oryx.als.serving.ServerRecommender;

/**
 * Tests the ALS implementation using the simple
 * <a href="http://grouplens.org/datasets/movielens/">GroupLens</a> 100K data set.
 *
 * @author Sean Owen
 */
public final class SimpleIT extends AbstractComputationIT {

  private static final Logger log = LoggerFactory.getLogger(SimpleIT.class);

  @Override
  protected File getTestDataPath() {
    return getResourceAsFile("grouplens100K");
  }

  @Test
  public void testIngest() throws Exception {
    Reader reader = new StringReader("0,1\n0,2,3.0\n");

    ServerRecommender client = getRecommender();
    client.ingest(reader);

    List<IDValue> recs = client.recommend("0", 3);
    log.info("{}", recs);
    assertEquals("117", recs.get(0).getID());
  }

  @Test
  public void testRecommend() throws Exception {

    ServerRecommender client = getRecommender();
    List<IDValue> recs = client.recommend("1", 3);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("475", recs.get(0).getID());
    assertEquals("582", recs.get(1).getID());
    assertEquals("403", recs.get(2).getID());

    try {
      client.recommend("0", 3);
      fail();
    } catch (NoSuchUserException nsue) {
      // good
    }

    recs = client.recommend("1", 3, true, null);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("179", recs.get(0).getID());
    assertEquals("475", recs.get(1).getID());
    assertEquals("135", recs.get(2).getID());
  }

  @Test
  public void testRecommendToMany() throws Exception {

    ServerRecommender client = getRecommender();
    // Adding non-existent item to make sure it is ignored
    List<IDValue> recs =
        client.recommendToMany(new String[] {"1", "3", "ABC"}, 3, false, null);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("286", recs.get(0).getID());
    assertEquals("288", recs.get(1).getID());
    assertEquals("302", recs.get(2).getID());
  }
  
  @Test
  public void testRecommendToMany2() throws Exception {
    ServerRecommender client = getRecommender();
    List<IDValue> recs =
        client.recommendToMany(new String[] {"3", "4", "5", "6", "7"}, 3, true, null);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("50", recs.get(0).getID());
    assertEquals("258", recs.get(1).getID());
    assertEquals("288", recs.get(2).getID());
  }
  
  @Test
  public void testRecommendVersusToMany() throws Exception {
    ServerRecommender client = getRecommender();
    List<IDValue> recs = client.recommend("1", 3);
    List<IDValue> recs2 =
        client.recommendToMany(new String[] {"1"}, 3, false, null);
    assertEquals(recs, recs2);
  }
  
  @Test
  public void testRecommendVersusToMany2() throws Exception {
    ServerRecommender client = getRecommender();
    List<IDValue> recs = client.recommendToMany(new String[] {"4", "2"}, 3, true, null);
    List<IDValue> recs2 = client.recommendToMany(new String[] {"2", "4"}, 3, true, null);
    assertEquals(recs, recs2);
  }

  @Test(expected = NoSuchUserException.class)
  public void testRecommendToManyNonexistent1() throws Exception {
    getRecommender().recommendToMany(new String[] {"ABC"}, 3, false, null);
  }

  @Test(expected = NoSuchUserException.class)
  public void testRecommendToManyNonexistent2() throws Exception {
    getRecommender().recommendToMany(new String[] {"ABC", "DEF"}, 3, false, null);
  }

  @Test
  public void testRecommendToAnonymous() throws Exception {

    ServerRecommender client = getRecommender();
    // Adding non-existent item to make sure it is ignored
    List<IDValue> recs = client.recommendToAnonymous(new String[] {"1", "3", "ABC"}, 3);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("151", recs.get(0).getID());
    assertEquals("50", recs.get(1).getID());
    assertEquals("181", recs.get(2).getID());
  }

  @Test(expected = NoSuchItemException.class)
  public void testRecommendToAnonymousNonexistent1() throws Exception {
    getRecommender().recommendToAnonymous(new String[] {"ABC"}, 3);
  }

  @Test(expected = NoSuchItemException.class)
  public void testRecommendToAnonymousNonexistent2() throws Exception {
    getRecommender().recommendToAnonymous(new String[] {"ABC", "DEF"}, 3);
  }

  @Test
  public void testMostPopular() throws Exception {

    ServerRecommender client = getRecommender();
    List<IDValue> popular = client.mostPopularItems(3);

    assertNotNull(popular);
    assertEquals(3, popular.size());

    log.info("{}", popular);

    assertEquals("50", popular.get(0).getID());
    assertEquals("258", popular.get(1).getID());
    assertEquals("100", popular.get(2).getID());
    assertEquals(583.0f, popular.get(0).getValue());
    assertEquals(509.0f, popular.get(1).getValue());
    assertEquals(508.0f, popular.get(2).getValue());
  }

  @Test
  public void testMostSimilar() throws Exception {

    ServerRecommender client = getRecommender();
    // Adding non-existent item to make sure it is ignored
    List<IDValue> similar = client.mostSimilarItems(new String[] {"449", "ABC"}, 3);

    assertNotNull(similar);
    assertEquals(3, similar.size());

    log.info("{}", similar);

    assertEquals("229", similar.get(0).getID());
    assertEquals("450", similar.get(1).getID());
    assertEquals("227", similar.get(2).getID());
  }

  @Test(expected = NoSuchItemException.class)
  public void testSimilarToNonexistent1() throws Exception {
    getRecommender().mostSimilarItems("ABC", 3);
  }

  @Test(expected = NoSuchItemException.class)
  public void testSimilarToNonexistent2() throws Exception {
    getRecommender().mostSimilarItems(new String[] {"ABC", "DEF"}, 3);
  }

  @Test
  public void testEstimate() throws Exception {

    ServerRecommender client = getRecommender();
    float[] estimates = client.estimatePreferences("10", "90", "91", "92");

    assertNotNull(estimates);
    assertEquals(3, estimates.length);

    log.info("{}", Arrays.toString(estimates));

    assertEquals(0.3006489f, estimates[0]);
    assertEquals(0.42647615f, estimates[1]);
    assertEquals(0.66089016f, estimates[2]);

    // Non-existent
    assertEquals(0.0f, client.estimatePreference("0", "90"));
    assertEquals(0.0f, client.estimatePreference("10", "0"));
  }
  
  @Test
  public void testEstimateForAnonymous() throws Exception {
    ServerRecommender client = getRecommender();
    float estimate = client.estimateForAnonymous("10", new String[] {"90", "91", "92"});
    assertEquals(-0.005063545f, estimate);
  }

  @Test
  public void testBecause() throws Exception {

    ServerRecommender client = getRecommender();
    List<IDValue> because = client.recommendedBecause("1", "321", 3);

    assertNotNull(because);
    assertEquals(3, because.size());

    log.info("{}", because);

    assertEquals("269", because.get(0).getID());
    assertEquals("116", because.get(1).getID());
    assertEquals("242", because.get(2).getID());

    try {
      client.recommendedBecause("0", "222", 3);
      fail();
    } catch (NoSuchUserException nsue) {
      // good
    }
    try {
      client.recommendedBecause("1", "0", 3);
      fail();
    } catch (NoSuchItemException nsie) {
      // good
    }
  }

  @Test
  public void testAnonymous() throws Exception {

    ServerRecommender client = getRecommender();
    List<IDValue> recs = client.recommendToAnonymous(new String[] {"190"}, 3);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("83", recs.get(0).getID());
    assertEquals("213", recs.get(1).getID());
    assertEquals("86", recs.get(2).getID());

    try {
      client.recommendToAnonymous(new String[]{"0"}, 3);
      fail();
    } catch (NoSuchItemException nsie) {
      // good
    }
  }

  @Test
  public void testAnonymous2() throws Exception {

    ServerRecommender client = getRecommender();
    List<IDValue> recs =
        client.recommendToAnonymous(new String[] {"190"}, new float[] {1.0f}, 3);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("83", recs.get(0).getID());
    assertEquals("213", recs.get(1).getID());
    assertEquals("86", recs.get(2).getID());

    try {
      client.recommendToAnonymous(new String[]{"0"}, 3);
      fail();
    } catch (NoSuchItemException nsie) {
      // good
    }
  }

  @Test
  public void testAnonymous3() throws Exception {

    ServerRecommender client = getRecommender();
    List<IDValue> recs =
        client.recommendToAnonymous(new String[] {"190", "200"}, new float[] {2.0f, 3.0f}, 3);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("234", recs.get(0).getID());
    assertEquals("185", recs.get(1).getID());
    assertEquals("176", recs.get(2).getID());

    try {
      client.recommendToAnonymous(new String[]{"0"}, 3);
      fail();
    } catch (NoSuchItemException nsie) {
      // good
    }
  }

  @Test
  public void testSet() throws Exception {
    ServerRecommender client = getRecommender();

    client.setPreference("0", "1");
    List<IDValue> recs = client.recommend("0", 1);
    assertEquals("50", recs.get(0).getID());

    client.setPreference("0", "2", 3.0f);
    recs = client.recommend("0", 1);
    assertEquals("117", recs.get(0).getID());

    client.setPreference("0", "2", -3.0f);
    recs = client.recommend("0", 1);
    assertEquals("117", recs.get(0).getID());

    client.setPreference("0", "1", -1.0f);
    // Don't really know/care what will be recommend at this point; the feature vec is nearly 0
    assertEquals(1, client.recommend("0", 1).size());
  }

  @Test
  public void testSetRemove() throws Exception {
    ServerRecommender client = getRecommender();

    client.setPreference("0", "1");
    List<IDValue> recs = client.recommend("0", 1);
    assertEquals("50", recs.get(0).getID());

    client.setPreference("0", "2", 1.0f);
    recs = client.recommend("0", 1);
    assertEquals("50", recs.get(0).getID());

    client.removePreference("0", "2");
    recs = client.recommend("0", 1);
    assertEquals("50", recs.get(0).getID());

    client.removePreference("0", "1");
    try {
      client.recommend("0", 1);
      fail();
    } catch (NoSuchUserException nsue) {
      // good
    }
  }

}
