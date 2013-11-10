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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import com.cloudera.oryx.als.common.IDValue;
import com.cloudera.oryx.als.common.NoSuchItemException;
import com.cloudera.oryx.als.common.NoSuchUserException;
import com.cloudera.oryx.als.serving.ServerRecommender;

/**
 * Tests the ALS implementation using the simple
 * <a href="http://grouplens.org/datasets/movielens/">GroupLens</a> 100K data set. The data set is
 * however modified such that IDs are non-numeric to test ability to handle this type of ID.
 *
 * @author Sean Owen
 */
public final class SimpleNonnumericDataIT extends AbstractComputationIT {

  private static final Logger log = LoggerFactory.getLogger(SimpleNonnumericDataIT.class);

  @Override
  protected File getTestDataPath() {
    return getResourceAsFile("grouplens100K-ABC");
  }

  @Test
  public void testIngest() throws Exception {
    Reader reader = new StringReader("A,B\nA,C,3.0\n");

    ServerRecommender client = getRecommender();
    client.ingest(reader);

    List<IDValue> recs = client.recommend("A", 3);
    log.info("{}", recs);
    assertEquals("22C", recs.get(0).getID());
  }

  @Test
  public void testRecommend() throws Exception {

    ServerRecommender client = getRecommender();
    List<IDValue> recs = client.recommend("B", 3);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("58C", recs.get(0).getID());
    assertEquals("47F", recs.get(1).getID());
    assertEquals("53B", recs.get(2).getID());

    try {
      client.recommend("A", 3);
      fail();
    } catch (NoSuchUserException nsue) {
      // good
    }

    recs = client.recommend("B", 3, true, null);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("15D", recs.get(0).getID());
    assertEquals("17J", recs.get(1).getID());
    assertEquals("7A", recs.get(2).getID());
  }

  @Test
  public void testRecommendToMany() throws Exception {

    ServerRecommender client = getRecommender();
    // Adding non-existent item to make sure it is ignored
    List<IDValue> recs =
        client.recommendToMany(new String[] {"B", "D", "foo"}, 3, false, null);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("28G", recs.get(0).getID());
    assertEquals("30C", recs.get(1).getID());
    assertEquals("28I", recs.get(2).getID());
  }
  
  @Test
  public void testRecommendToMany2() throws Exception {
    ServerRecommender client = getRecommender();
    List<IDValue> recs =
        client.recommendToMany(new String[] {"D", "E", "F", "G", "H"}, 3, true, null);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("5A", recs.get(0).getID());
    assertEquals("25I", recs.get(1).getID());
    assertEquals("28I", recs.get(2).getID());
  }
  
  @Test
  public void testRecommendVersusToMany() throws Exception {
    ServerRecommender client = getRecommender();
    List<IDValue> recs = client.recommend("B", 3);
    List<IDValue> recs2 =
        client.recommendToMany(new String[] {"B"}, 3, false, null);
    assertEquals(recs, recs2);
  }
  
  @Test
  public void testRecommendVersusToMany2() throws Exception {
    ServerRecommender client = getRecommender();
    List<IDValue> recs = client.recommendToMany(new String[] {"E", "C"}, 3, true, null);
    List<IDValue> recs2 = client.recommendToMany(new String[] {"C", "E"}, 3, true, null);
    assertEquals(recs, recs2);
  }

  @Test(expected = NoSuchUserException.class)
  public void testRecommendToManyNonexistent1() throws Exception {
    getRecommender().recommendToMany(new String[] {"foo"}, 3, false, null);
  }

  @Test(expected = NoSuchUserException.class)
  public void testRecommendToManyNonexistent2() throws Exception {
    getRecommender().recommendToMany(new String[] {"foo", "bar"}, 3, false, null);
  }

  @Test
  public void testRecommendToAnonymous() throws Exception {

    ServerRecommender client = getRecommender();
    // Adding non-existent item to make sure it is ignored
    List<IDValue> recs = client.recommendToAnonymous(new String[] {"B", "D", "foo"}, 3);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("15B", recs.get(0).getID());
    assertEquals("22C", recs.get(1).getID());
    assertEquals("5A", recs.get(2).getID());
  }

  @Test(expected = NoSuchItemException.class)
  public void testRecommendToAnonymousNonexistent1() throws Exception {
    getRecommender().recommendToAnonymous(new String[] {"foo"}, 3);
  }

  @Test(expected = NoSuchItemException.class)
  public void testRecommendToAnonymousNonexistent2() throws Exception {
    getRecommender().recommendToAnonymous(new String[] {"foo", "bar"}, 3);
  }

  @Test
  public void testMostPopular() throws Exception {

    ServerRecommender client = getRecommender();
    List<IDValue> popular = client.mostPopularItems(3);

    assertNotNull(popular);
    assertEquals(3, popular.size());

    log.info("{}", popular);

    assertEquals("5A", popular.get(0).getID());
    assertEquals("25I", popular.get(1).getID());
    assertEquals("10A", popular.get(2).getID());
    assertEquals(583.0f, popular.get(0).getValue());
    assertEquals(509.0f, popular.get(1).getValue());
    assertEquals(508.0f, popular.get(2).getValue());
  }

  @Test
  public void testMostSimilar() throws Exception {

    ServerRecommender client = getRecommender();
    // Adding non-existent item to make sure it is ignored
    List<IDValue> similar = client.mostSimilarItems(new String[] {"44J", "foo"}, 3);

    assertNotNull(similar);
    assertEquals(3, similar.size());

    log.info("{}", similar);

    assertEquals("22J", similar.get(0).getID());
    assertEquals("45A", similar.get(1).getID());
    assertEquals("22H", similar.get(2).getID());
  }

  @Test(expected = NoSuchItemException.class)
  public void testSimilarToNonexistent1() throws Exception {
    getRecommender().mostSimilarItems("foo", 3);
  }

  @Test(expected = NoSuchItemException.class)
  public void testSimilarToNonexistent2() throws Exception {
    getRecommender().mostSimilarItems(new String[] {"foo", "bar"}, 3);
  }

  @Test
  public void testEstimate() throws Exception {

    ServerRecommender client = getRecommender();
    float[] estimates = client.estimatePreferences("1A", "9A", "9B", "9C");

    assertNotNull(estimates);
    assertEquals(3, estimates.length);

    log.info("{}", Arrays.toString(estimates));

    assertEquals(0.3099646f, estimates[0]);
    assertEquals(0.4366314f, estimates[1]);
    assertEquals(0.64034194f, estimates[2]);

    // Non-existent
    assertEquals(0.0f, client.estimatePreference("A", "90"));
    assertEquals(0.0f, client.estimatePreference("1A", "0"));
  }
  
  @Test
  public void testEstimateForAnonymous() throws Exception {
    ServerRecommender client = getRecommender();
    float estimate = client.estimateForAnonymous("1A", new String[] {"9A", "9B", "9C"});
    assertEquals(-0.0042706127f, estimate);
  }

  @Test
  public void testBecause() throws Exception {

    ServerRecommender client = getRecommender();
    List<IDValue> because = client.recommendedBecause("B", "32B", 3);

    assertNotNull(because);
    assertEquals(3, because.size());

    log.info("{}", because);

    assertEquals("26J", because.get(0).getID());
    assertEquals("11G", because.get(1).getID());
    assertEquals("12E", because.get(2).getID());

    try {
      client.recommendedBecause("A", "22B", 3);
      fail();
    } catch (NoSuchUserException nsue) {
      // good
    }
    try {
      client.recommendedBecause("B", "A", 3);
      fail();
    } catch (NoSuchItemException nsie) {
      // good
    }
  }

  @Test
  public void testAnonymous() throws Exception {

    ServerRecommender client = getRecommender();
    List<IDValue> recs = client.recommendToAnonymous(new String[] {"19A"}, 3);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("8D", recs.get(0).getID());
    assertEquals("103J", recs.get(1).getID());
    assertEquals("20H", recs.get(2).getID());

    try {
      client.recommendToAnonymous(new String[]{"A"}, 3);
      fail();
    } catch (NoSuchItemException nsie) {
      // good
    }
  }

  @Test
  public void testAnonymous2() throws Exception {

    ServerRecommender client = getRecommender();
    List<IDValue> recs =
        client.recommendToAnonymous(new String[] {"19A"}, new float[] {1.0f}, 3);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("8D", recs.get(0).getID());
    assertEquals("103J", recs.get(1).getID());
    assertEquals("20H", recs.get(2).getID());

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
        client.recommendToAnonymous(new String[] {"19A", "20A"}, new float[] {2.0f, 3.0f}, 3);

    assertNotNull(recs);
    assertEquals(3, recs.size());

    log.info("{}", recs);

    assertEquals("18F", recs.get(0).getID());
    assertEquals("16E", recs.get(1).getID());
    assertEquals("103J", recs.get(2).getID());

    try {
      client.recommendToAnonymous(new String[]{"A"}, 3);
      fail();
    } catch (NoSuchItemException nsie) {
      // good
    }
  }

  @Test
  public void testSet() throws Exception {
    ServerRecommender client = getRecommender();

    client.setPreference("A", "B");
    List<IDValue> recs = client.recommend("A", 1);
    assertEquals("5A", recs.get(0).getID());

    client.setPreference("A", "C", 3.0f);
    recs = client.recommend("A", 1);
    assertEquals("22C", recs.get(0).getID());

    client.setPreference("A", "C", -3.0f);
    recs = client.recommend("A", 1);
    assertEquals("22C", recs.get(0).getID());

    client.setPreference("A", "B", -1.0f);
    // Don't really know/care what will be recommend at this point; the feature vec is nearly 0
    assertEquals(1, client.recommend("A", 1).size());
  }

  @Test
  public void testSetRemove() throws Exception {
    ServerRecommender client = getRecommender();

    client.setPreference("A", "B");
    List<IDValue> recs = client.recommend("A", 1);
    assertEquals("5A", recs.get(0).getID());

    client.setPreference("A", "C", 1.0f);
    recs = client.recommend("A", 1);
    assertEquals("22C", recs.get(0).getID());

    client.removePreference("A", "C");
    recs = client.recommend("A", 1);
    assertEquals("22C", recs.get(0).getID());

    client.removePreference("A", "B");
    try {
      client.recommend("A", 1);
      fail();
    } catch (NoSuchUserException nsue) {
      // good
    }
  }

}
