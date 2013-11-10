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

package com.cloudera.oryx.als.computation.merge;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.fn.OryxReduceDoFn;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.commons.math3.util.FastMath;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

public final class MergeNewOldValuesFn extends OryxReduceDoFn<Pair<Long, Integer>, Iterable<NumericIDValue>,
    Pair<Long, NumericIDValue>> {

  private static final int BEFORE = 0;
  private static final int AFTER = 1;

  private boolean doDecay;
  private float decayFactor;
  private float zeroThreshold;
  private Long previousUserID;
  private LongFloatMap previousUserPrefs;

  @Override
  public void initialize() {
    super.initialize();
    Config config = ConfigUtils.getDefaultConfig();
    decayFactor = (float) config.getDouble("model.decay.factor");
    Preconditions.checkArgument(decayFactor > 0.0f && decayFactor <= 1.0f,
                                "Decay factor must be in (0,1]: %s", zeroThreshold);
    zeroThreshold = (float) config.getDouble("model.decay.zeroThreshold");
    Preconditions.checkArgument(zeroThreshold >= 0.0f,
                                "Zero threshold must be nonnegative: %s", zeroThreshold);
    doDecay = decayFactor < 1.0f;
  }

  @Override
  public void process(Pair<Pair<Long, Integer>, Iterable<NumericIDValue>> input,
                      Emitter<Pair<Long, NumericIDValue>> emitter) {
    Pair<Long, Integer> key = input.first();
    long currentUserID = key.first();

    if (key.second() == BEFORE) {

      // Last old data had no match, just output it
      if (previousUserPrefs != null) {
        Preconditions.checkNotNull(previousUserID);
        output(previousUserID, previousUserPrefs, null, null, emitter);
        previousUserPrefs = null;
        previousUserID = null;
      }

      LongFloatMap oldPrefs = new LongFloatMap();
      for (NumericIDValue itemPref : input.second()) {
        float oldPrefValue = itemPref.getValue();
        Preconditions.checkState(!Float.isNaN(oldPrefValue), "No prior pref value?");
        // Apply decay factor here, if applicable:
        oldPrefs.increment(itemPref.getID(), doDecay ? oldPrefValue * decayFactor : oldPrefValue);
      }

      previousUserPrefs = oldPrefs;
      previousUserID = currentUserID;

    } else {
      // Last old data had no match, just output it
      if (previousUserPrefs != null && currentUserID != previousUserID) {
        Preconditions.checkNotNull(previousUserID);
        output(previousUserID, previousUserPrefs, null, null, emitter);
        previousUserPrefs = null;
        previousUserID = null;
      }

      LongFloatMap newPrefs = new LongFloatMap();
      LongSet removedItemIDs = new LongSet();
      for (NumericIDValue itemPref : input.second()) {
        long itemID = itemPref.getID();
        float newPrefValue = itemPref.getValue();
        if (Float.isNaN(newPrefValue)) {
          removedItemIDs.add(itemID);
        } else {
          newPrefs.increment(itemID, newPrefValue);
        }
      }

      output(currentUserID, previousUserPrefs, newPrefs, removedItemIDs, emitter);

      previousUserPrefs = null;
      previousUserID = null;
    }
  }

  @Override
  public void cleanup(Emitter<Pair<Long, NumericIDValue>> emitter) {
    if (previousUserPrefs != null) {
      Preconditions.checkNotNull(previousUserID);
      output(previousUserID, previousUserPrefs, null, null, emitter);
    }
    super.cleanup(emitter);
  }

  private void output(long userID,
                      LongFloatMap oldPrefs,
                      LongFloatMap newPrefs,
                      LongSet removedItemIDs,
                      Emitter<Pair<Long, NumericIDValue>> emitter) {
    // Old prefs may be null when there is no previous generation, for example, or the user is new.
    // First, write out existing prefs, possibly updated by new values
    if (oldPrefs != null && !oldPrefs.isEmpty()) {
      for (LongFloatMap.MapEntry entry : oldPrefs.entrySet()) {
        long itemID = entry.getKey();
        float oldPrefValue = entry.getValue();
        Preconditions.checkState(!Float.isNaN(oldPrefValue), "No prior pref value?");

        // May be NaN if no new data at all, or new data has no update:
        float sum = oldPrefValue;
        if (newPrefs != null) {
          float newPrefValue = newPrefs.get(itemID);
          if (!Float.isNaN(newPrefValue)) {
            sum += newPrefValue;
          }
        }

        boolean remove = false;
        if (removedItemIDs != null && removedItemIDs.contains(itemID)) {
          remove = true;
        } else if (FastMath.abs(sum) <= zeroThreshold) {
          remove = true;
        }

        if (!remove) {
          emitter.emit(Pair.of(userID, new NumericIDValue(itemID, sum)));
        }
      }
    }

    // Now output new data, that didn't exist in old prefs
    if (newPrefs != null && !newPrefs.isEmpty()) {
      for (LongFloatMap.MapEntry entry : newPrefs.entrySet()) {
        long itemID = entry.getKey();
        if (oldPrefs == null || !oldPrefs.containsKey(itemID)) {
          // It wasn't already written. If it exists in newPrefs, it's also not removed
          float newPrefValue = entry.getValue();
          if (FastMath.abs(newPrefValue) > zeroThreshold) {
            emitter.emit(Pair.of(userID, new NumericIDValue(itemID, newPrefValue)));
          }
        }
      }
    }
  }
}
