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

/*
 * Copyright 2007 Kasper B. Graversen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.oryx.common.io;

import com.google.common.collect.Lists;

import java.util.List;

import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Adapted from <a href="http://supercsv.sourceforge.net/">SuperCSV</a> as a fast/lightweight alternative to
 * its full API.
 *
 * @author Sean Owen
 */
public final class DelimitedDataUtils {

  /**
   * Globally configured delimiter character that is used to delimit and parse delimited data.
   */
  public static final char DELIMITER = ConfigUtils.getDefaultConfig().getString("inbound.delim").charAt(0);
  private static final char QUOTE = '"';
  private static final char EOL_SYMBOLS = '\n';
  private static final String[] NO_TOKENS = new String[0];

  private DelimitedDataUtils() {
  }

  /**
   * @param columns values whose string representation ({@link #toString()}) should be joined into one delimited string
   * @return string representations joined by delimiters
   */
  public static String encode(Object... columns) {
    return encode(columns, DELIMITER);
  }

  public static String encode(Object[] columns, char delim) {
    StringBuilder record = new StringBuilder();
    for (int i = 0; i < columns.length; i++) {
      if (i > 0) {
        record.append(delim);
      }
      record.append(doEncode(columns[i].toString(), delim));
    }
    return record.toString();
  }

  /**
   * @param columns values to be joined into one delimited string
   * @return values joined by delimiters
   */
  public static String encode(CharSequence... columns) {
    return encode(columns, DELIMITER);
  }

  public static String encode(CharSequence[] columns, char delim) {
    StringBuilder record = new StringBuilder();
    for (int i = 0; i < columns.length; i++) {
      if (i > 0) {
        record.append(delim);
      }
      record.append(doEncode(columns[i], delim));
    }
    return record.toString();
  }

  /**
   * @param columns values to be joined into one delimited string
   * @return values joined by delimiters
   */
  public static String encode(Iterable<?> columns) {
    return encode(columns, DELIMITER);
  }

  public static String encode(Iterable<?> columns, char delim) {
    StringBuilder record = new StringBuilder();
    boolean first = true;
    for (Object column : columns) {
      if (first) {
        first = false;
      } else {
        record.append(delim);
      }
      record.append(doEncode(column.toString(), delim));
    }
    return record.toString();
  }

  private static CharSequence doEncode(CharSequence input, char delim) {

    StringBuilder currentColumn = new StringBuilder(input.length());

    int lastCharIndex = input.length() - 1;
    boolean quotesRequiredForSpecialChar = false;
    boolean skipNewline = false;

    for( int i = 0; i <= lastCharIndex; i++ ) {

      char c = input.charAt(i);

      if (skipNewline) {
        skipNewline = false;
        if (c == '\n') {
          continue; // newline following a carriage return is skipped
        }
      }

      if (c == delim) {
        quotesRequiredForSpecialChar = true;
        currentColumn.append(c);
      } else if (c == QUOTE) {
        quotesRequiredForSpecialChar = true;
        currentColumn.append(QUOTE);
        currentColumn.append(QUOTE);
      } else if (c == '\r') {
        quotesRequiredForSpecialChar = true;
        currentColumn.append(EOL_SYMBOLS);
        skipNewline = true;
      } else if (c == '\n') {
        quotesRequiredForSpecialChar = true;
        currentColumn.append(EOL_SYMBOLS);
      } else {
        currentColumn.append(c);
      }
    }

    if (quotesRequiredForSpecialChar) {
      currentColumn.insert(0, QUOTE).append(QUOTE);
    }
    return currentColumn;
  }

  /**
   * @param line a string containing several values delimited by the globally configured delimiter
   * @return those values parsed into individual strings as a {@code String[]}
   */
  public static String[] decode(CharSequence line) {
    return decode(line, DELIMITER);
  }

  public static String[] decode(CharSequence line, char delim) {

    if (line.length() == 0) {
      return NO_TOKENS;
    }

    List<String> columns = Lists.newArrayList();
    StringBuilder currentColumn = new StringBuilder();

    boolean inQuoteMode = false;

    for (int i = 0; i < line.length(); i++) {

      char c = line.charAt(i);

      if (inQuoteMode) {

        if (c == QUOTE) {
          if (i < line.length() - 1 && line.charAt(i + 1) == QUOTE) {
						/*
						 * An escaped quote (""). Add a single quote, then move the cursor so the next iteration of the
						 * loop will read the character following the escaped quote.
						 */
            currentColumn.append(QUOTE);
            i++;
          } else {
						/*
						 * A single quote ("). Update to NORMAL (but don't save quote), then continue to next character.
						 */
            inQuoteMode = false;
          }
        } else {
					/*
					 * Just a normal character, delimiter (they don't count in QUOTESCOPE) or space. Add the character,
					 * then continue to next character.
					 */
          currentColumn.append(c);
        }

      } else {

        if (c == delim) {
					/*
					 * Delimiter. Save the column then continue to next character.
					 */
          if (currentColumn.length() > 0) {
            columns.add(currentColumn.toString());
            currentColumn.setLength(0);
          } else {
            columns.add("");
          }

        } else if (c == QUOTE) {
					/*
					 * A single quote ("). Update (but don't save quote), then continue to next character.
					 */
          inQuoteMode = true;
        } else {
					/*
					 * Just a normal character. Add  the character, then continue to next character.
					 */
          currentColumn.append(c);
        }

      }

    }

    columns.add(currentColumn.toString());

    return columns.isEmpty() ? NO_TOKENS : columns.toArray(new String[columns.size()]);
  }

}
