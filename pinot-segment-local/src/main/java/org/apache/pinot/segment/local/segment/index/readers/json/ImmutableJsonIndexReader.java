/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.index.readers.json;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.utils.regex.Matcher;
import org.apache.pinot.common.utils.regex.Pattern;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.BaseJsonIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Reader for json index.
 */
public class ImmutableJsonIndexReader implements JsonIndexReader {
  // NOTE: Use long type for _numDocs to comply with the RoaringBitmap APIs.
  private final long _numDocs;
  private final int _version;
  private final StringDictionary _dictionary;
  private final BitmapInvertedIndexReader _invertedIndex;
  private final long _numFlattenedDocs;
  private final PinotDataBuffer _docIdMapping;

  // empty bitmap used to limit creation of new empty mutable bitmaps
  private static final ImmutableRoaringBitmap EMPTY_BITMAP;

  static {
    // this convoluted way of creating empty immutable bitmap is used here to avoid creating another
    // subclass and potentially affecting roaring bitmap call performance
    MutableRoaringBitmap temp = MutableRoaringBitmap.bitmapOf();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    try (DataOutputStream dos = new DataOutputStream(bos)) {
      temp.serialize(dos);
    } catch (IOException ignoreMe) {
      // nothing to do
    }

    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
    EMPTY_BITMAP = new ImmutableRoaringBitmap(bb);
  }

  public ImmutableJsonIndexReader(PinotDataBuffer dataBuffer, int numDocs) {
    _numDocs = numDocs;
    _version = dataBuffer.getInt(0);
    Preconditions.checkState(_version == BaseJsonIndexCreator.VERSION_1 || _version == BaseJsonIndexCreator.VERSION_2,
        "Unsupported json index version: %s", _version);

    int maxValueLength = dataBuffer.getInt(4);
    long dictionaryLength = dataBuffer.getLong(8);
    long invertedIndexLength = dataBuffer.getLong(16);
    long docIdMappingLength = dataBuffer.getLong(24);

    long dictionaryStartOffset = BaseJsonIndexCreator.HEADER_LENGTH;
    long dictionaryEndOffset = dictionaryStartOffset + dictionaryLength;
    _dictionary =
        new StringDictionary(dataBuffer.view(dictionaryStartOffset, dictionaryEndOffset, ByteOrder.BIG_ENDIAN), 0,
            maxValueLength);
    long invertedIndexEndOffset = dictionaryEndOffset + invertedIndexLength;
    _invertedIndex = new BitmapInvertedIndexReader(
        dataBuffer.view(dictionaryEndOffset, invertedIndexEndOffset, ByteOrder.BIG_ENDIAN), _dictionary.length());
    long docIdMappingEndOffset = invertedIndexEndOffset + docIdMappingLength;
    _numFlattenedDocs = (docIdMappingLength / Integer.BYTES);
    _docIdMapping = dataBuffer.view(invertedIndexEndOffset, docIdMappingEndOffset, ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  public MutableRoaringBitmap getMatchingDocIds(String filterString) {
    FilterContext filter;
    try {
      filter = RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(filterString));
      Preconditions.checkArgument(!filter.isConstant());
    } catch (Exception e) {
      throw new BadQueryRequestException("Invalid json match filter: " + filterString);
    }
    return getMatchingDocIds(filter);
  }

  @Override
  public MutableRoaringBitmap getMatchingDocIds(Object filterObj) {
    if (!(filterObj instanceof FilterContext)) {
      throw new BadQueryRequestException("Invalid json match filter: " + filterObj);
    }
    FilterContext filter = (FilterContext) filterObj;
    if (filter.getType() == FilterContext.Type.PREDICATE && isExclusive(filter.getPredicate().getType())) {
      // Handle exclusive predicate separately because the flip can only be applied to the unflattened doc ids in order
      // to get the correct result, and it cannot be nested
      ImmutableRoaringBitmap flattenedDocIds = getMatchingFlattenedDocIds(filter.getPredicate());
      MutableRoaringBitmap resultDocIds = new MutableRoaringBitmap();
      flattenedDocIds.forEach((IntConsumer) flattenedDocId -> resultDocIds.add(getDocId(flattenedDocId)));
      resultDocIds.flip(0, _numDocs);
      return resultDocIds;
    } else {
      ImmutableRoaringBitmap flattenedDocIds = getMatchingFlattenedDocIds(filter);
      MutableRoaringBitmap resultDocIds = new MutableRoaringBitmap();
      flattenedDocIds.forEach((IntConsumer) flattenedDocId -> resultDocIds.add(getDocId(flattenedDocId)));
      return resultDocIds;
    }
  }

  /**
   * Returns {@code true} if the given predicate type is exclusive for json_match calculation, {@code false} otherwise.
   */
  private boolean isExclusive(Predicate.Type predicateType) {
    return predicateType == Predicate.Type.IS_NULL;
  }

  // AND given bitmaps, optionally converting first one to mutable (if it's not already)
  private static MutableRoaringBitmap and(ImmutableRoaringBitmap target, ImmutableRoaringBitmap other) {
    if (target instanceof MutableRoaringBitmap) {
      MutableRoaringBitmap mutableTarget = (MutableRoaringBitmap) target;
      mutableTarget.and(other);
      return mutableTarget;
    } else if (other instanceof MutableRoaringBitmap) {
      MutableRoaringBitmap mutableOther = (MutableRoaringBitmap) other;
      mutableOther.and(target);
      return mutableOther;
    } else { // base implementation
      MutableRoaringBitmap mutableTarget = toMutable(target);
      mutableTarget.and(other);
      return mutableTarget;
    }
  }

  private static ImmutableRoaringBitmap filter(ImmutableRoaringBitmap result,
      ImmutableRoaringBitmap matchingDocIds) {
    if (result == null) {
      return EMPTY_BITMAP;
    } else if (matchingDocIds == null) {
      return result;
    } else {
      return and(matchingDocIds, result);
    }
  }

  // OR given bitmaps, optionally converting first one to mutable (if it's not already)
  private static MutableRoaringBitmap or(ImmutableRoaringBitmap target, ImmutableRoaringBitmap other) {
    if (target instanceof MutableRoaringBitmap) {
      MutableRoaringBitmap mutableTarget = (MutableRoaringBitmap) target;
      mutableTarget.or(other);
      return mutableTarget;
    } else if (other instanceof MutableRoaringBitmap) {
      MutableRoaringBitmap mutableOther = (MutableRoaringBitmap) other;
      mutableOther.or(target);
      return mutableOther;
    } else { // base implementation
      MutableRoaringBitmap mutableTarget = toMutable(target);
      mutableTarget.or(other);
      return mutableTarget;
    }
  }

  // If given bitmap is not mutable, convert it to such
  // used to delay immutable -> mutable conversion as much as possible
  private static MutableRoaringBitmap toMutable(ImmutableRoaringBitmap bitmap) {
    if (bitmap instanceof MutableRoaringBitmap) {
      return (MutableRoaringBitmap) bitmap;
    } else {
      return bitmap.toMutableRoaringBitmap();
    }
  }

  /**
   * Returns the matching flattened doc ids for the given filter.
   */
  private ImmutableRoaringBitmap getMatchingFlattenedDocIds(FilterContext filter) {
    switch (filter.getType()) {
      case AND: {
        List<FilterContext> filters = filter.getChildren();
        ImmutableRoaringBitmap matchingDocIds = getMatchingFlattenedDocIds(filters.get(0));

        for (int i = 1, numFilters = filters.size(); i < numFilters; i++) {
          // if current set is empty then there is no point AND-ing it with another one
          if (matchingDocIds.isEmpty()) {
            break;
          }

          ImmutableRoaringBitmap filterDocIds = getMatchingFlattenedDocIds(filters.get(i));
          if (filterDocIds.isEmpty()) {
            // potentially avoid converting matchingDocIds to mutable map
            return filterDocIds;
          } else {
            matchingDocIds = and(matchingDocIds, filterDocIds);
          }
        }
        return matchingDocIds;
      }
      case OR: {
        List<FilterContext> filters = filter.getChildren();
        ImmutableRoaringBitmap matchingDocIds = getMatchingFlattenedDocIds(filters.get(0));

        for (int i = 1, numFilters = filters.size(); i < numFilters; i++) {
          ImmutableRoaringBitmap filterDocIds = getMatchingFlattenedDocIds(filters.get(i));
          // avoid having to convert matchingDocIds to mutable map
          if (filterDocIds.isEmpty()) {
            continue;
          }
          matchingDocIds = or(matchingDocIds, filterDocIds);
        }
        return matchingDocIds;
      }
      case PREDICATE: {
        Predicate predicate = filter.getPredicate();
        Preconditions
            .checkArgument(!isExclusive(predicate.getType()), "Exclusive predicate: %s cannot be nested", predicate);
        return getMatchingFlattenedDocIds(predicate);
      }
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Returns the matching flattened doc ids for the given predicate.
   * <p>Exclusive predicate is handled as the inclusive predicate, and the caller should flip the unflattened doc ids in
   * order to get the correct exclusive predicate result.
   * Note: returned bitmap could actually be mutable
   */
  private ImmutableRoaringBitmap getMatchingFlattenedDocIds(Predicate predicate) {
    ExpressionContext lhs = predicate.getLhs();
    Preconditions.checkArgument(lhs.getType() == ExpressionContext.Type.IDENTIFIER,
        "Left-hand side of the predicate must be an identifier, got: %s (%s). Put double quotes around the identifier"
            + " if needed.", lhs, lhs.getType());
    String key = lhs.getIdentifier();
    // Support 2 formats:
    // - JSONPath format (e.g. "$.a[1].b"='abc', "$[0]"=1, "$"='abc')
    // - Legacy format (e.g. "a[1].b"='abc')
    if (_version == BaseJsonIndexCreator.VERSION_2) {
      if (key.startsWith("$")) {
        key = key.substring(1);
      } else {
        key = JsonUtils.KEY_SEPARATOR + key;
      }
    } else {
      // For V1 backward-compatibility
      if (key.startsWith("$.")) {
        key = key.substring(2);
      }
    }
    Pair<String, ImmutableRoaringBitmap> pair = getKeyAndFlattenedDocIds(key);
    key = pair.getLeft();
    ImmutableRoaringBitmap matchingDocIds = pair.getRight();
    if (matchingDocIds != null && matchingDocIds.isEmpty()) {
      return matchingDocIds;
    }

    Predicate.Type predicateType = predicate.getType();
    switch (predicateType) {
      case EQ: {
        String value = ((EqPredicate) predicate).getValue();
        String keyValuePair = key + JsonIndexCreator.KEY_VALUE_SEPARATOR + value;
        int dictId = _dictionary.indexOf(keyValuePair);
        ImmutableRoaringBitmap result = null;
        if (dictId >= 0) {
          result = _invertedIndex.getDocIds(dictId);
        }
        return filter(result, matchingDocIds);
      }

      case NOT_EQ: {
        // each array is un-nested and so flattened json document contains only one value
        // that means for each key-value pair the set of flattened document ids is disjoint
        String notEqualValue = ((NotEqPredicate) predicate).getValue();
        ImmutableRoaringBitmap result = null;

        // read bitmap with all values for this key instead of OR-ing many per-value bitmaps
        int allValuesDictId = _dictionary.indexOf(key);
        if (allValuesDictId >= 0) {
          ImmutableRoaringBitmap allValuesDocIds = _invertedIndex.getDocIds(allValuesDictId);

          if (!allValuesDocIds.isEmpty()) {
            int notEqDictId = _dictionary.indexOf(key + JsonIndexCreator.KEY_VALUE_SEPARATOR + notEqualValue);
            if (notEqDictId >= 0) {
              ImmutableRoaringBitmap notEqDocIds = _invertedIndex.getDocIds(notEqDictId);
              if (notEqDocIds.isEmpty()) {
                //  there's no value to remove, use found bitmap (is this possible ?)
                result = allValuesDocIds;
              } else {
                // remove doc ids for unwanted value
                MutableRoaringBitmap mutableBitmap = allValuesDocIds.toMutableRoaringBitmap();
                mutableBitmap.andNot(notEqDocIds);
                result = mutableBitmap;
              }
            } else { // there's no value to remove, use found bitmap
              result = allValuesDocIds;
            }
          }
        }

        return filter(result, matchingDocIds);
      }

      case IN: {
        List<String> values = ((InPredicate) predicate).getValues();
        ImmutableRoaringBitmap result = null;
        for (String value : values) {
          String keyValuePair = key + JsonIndexCreator.KEY_VALUE_SEPARATOR + value;
          int dictId = _dictionary.indexOf(keyValuePair);
          if (dictId >= 0) {
            ImmutableRoaringBitmap docIds = _invertedIndex.getDocIds(dictId);
            if (result == null) {
              result = docIds;
            } else {
              result = or(result, docIds);
            }
          }
        }

        return filter(result, matchingDocIds);
      }

      case NOT_IN: {
        List<String> notInValues = ((NotInPredicate) predicate).getValues();
        int[] dictIds = getDictIdRangeForKey(key);
        ImmutableRoaringBitmap result = null;

        int valueCount = dictIds[1] - dictIds[0];

        if (notInValues.size() < valueCount / 2) {
          // if there is less notIn values than In values
          // read bitmap for all values and then remove values from bitmaps associated with notIn values

          int allValuesDictId = _dictionary.indexOf(key);
          if (allValuesDictId >= 0) {
            ImmutableRoaringBitmap allValuesDocIds = _invertedIndex.getDocIds(allValuesDictId);

            if (!allValuesDocIds.isEmpty()) {
              result = allValuesDocIds;

              for (String notInValue : notInValues) {
                int notInDictId = _dictionary.indexOf(key + JsonIndexCreator.KEY_VALUE_SEPARATOR + notInValue);
                if (notInDictId >= 0) {
                  ImmutableRoaringBitmap notEqDocIds = _invertedIndex.getDocIds(notInDictId);
                  // remove doc ids for unwanted value
                  MutableRoaringBitmap mutableBitmap = toMutable(result);
                  mutableBitmap.andNot(notEqDocIds);
                  result = mutableBitmap;
                }
              }
            }
          }
        } else {
          // if there is more In values than notIn then OR bitmaps for all values except notIn values
          // resolve dict ids for string values to avoid comparing strings
          IntOpenHashSet notInDictIds = null;
          if (dictIds[0] < dictIds[1]) {
            notInDictIds = new IntOpenHashSet();
            for (String notInValue : notInValues) {
              int dictId = _dictionary.indexOf(key + JsonIndexCreator.KEY_VALUE_SEPARATOR + notInValue);
              if (dictId >= 0) {
                notInDictIds.add(dictId);
              }
            }
          }

          for (int dictId = dictIds[0]; dictId < dictIds[1]; dictId++) {
            if (notInDictIds.contains(dictId)) {
              continue;
            }

            if (result == null) {
              result = _invertedIndex.getDocIds(dictId);
            } else {
              result = or(result, _invertedIndex.getDocIds(dictId));
            }
          }
        }

        return filter(result, matchingDocIds);
      }

      case IS_NOT_NULL:
      case IS_NULL: {
        ImmutableRoaringBitmap result = null;
        int dictId = _dictionary.indexOf(key);
        if (dictId >= 0) {
          result = _invertedIndex.getDocIds(dictId);
        }

        return filter(result, matchingDocIds);
      }

      case REGEXP_LIKE: {
        Pattern pattern = ((RegexpLikePredicate) predicate).getPattern();
        Matcher matcher = pattern.matcher("");
        int[] dictIds = getDictIdRangeForKey(key);

        ImmutableRoaringBitmap result = null;
        byte[] dictBuffer = dictIds[0] < dictIds[1] ? _dictionary.getBuffer() : null;
        StringBuilder value = new StringBuilder();

        for (int dictId = dictIds[0]; dictId < dictIds[1]; dictId++) {
          String stringValue = _dictionary.getStringValue(dictId, dictBuffer);
          value.setLength(0);
          value.append(stringValue, key.length() + 1, stringValue.length());

          if (matcher.reset(value).matches()) {
            if (result == null) {
              result = _invertedIndex.getDocIds(dictId);
            } else {
              result = or(result, _invertedIndex.getDocIds(dictId));
            }
          }
        }

        return filter(result, matchingDocIds);
      }

      case RANGE: {
        RangePredicate rangePredicate = (RangePredicate) predicate;
        FieldSpec.DataType rangeDataType = rangePredicate.getRangeDataType();
        // Simplify to only support numeric and string types
        if (rangeDataType.isNumeric()) {
          rangeDataType = FieldSpec.DataType.DOUBLE;
        } else {
          rangeDataType = FieldSpec.DataType.STRING;
        }

        boolean lowerUnbounded = rangePredicate.getLowerBound().equals(RangePredicate.UNBOUNDED);
        boolean upperUnbounded = rangePredicate.getUpperBound().equals(RangePredicate.UNBOUNDED);
        boolean lowerInclusive = lowerUnbounded || rangePredicate.isLowerInclusive();
        boolean upperInclusive = upperUnbounded || rangePredicate.isUpperInclusive();
        Object lowerBound = lowerUnbounded ? null : rangeDataType.convert(rangePredicate.getLowerBound());
        Object upperBound = upperUnbounded ? null : rangeDataType.convert(rangePredicate.getUpperBound());

        int[] dictIds = getDictIdRangeForKey(key);
        ImmutableRoaringBitmap result = null;
        byte[] dictBuffer = dictIds[0] < dictIds[1] ? _dictionary.getBuffer() : null;

        for (int dictId = dictIds[0]; dictId < dictIds[1]; dictId++) {
          String value = _dictionary.getStringValue(dictId, dictBuffer).substring(key.length() + 1);
          Object valueObj = rangeDataType.convert(value);
          boolean lowerCompareResult =
              lowerUnbounded || (lowerInclusive ? rangeDataType.compare(valueObj, lowerBound) >= 0
                  : rangeDataType.compare(valueObj, lowerBound) > 0);
          boolean upperCompareResult =
              upperUnbounded || (upperInclusive ? rangeDataType.compare(valueObj, upperBound) <= 0
                  : rangeDataType.compare(valueObj, upperBound) < 0);

          if (lowerCompareResult && upperCompareResult) {
            if (result == null) {
              result = _invertedIndex.getDocIds(dictId);
            } else {
              result = or(result, _invertedIndex.getDocIds(dictId));
            }
          }
        }

        return filter(result, matchingDocIds);
      }

      default:
        throw new IllegalStateException("Unsupported json_match predicate type: " + predicate);
    }
  }

  private int getDocId(int flattenedDocId) {
    return _docIdMapping.getInt((long) flattenedDocId << 2);
  }

  public void convertFlattenedDocIdsToDocIds(Map<String, RoaringBitmap> valueToFlattenedDocIds) {
    valueToFlattenedDocIds.replaceAll((key, value) -> {
      RoaringBitmap docIds = new RoaringBitmap();
      value.forEach((IntConsumer) flattenedDocId -> docIds.add(getDocId(flattenedDocId)));
      return docIds;
    });
  }

  @Override
  public Map<String, RoaringBitmap> getMatchingFlattenedDocsMap(String jsonPathKey, @Nullable String filterString) {
    RoaringBitmap filteredFlattenedDocIds = null;
    if (filterString != null) {
      FilterContext filter;
      try {
        filter = RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(filterString));
        Preconditions.checkArgument(!filter.isConstant());
      } catch (Exception e) {
        throw new BadQueryRequestException("Invalid json match filter: " + filterString);
      }
      if (filter.getType() == FilterContext.Type.PREDICATE && isExclusive(filter.getPredicate().getType())) {
        // Handle exclusive predicate separately because the flip can only be applied to the
        // unflattened doc ids in order to get the correct result, and it cannot be nested
        filteredFlattenedDocIds = getMatchingFlattenedDocIds(filter.getPredicate()).toRoaringBitmap();
        filteredFlattenedDocIds.flip(0, _numFlattenedDocs);
      } else {
        filteredFlattenedDocIds = getMatchingFlattenedDocIds(filter).toRoaringBitmap();
      }
    }
    // Support 2 formats:
    // - JSONPath format (e.g. "$.a[1].b"='abc', "$[0]"=1, "$"='abc')
    // - Legacy format (e.g. "a[1].b"='abc')
    if (_version == BaseJsonIndexCreator.VERSION_2) {
      if (jsonPathKey.startsWith("$")) {
        jsonPathKey = jsonPathKey.substring(1);
      } else {
        jsonPathKey = JsonUtils.KEY_SEPARATOR + jsonPathKey;
      }
    } else {
      // For V1 backward-compatibility
      if (jsonPathKey.startsWith("$.")) {
        jsonPathKey = jsonPathKey.substring(2);
      }
    }
    Map<String, RoaringBitmap> result = new HashMap<>();
    Pair<String, ImmutableRoaringBitmap> pathKey = getKeyAndFlattenedDocIds(jsonPathKey);
    if (pathKey.getRight() != null && pathKey.getRight().isEmpty()) {
      return result;
    }

    jsonPathKey = pathKey.getLeft();
    RoaringBitmap arrayIndexFlattenDocIds = null;
    if (pathKey.getRight() != null) {
      arrayIndexFlattenDocIds = pathKey.getRight().toRoaringBitmap();
    }
    int[] dictIds = getDictIdRangeForKey(jsonPathKey);
    byte[] dictBuffer = dictIds[0] < dictIds[1] ? _dictionary.getBuffer() : null;

    for (int dictId = dictIds[0]; dictId < dictIds[1]; dictId++) {
      String key = _dictionary.getStringValue(dictId, dictBuffer);
      RoaringBitmap docIds = _invertedIndex.getDocIds(dictId).toRoaringBitmap();
      if (filteredFlattenedDocIds != null) {
        docIds.and(filteredFlattenedDocIds);
      }

      if (arrayIndexFlattenDocIds != null) {
        docIds.and(arrayIndexFlattenDocIds);
      }

      if (!docIds.isEmpty()) {
        result.put(key.substring(jsonPathKey.length() + 1), docIds);
        Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(result.size());
      }
    }

    return result;
  }

  @Override
  public String[][] getValuesMV(int[] docIds, int length,
      Map<String, RoaringBitmap> valueToMatchingFlattenedDocs) {
    String[][] result = new String[length][];
    List<PriorityQueue<Pair<String, Integer>>> docIdToFlattenedDocIdsAndValues = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      // Sort based on flattened doc id
      docIdToFlattenedDocIdsAndValues.add(new PriorityQueue<>(Comparator.comparingInt(Pair::getRight)));
    }
    Map<Integer, Integer> docIdToPos = new HashMap<>();
    for (int i = 0; i < length; i++) {
      docIdToPos.put(docIds[i], i);
    }

    for (Map.Entry<String, RoaringBitmap> entry : valueToMatchingFlattenedDocs.entrySet()) {
      String value = entry.getKey();
      RoaringBitmap matchingFlattenedDocIds = entry.getValue();
      matchingFlattenedDocIds.forEach((IntConsumer) flattenedDocId -> {
        int docId = getDocId(flattenedDocId);
        if (docIdToPos.containsKey(docId)) {
          docIdToFlattenedDocIdsAndValues.get(docIdToPos.get(docId)).add(Pair.of(value, flattenedDocId));
        }
      });
    }

    for (int i = 0; i < length; i++) {
      PriorityQueue<Pair<String, Integer>> pq = docIdToFlattenedDocIdsAndValues.get(i);
      result[i] = new String[pq.size()];
      int j = 0;
      while (!pq.isEmpty()) {
        result[i][j++] = pq.poll().getLeft();
      }
    }

    return result;
  }

  @Override
  public String[] getValuesSV(int[] docIds, int length, Map<String, RoaringBitmap> valueToMatchingDocs,
      boolean isFlattenedDocIds) {
    Int2ObjectOpenHashMap<String> docIdToValues = new Int2ObjectOpenHashMap<>(docIds.length);
    RoaringBitmap docIdMask = RoaringBitmap.bitmapOf(docIds);

    for (Map.Entry<String, RoaringBitmap> entry : valueToMatchingDocs.entrySet()) {
      String value = entry.getKey();
      RoaringBitmap matchingDocIds = entry.getValue();
      if (isFlattenedDocIds) {
        matchingDocIds.forEach((IntConsumer) flattenedDocId -> {
          int docId = getDocId(flattenedDocId);
          if (docIdMask.contains(docId)) {
            docIdToValues.put(docId, value);
          }
        });
      } else {
        RoaringBitmap intersection = RoaringBitmap.and(matchingDocIds, docIdMask);
        if (intersection.isEmpty()) {
          continue;
        }
        for (int docId : intersection) {
          docIdToValues.put(docId, entry.getKey());
        }
      }
    }

    String[] values = new String[length];
    for (int i = 0; i < length; i++) {
      values[i] = docIdToValues.get(docIds[i]);
    }
    return values;
  }

  /**
   * For a JSON key path, returns an int array of the range [min, max] spanning all values for the JSON key path
   */
  private int[] getDictIdRangeForKey(String key) {
    // json_index uses \0 as the separator (or \u0000 in unicode)
    // therefore, use the unicode char \u0001 to get the range of dict entries that have this prefix

    // get min for key
    int indexOfMin = _dictionary.indexOf(key);
    if (indexOfMin == -1) {
      return new int[]{-1, -1}; // if key does not exist, immediately return
    }
    int indexOfMax = _dictionary.insertionIndexOf(key + JsonIndexCreator.KEY_VALUE_SEPARATOR_NEXT_CHAR);

    int minDictId = indexOfMin + 1; // skip the index of the key only
    int maxDictId = -1 * indexOfMax - 1; // undo the binary search
    if (indexOfMax > 0) {
      maxDictId = indexOfMax;
    }

    return new int[]{minDictId, maxDictId};
  }

  /**
   *  If key doesn't contain the array index, return <original key, null bitmap>
   *  Elif the key, i.e. the json path provided by user doesn't match any data, return <null, empty bitmap>
   *  Else, return the json path that is generated by replacing array index with . on the original key
   *  and the associated flattenDocId bitmap
   */
  private Pair<String, ImmutableRoaringBitmap> getKeyAndFlattenedDocIds(String key) {
    ImmutableRoaringBitmap matchingDocIds = null;

    if (_version == BaseJsonIndexCreator.VERSION_2) {
      // Process the array index within the key if exists
      // E.g. "[*]"=1 -> "."='1'
      // E.g. "[0]"=1 -> ".$index"='0' && "."='1'
      // E.g. "[0][1]"=1 -> ".$index"='0' && "..$index"='1' && ".."='1'
      // E.g. ".foo[*].bar[*].foobar"='abc' -> ".foo..bar..foobar"='abc'
      // E.g. ".foo[0].bar[1].foobar"='abc' -> ".foo.$index"='0' && ".foo..bar.$index"='1' && ".foo..bar..foobar"='abc'
      // E.g. ".foo[0][1].bar"='abc' -> ".foo.$index"='0' && ".foo..$index"='1' && ".foo...bar"='abc'
      int leftBracketIndex;
      while ((leftBracketIndex = key.indexOf('[')) >= 0) {
        int rightBracketIndex = key.indexOf(']', leftBracketIndex + 2);
        Preconditions.checkArgument(rightBracketIndex > 0, "Missing right bracket in key: %s", key);

        String leftPart = key.substring(0, leftBracketIndex);
        String arrayIndex = key.substring(leftBracketIndex + 1, rightBracketIndex);
        String rightPart = key.substring(rightBracketIndex + 1);

        if (!arrayIndex.equals(JsonUtils.WILDCARD)) {
          // "[0]"=1 -> ".$index"='0' && "."='1'
          // ".foo[1].bar"='abc' -> ".foo.$index"=1 && ".foo..bar"='abc'
          String searchKey =
                  leftPart + JsonUtils.ARRAY_INDEX_KEY + BaseJsonIndexCreator.KEY_VALUE_SEPARATOR + arrayIndex;
          int dictId = _dictionary.indexOf(searchKey);
          if (dictId >= 0) {
            ImmutableRoaringBitmap docIds = _invertedIndex.getDocIds(dictId);
            if (matchingDocIds == null) {
              matchingDocIds = docIds;
            } else {
              matchingDocIds = and(matchingDocIds, docIds);
            }
          } else {
            return Pair.of(null, EMPTY_BITMAP);
          }
        }

        key = leftPart + JsonUtils.KEY_SEPARATOR + rightPart;
      }
    } else {
      // For V1 backward-compatibility
      // Process the array index within the key if exists
      // E.g. "foo[0].bar[1].foobar"='abc' -> "foo.$index"=0 && "foo.bar.$index"=1 && "foo.bar.foobar"='abc'
      int leftBracketIndex;
      while ((leftBracketIndex = key.indexOf('[')) > 0) {
        int rightBracketIndex = key.indexOf(']', leftBracketIndex + 2);
        Preconditions.checkArgument(rightBracketIndex > 0, "Missing right bracket in key: %s", key);

        String leftPart = key.substring(0, leftBracketIndex);
        String arrayIndex = key.substring(leftBracketIndex + 1, rightBracketIndex);
        String rightPart = key.substring(rightBracketIndex + 1);

        if (!arrayIndex.equals(JsonUtils.WILDCARD)) {
          // "foo[1].bar"='abc' -> "foo.$index"=1 && "foo.bar"='abc'
          String searchKey =
                  leftPart + JsonUtils.ARRAY_INDEX_KEY + BaseJsonIndexCreator.KEY_VALUE_SEPARATOR + arrayIndex;
          int dictId = _dictionary.indexOf(searchKey);
          if (dictId >= 0) {
            ImmutableRoaringBitmap docIds = _invertedIndex.getDocIds(dictId);
            if (matchingDocIds == null) {
              matchingDocIds = docIds;
            } else {
              matchingDocIds = and(matchingDocIds, docIds);
            }
          } else {
            return Pair.of(null, EMPTY_BITMAP);
          }
        }

        key = leftPart + rightPart;
      }
    }
    return Pair.of(key, matchingDocIds);
  }

  private PeekableIntIterator intersect(MutableRoaringBitmap a, ImmutableRoaringBitmap b) {
    a.and(b);
    return a.getIntIterator();
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
