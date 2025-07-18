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

package org.apache.pinot.segment.local.segment.index.text;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.NativeMutableTextIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndex;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.text.NativeTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.TextIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.text.NativeTextIndexReader;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TextIndexType extends AbstractIndexType<TextIndexConfig, TextIndexReader, TextIndexCreator> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(TextIndexType.class);

  public static final String INDEX_DISPLAY_NAME = "text";
  // TODO: Should V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION be added here?
  private static final List<String> EXTENSIONS = Lists.newArrayList(
      V1Constants.Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.NATIVE_TEXT_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.LUCENE_V99_TEXT_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION
  );

  protected TextIndexType() {
    super(StandardIndexes.TEXT_ID);
  }

  @Override
  public Class<TextIndexConfig> getIndexConfigClass() {
    return TextIndexConfig.class;
  }

  @Override
  public TextIndexConfig getDefaultConfig() {
    return TextIndexConfig.DISABLED;
  }

  @Override
  public void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
    TextIndexConfig textIndexConfig = indexConfigs.getConfig(StandardIndexes.text());
    if (textIndexConfig.isEnabled()) {
      Preconditions.checkState(fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.STRING,
          "Cannot create TEXT index on column: %s of stored type other than STRING", fieldSpec.getName());
    }
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  protected ColumnConfigDeserializer<TextIndexConfig> createDeserializerForLegacyConfigs() {
    return IndexConfigDeserializer.fromIndexTypes(FieldConfig.IndexType.TEXT, (tableConfig, fieldConfig) -> {
      Map<String, String> properties = fieldConfig.getProperties();
      FSTType fstType = TextIndexUtils.isFstTypeNative(properties) ? FSTType.NATIVE : FSTType.LUCENE;
      return new TextIndexConfigBuilder(fstType).withProperties(properties).build();
    });
  }

  @Override
  public TextIndexCreator createIndexCreator(IndexCreationContext context, TextIndexConfig indexConfig)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.STRING,
        "Text index is currently only supported on STRING type columns");
    if (indexConfig.getFstType() == FSTType.NATIVE) {
      return new NativeTextIndexCreator(context.getFieldSpec().getName(), context.getTableNameWithType(),
          context.isContinueOnError(), context.getIndexDir());
    } else {
      return new LuceneTextIndexCreator(context, indexConfig);
    }
  }

  @Override
  protected IndexReaderFactory<TextIndexReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      Schema schema, TableConfig tableConfig) {
    return new TextIndexHandler(segmentDirectory, configsByCol, tableConfig, schema);
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }

  private static class ReaderFactory implements IndexReaderFactory<TextIndexReader> {

    public static final ReaderFactory INSTANCE = new ReaderFactory();

    private ReaderFactory() {
    }

    @Override
    public TextIndexReader createIndexReader(SegmentDirectory.Reader segmentReader, FieldIndexConfigs fieldIndexConfigs,
        ColumnMetadata metadata)
        throws IndexReaderConstraintException {
      if (metadata.getDataType() != FieldSpec.DataType.STRING) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), StandardIndexes.text(),
            "Text index is currently only supported on STRING type columns");
      }
      File segmentDir = segmentReader.toSegmentDirectory().getPath().toFile();
      FSTType textIndexFSTType = TextIndexUtils.getFSTTypeOfIndex(segmentDir, metadata.getColumnName());
      if (textIndexFSTType == FSTType.NATIVE) {
        // TODO: Support loading native text index from a PinotDataBuffer
        return new NativeTextIndexReader(metadata.getColumnName(), segmentDir);
      }
      TextIndexConfig indexConfig = fieldIndexConfigs.getConfig(StandardIndexes.text());
      return new LuceneTextIndexReader(metadata.getColumnName(), segmentDir, metadata.getTotalDocs(), indexConfig);
    }
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, TextIndexConfig config) {
    if (config.isDisabled()) {
      return null;
    }
    if (config.getFstType() == FSTType.NATIVE) {
      return new NativeMutableTextIndex(context.getFieldSpec().getName());
    }
    return new RealtimeLuceneTextIndex(context.getFieldSpec().getName(), context.getConsumerDir(),
        context.getSegmentName(), config);
  }
}
