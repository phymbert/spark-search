/*
 * Copyright © 2020 Spark Search (The Spark Search Contributors)
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
package org.apache.spark.sql.search;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.ScoreDoc;
import org.apache.spark.search.DocumentConverter;
import org.apache.spark.search.SearchException;
import org.apache.spark.search.SearchRecordJava;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Convert back a document to row.
 *
 * @author Pierrick HYMBERT
 */
public class DocumentRowConverter implements DocumentConverter<InternalRow> {

    private final StructType schema;

    public DocumentRowConverter(StructType schema) {
        this.schema = schema;
    }

    @Override
    public SearchRecordJava<InternalRow> convert(int partitionIndex, ScoreDoc scoreDoc, Class<InternalRow> classTag, Document doc) throws Exception {
        return new SearchRecordJava<>(scoreDoc.doc, partitionIndex,
                scoreDoc.score, scoreDoc.shardIndex, asRow(doc));
    }

    private InternalRow asRow(Document doc) {
        return new GenericInternalRow(doc.getFields().stream().map(this::convert).toArray());
    }

    private Object convert(IndexableField indexableField) {
        DataType dataType = schema.fields()[schema.fieldIndex(indexableField.name())].dataType();
        if (dataType == DataTypes.StringType) {
            return indexableField.stringValue();
        } else if (dataType == DataTypes.ShortType) {
            return ConvertUtils.convert(indexableField.stringValue(), Short.class);
        } else if (dataType == DataTypes.IntegerType) {//Need to think why we allow indexing other type than strings....
            return ConvertUtils.convert(indexableField.stringValue(), Integer.class);
        } else if (dataType == DataTypes.LongType) {
            return ConvertUtils.convert(indexableField.stringValue(), Long.class);
        } else if (dataType == DataTypes.FloatType) {
            return ConvertUtils.convert(indexableField.stringValue(), Float.class);
        } else if (dataType == DataTypes.DoubleType) {
            return ConvertUtils.convert(indexableField.stringValue(), Double.class);
        } else if (dataType == DataTypes.BooleanType) {
            return ConvertUtils.convert(indexableField.stringValue(), Boolean.class);
        } else {
            throw new SearchException("unsuported row data type " + dataType + " on field " + indexableField.name());
        }
    }
}
