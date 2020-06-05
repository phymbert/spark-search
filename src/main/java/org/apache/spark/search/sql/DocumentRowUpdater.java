/*
 *    Copyright 2020 the Spark Search contributors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.spark.search.sql;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.spark.search.DocumentUpdater;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * Update a document based on an input Row.
 *
 * @author Pierrick HYMBERT
 */
public class DocumentRowUpdater implements DocumentUpdater<InternalRow> {

    private static final long serialVersionUID = 1L;

    private final StructType schema;

    public DocumentRowUpdater(StructType schema) {
        this.schema = schema;
    }

    @Override
    public void update(IndexingDocument<InternalRow> indexingDocument) throws Exception {
        if (indexingDocument.doc.getFields().isEmpty()) {
            buildFields(indexingDocument);
        }

        InternalRow row = indexingDocument.element;
        StructField[] sqlFields = schema.fields();
        List<IndexableField> docFields = indexingDocument.doc.getFields();
        for (int i = 0; i < sqlFields.length; i++) {
            StructField sqlField = sqlFields[i]; // FIXME support struct fields / array
            String value = null;
            if (!row.isNullAt(i)) {
                value = ConvertUtils.convert(row.getString(i));
            }

            if (value == null) {
                value = "";
            }
            ((Field) docFields.get(i)).setStringValue(value);
        }
    }

    private void buildFields(IndexingDocument<InternalRow> indexingDocument) {
        for (StructField sqlField : schema.fields()) {
            String fieldName = sqlField.name();
            Field.Store storedField = Field.Store.YES;
            if (!indexingDocument.options.isStoreFields()
                    || indexingDocument.options.getNotStoredFields().contains(fieldName)) {
                storedField = Field.Store.NO;
            }

            Field field; //FIXME support struct fields
            if (indexingDocument.options.getFieldIndexOptions() != IndexOptions.DOCS) {
                FieldType fieldType = new FieldType();
                fieldType.setIndexOptions(indexingDocument.options.getFieldIndexOptions());
                if (storedField == Field.Store.YES) {
                    fieldType.setStored(true);
                }
                fieldType.freeze();
                field = new Field(fieldName, "", fieldType);
            } else {
                field = new StringField(fieldName, "", storedField);
            }
            indexingDocument.doc.add(field);
        }
    }
}
