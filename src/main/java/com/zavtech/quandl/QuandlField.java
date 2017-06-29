/**
 * Copyright (C) 2014-2017 Xavier Witdouck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zavtech.quandl;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public enum QuandlField {

    NAME,
    DESCRIPTION,
    DATABASE_CODE,
    DATASET_CODE,
    DATASET_COUNT,
    DOWNLOADS,
    PREMIUM,
    IMAGE_URL,
    LAST_REFRESH_TIME,
    START_DATE,
    END_DATE,
    DATASET_TYPE,
    FREQUENCY,
    DATABASE_ID,
    DATASET_ID,
    COLUMN_NAMES,
    FAVOURITE,
    URL_NAME;

    public static final Map<String,QuandlField> fieldMap = new HashMap<>();

    /**
     * Static initializer
     */
    static {
        fieldMap.put("name", QuandlField.NAME);
        fieldMap.put("description", QuandlField.DESCRIPTION);
        fieldMap.put("databasecode", QuandlField.DATABASE_CODE);
        fieldMap.put("database_code", QuandlField.DATABASE_CODE);
        fieldMap.put("datasetcode", QuandlField.DATASET_CODE);
        fieldMap.put("dataset_code", QuandlField.DATASET_CODE);
        fieldMap.put("datasetcount", QuandlField.DATASET_COUNT);
        fieldMap.put("datasets_count", QuandlField.DATASET_COUNT);
        fieldMap.put("downloads", QuandlField.DOWNLOADS);
        fieldMap.put("premium", QuandlField.PREMIUM);
        fieldMap.put("imageurl", QuandlField.IMAGE_URL);
        fieldMap.put("image", QuandlField.IMAGE_URL);
        fieldMap.put("favorite", QuandlField.FAVOURITE);
        fieldMap.put("url_name", QuandlField.URL_NAME);
    }

    /**
     * Returns the QuandlField representation for the name specified
     * @param name  the name for a field
     * @return      the matched field
     * @throws QuandlException  if there is no match for a field
     */
    public static QuandlField of(String name) {
        final QuandlField field = fieldMap.get(name.toLowerCase());
        if (field == null) {
            throw new QuandlException("No match for field named: " + name);
        } else {
            return field;
        }
    }

}
