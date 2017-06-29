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

import java.time.LocalDate;
import java.time.ZonedDateTime;

import com.google.gson.annotations.SerializedName;

/**
 * A class that captures meta-data for a specific dataset in a Qunadl database
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class QuandlDatasetInfo {

    @SerializedName("dataset")
    private Details dataset = new Details();

    /**
     * Constructor
     */
    public QuandlDatasetInfo() {
        super();
    }

    /**
     * Returns the dataset id
     * @return  dataset id
     */
    public int getId() {
        return dataset.id;
    }

    /**
     * Returns the dataset Quandl code
     * @return  the dataset Quandl code
     */
    public String getDatasetCode() {
        return dataset.datasetCode;
    }

    /**
     * Returns the database Quandl code
     * @return  the database Quandl code
     */
    public String getDatabaseCode() {
        return dataset.databaseCode;
    }

    /**
     * Returns the dataset name
     * @return  the dataset name
     */
    public String getName() {
        return dataset.name;
    }

    /**
     * Returns the dataset description
     * @return  the dataset description
     */
    public String getDescription() {
        return dataset.description;
    }

    /**
     * Returns the last refresh time for dataset
     * @return  last refresh time
     */
    public ZonedDateTime getRefreshedAt() {
        return dataset.refreshedAt;
    }

    /**
     * Returns the last available date for this dataset
     * @return  the last available date
     */
    public LocalDate getNewestAvailableDate() {
        return dataset.newestAvailableDate;
    }

    /**
     * Returns the first available date for thid dataset
     * @return  the first available date
     */
    public LocalDate getOldestAvailableDate() {
        return dataset.oldestAvailableDate;
    }

    /**
     * Returns the column names for this dataset
     * @return  the column names
     */
    public String[] getColumnNames() {
        return dataset.columnNames;
    }

    /**
     * Returns the frequency for this dataset
     * @return  the frequency for dataset
     */
    public String getFrequency() {
        return dataset.frequency;
    }

    /**
     * Returns the dataset type
     * @return  the dataset type
     */
    public String getType() {
        return dataset.type;
    }

    /**
     * Returns true if this is a premium dataset
     * @return  true if premium dataset
     */
    public boolean isPremium() {
        return dataset.premium;
    }

    /**
     * Returns the database id
     * @return  the database id
     */
    public int getDatabaseId() {
        return dataset.databaseId;
    }

    public static class Details {
        @SerializedName("id")
        private int id;
        @SerializedName("dataset_code")
        private String datasetCode;
        @SerializedName("database_code")
        private String databaseCode;
        @SerializedName("name")
        private String name;
        @SerializedName("description")
        private String description;
        @SerializedName("refreshed_at")
        private ZonedDateTime refreshedAt;
        @SerializedName("newest_available_date")
        private LocalDate newestAvailableDate;
        @SerializedName("oldest_available_date")
        private LocalDate oldestAvailableDate;
        @SerializedName("column_names")
        private String[] columnNames;
        @SerializedName("frequency")
        private String frequency;
        @SerializedName("type")
        private String type;
        @SerializedName("premium")
        private boolean premium;
        @SerializedName("database_id")
        private int databaseId;
    }
}
