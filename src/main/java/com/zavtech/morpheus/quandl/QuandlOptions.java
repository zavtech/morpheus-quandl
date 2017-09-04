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
package com.zavtech.morpheus.quandl;

import java.time.LocalDate;
import java.util.Optional;

import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;

/**
 * A generic request descriptor to load various kinds of data and meta-data from Quandl.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class QuandlOptions<R,C> implements DataFrameSource.Options<R,C> {

    enum Operation {
        DATA,
        DATABASES,
        DATASETS,
        META_DATA
    }

    private String apiKey;
    private Integer limit;
    private Integer rows;
    private Integer colIndex;
    private Integer maxPages;
    private Integer pageSize;
    private String databaseCode;
    private String datasetCode;
    private LocalDate startDate;
    private LocalDate endDate;
    private Operation operation;
    private Boolean ascending;


    /**
     * Constructor
     */
    public QuandlOptions() {
        this.ascending = true;
    }


    @Override
    public void validate() {
        Asserts.notNull(getOperation(), "The operation code must be specified");
        if (getOperation() == Operation.DATA) {
            Asserts.check(getStartDate().isPresent(), "The start date must be specified");
            Asserts.check(getEndDate().isPresent(), "The end date must be specified");
            Asserts.check(getDatabaseCode().isPresent(), "The database code must be specified");
            Asserts.check(getDatasetCode().isPresent(), "The dataset code must be specified");
        } else if (getOperation() == Operation.META_DATA) {

        }
    }

    /**
     * Sets the operation for these options
     * @param operation the operation
     */
    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    /**
     * Sets the API key used to access Quandl for this request
     * @param apiKey    the API key for request, otherwise use key associated with source
     */
    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    /**
     * Sets the database code for these options
     * @param databaseCode  the database code
     */
    public void setDatabase(String databaseCode) {
        this.databaseCode = databaseCode;
    }

    /**
     * Sets the dataset code for these options
     * @param datasetCode   the dataset code
     */
    public void setDataset(String datasetCode) {
        this.datasetCode = datasetCode;
    }

    /**
     * Sets the start date for these options
     * @param startDate the start date
     */
    public void setStartDate(LocalDate startDate) {
        this.startDate = startDate;
    }

    /**
     * Sets the end date for these options
     * @param endDate   the end date
     */
    public void setEndDate(LocalDate endDate) {
        this.endDate = endDate;
    }

    /**
     * Sets the start date for these options
     * @param startDate the start date
     */
    public void setStartDate(String startDate) {
        this.startDate = startDate != null ? LocalDate.parse(startDate) : null;
    }

    /**
     * Sets the end date for these options
     * @param endDate the end date
     */
    public void setEndDate(String endDate) {
        this.endDate = endDate != null ? LocalDate.parse(endDate) : null;
    }

    /**
     * Returns the operation code for these options
     * @return  the operation code
     */
    public Operation getOperation() {
        return operation;
    }

    /**
     * Returns the optional api key for this request
     * @return  the optional api key
     */
    public Optional<String> getApiKey() {
        return Optional.ofNullable(apiKey);
    }

    /**
     * Returns the optional limit for data requests
     * @return  the limit for data requests
     */
    public Optional<Integer> getLimit() {
        return Optional.ofNullable(limit);
    }

    /**
     * Returns the optional max pages for a data request
     * @return  the optional max pages
     */
    public Optional<Integer> getMaxPages() {
        return Optional.ofNullable(maxPages);
    }

    /**
     * Returns the optional page size for a data request
     * @return  the optional page size
     */
    public Optional<Integer> getPageSize() {
        return Optional.ofNullable(pageSize);
    }

    public Optional<Integer> getRows() {
        return Optional.ofNullable(rows);
    }

    public Optional<Integer> getColIndex() {
        return Optional.ofNullable(colIndex);
    }

    /**
     * Returns the database code for these options
     * @return  the database code
     */
    public Optional<String> getDatabaseCode() {
        return Optional.ofNullable(databaseCode);
    }

    /**
     * Returns the dataset code for these options
     * @return  the dataset code
     */
    public Optional<String> getDatasetCode() {
        return Optional.ofNullable(datasetCode);
    }

    /**
     * Returns the optional start date for data requests
     * @return  the optional start date
     */
    public Optional<LocalDate> getStartDate() {
        return Optional.ofNullable(startDate);
    }

    /**
     * Returns the optional end date for data requests
     * @return  the optional end date
     */
    public Optional<LocalDate> getEndDate() {
        return Optional.ofNullable(endDate);
    }

    /**
     * Returns the optional for ascending/descending order for data requests
     * @return      the optional for data order
     */
    public Optional<Boolean> isAscending() {
        return Optional.ofNullable(ascending);
    }

    /**
     * Returns a URL query string for these options
     * @return      the URL query string
     */
    String toQueryString() {
        final StringBuilder query = new StringBuilder();
        getStartDate().ifPresent(date -> query.append("start_date=").append(date));
        query.append(query.length() > 0 ? "&" : "");
        getEndDate().ifPresent(date -> query.append("end_date=").append(date));
        query.append(query.length() > 0 ? "&" : "");
        getColIndex().ifPresent(colIndex -> query.append("column_index=").append(colIndex));
        query.append(query.length() > 0 ? "&" : "");
        getRows().ifPresent(rows -> query.append("rows=").append(colIndex));
        query.append(query.length() > 0 ? "&" : "");
        getLimit().ifPresent(limit -> query.append("limit=").append(limit));
        query.append(query.length() > 0 ? "&" : "");
        isAscending().ifPresent(asc -> query.append(asc ? "order=asc" : "order=desc"));
        return query.toString();
    }

}
