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
import java.util.function.Consumer;

import com.zavtech.morpheus.frame.DataFrame;

/**
 * A convenience class that provides a high level API to load both meta-data and data from Quandl.com
 *
 * @link https://www.quandl.com/docs/api
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class Quandl {

    private QuandlSource<?,?> source;

    /**
     * Constructor
     * @param apiKey    the API key to access Quandl
     */
    public Quandl(String apiKey) {
        this("https://www.quandl.com/", apiKey);
    }

    /**
     * Constructor
     * @param baseUrl   the base url to access Quandl, such as https://www.quandl.com/
     * @param apiKey    the API key to access Quandl
     */
    public Quandl(String baseUrl, String apiKey) {
        DataFrame.read().register(new QuandlSource(baseUrl, apiKey));
    }

    /**
     * Returns a DataFrame with a full listing of all databases available on Quandl
     * @return  the DataFrame with a full listing of Quandl databases
     * @throws QuandlException  if this operation fails
     */
    @SuppressWarnings("unchecked")
    public DataFrame<Integer,QuandlField> getDatabaseListing() throws QuandlException {
        return DataFrame.read().apply(QuandlOptions.class, options -> {
            options.setOperation(QuandlOptions.Operation.DATABASES);
        });
    }

    /**
     * Returns a DataFrame with a listing of all datasets in the specified database
     * @param database  the Quandl database code, for example "WIKI"
     * @return          the DataFrame with dataset listing for database
     * @throws QuandlException  if this operation fails
     */
    @SuppressWarnings("unchecked")
    public DataFrame<String,QuandlField> getDatasetListing(String database) throws QuandlException {
        return DataFrame.read().apply(QuandlOptions.class, options -> {
            options.setOperation(QuandlOptions.Operation.DATASETS);
            options.setDatabase(database);
        });
    }

    /**
     * Returns a DataFrame containing metadata for the database and dataset specified
     * @param database  the Quandl database code, for example "WIKI"
     * @param dataset   the Quandl dataset code in database, for example "AAPL"
     * @return          the Dataframe containing data
     */
    @SuppressWarnings("unchecked")
    public DataFrame<String,QuandlField> getMetaData(String database, String dataset) throws QuandlException {
        return DataFrame.read().apply(QuandlOptions.class, options -> {
            options.setOperation(QuandlOptions.Operation.META_DATA);
            options.setDatabase(database);
            options.setDataset(dataset);
        });
    }

    /**
     * Returns a DataFrame containing data for the database and dataset specified
     * @param database      the Quandl database code, for example "WIKI"
     * @param dataset       the Quandl dataset code in database, for example "AAPL"
     * @param configurator  the configurator for options
     * @return              the Dataframe containing data
     */
    @SuppressWarnings("unchecked")
    public DataFrame<LocalDate,String> getDailyData(String database, String dataset, Consumer<QuandlOptions> configurator) throws QuandlException {
        return DataFrame.read().apply(QuandlOptions.class, options -> {
            options.setOperation(QuandlOptions.Operation.DATA);
            options.setDatabase(database);
            options.setDataset(dataset);
            configurator.accept(options);
        });
    }

}
