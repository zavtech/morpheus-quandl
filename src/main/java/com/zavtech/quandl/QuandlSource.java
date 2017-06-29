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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipFile;

import static com.zavtech.quandl.QuandlField.COLUMN_NAMES;
import static com.zavtech.quandl.QuandlField.DATABASE_CODE;
import static com.zavtech.quandl.QuandlField.DATABASE_ID;
import static com.zavtech.quandl.QuandlField.DATASET_CODE;
import static com.zavtech.quandl.QuandlField.DATASET_ID;
import static com.zavtech.quandl.QuandlField.DATASET_TYPE;
import static com.zavtech.quandl.QuandlField.DESCRIPTION;
import static com.zavtech.quandl.QuandlField.END_DATE;
import static com.zavtech.quandl.QuandlField.FREQUENCY;
import static com.zavtech.quandl.QuandlField.LAST_REFRESH_TIME;
import static com.zavtech.quandl.QuandlField.NAME;
import static com.zavtech.quandl.QuandlField.PREMIUM;
import static com.zavtech.quandl.QuandlField.START_DATE;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.IO;
import com.zavtech.morpheus.util.Json;

/**
 * A DataFrameSource implementation used to load meta-data and data from Qunadl.com
 *
 * @link https://www.quandl.com/docs/api
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class QuandlSource<R,C> implements DataFrameSource<R,C,QuandlOptions<R,C>> {

    private String apiKey;
    private String baseUrl;

    /**
     * Constructor
     * @param apiKey    the Quandl API token
     */
    public QuandlSource(String apiKey) {
        this("https://www.quandl.com", apiKey);
    }

    /**
     * Constructor
     * @param baseUrl   the Quandl base url
     * @param apiKey    the Quandl API token
     */
    public QuandlSource(String baseUrl, String apiKey) {
        Objects.requireNonNull(baseUrl, "The Quandl baseUrl cannot be null");
        Objects.requireNonNull(apiKey, "The Quandl apiKey cannot be null");
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
    }

    /**
     * Returns the fully qualified Quandl URL string
     * @param request   the request descriptor
     * @param path      the path to append to base url
     * @return          the Quandl request URL
     */
    private String createUrl(QuandlOptions<R,C> request, String path) {
        return createUrl(request, path, null);
    }

    /**
     * Returns the fully qualified Quandl URL string
     * @param request   the request descriptor
     * @param path      the path to append to base url
     * @param query     the query string if any, null permitted
     * @return          the Quandl request URL
     */
    private String createUrl(QuandlOptions<R,C> request, String path, String query) {
        final String apiKey = request.getApiKey().orElse(this.apiKey);
        final String url = baseUrl + path + "?api_key=" + apiKey;
        return query != null ? url + "&" + query : url;
    }


    @Override
    public <T extends Options<?,?>> boolean isSupported(T options) {
        return options instanceof QuandlOptions;
    }

    @Override
    @SuppressWarnings("unchecked")
    public DataFrame<R, C> read(QuandlOptions options) throws DataFrameException {
        switch (Options.validate(options).getOperation()) {
            case DATA:      return (DataFrame<R,C>)getData(options);
            case DATASETS:  return (DataFrame<R,C>)getDatasets(options);
            case DATABASES: return (DataFrame<R,C>)getDatabases(options);
            case META_DATA: return (DataFrame<R,C>)getMetaData(options);
            default:        throw new DataFrameException("Unsupported request: " + options);
        }
    }


    /**
     * Returns a DataFrame containing data for the request specified
     * @param request   the Quandl request for data
     * @return          the resulting DataFrame
     */
    private DataFrame<LocalDate,String> getData(QuandlOptions<R,C> request) {
        try {
            final String database = request.getDatabaseCode().orElse(null);
            if (database == null) throw new QuandlException("No database code specified in Quandl request");
            final String dataset = request.getDatasetCode().orElse(null);
            if (dataset == null) throw new QuandlException("No dataset code specified in Quandl request");
            final String queryString = request.toQueryString();
            final String urlString = createUrl(request, "/api/v3/datasets/" + database + "/" + dataset + ".csv", queryString);
            System.out.println(urlString);
            return DataFrame.read().csv(options -> {
                options.setResource(urlString);
                options.setColIndexPredicate(index -> index != 0);
                options.setRowKeyParser(LocalDate.class, v -> LocalDate.parse(v[0]));
            });
        } catch (Exception ex) {
            throw new QuandlException("Failed to load data from Quandl for " + request, ex);
        }
    }


    /**
     * Returns a DataFrame containing metadata for the request specified
     * @param request   the Quandl request for data
     * @return          the resulting DataFrame
     */
    private DataFrame<Integer,QuandlField> getMetaData(QuandlOptions<R,C> request) {
        try {
            final String database = request.getDatabaseCode().orElse(null);
            if (database == null) throw new QuandlException("No database code specified in Quandl request");
            final String dataset = request.getDatasetCode().orElse(null);
            if (dataset == null) throw new QuandlException("No dataset code specified in Quandl request");
            final String urlString = createUrl(request, "/api/v3/datasets/" + database + "/" + dataset + "/metadata.json");
            final QuandlDatasetInfo datasetInfo = new Json().parse(new URL(urlString), QuandlDatasetInfo.class);
            final Set<Integer> rowKeys = Collections.singleton(datasetInfo.getId());
            return DataFrame.of(rowKeys, QuandlField.class, columns -> {
                columns.add(NAME, Array.of(datasetInfo.getName()));
                columns.add(DESCRIPTION, Array.of(datasetInfo.getDescription()));
                columns.add(DATASET_CODE, Array.of(datasetInfo.getDatasetCode()));
                columns.add(DATABASE_CODE, Array.of(datasetInfo.getDatabaseCode()));
                columns.add(LAST_REFRESH_TIME, Array.of(datasetInfo.getRefreshedAt()));
                columns.add(START_DATE, Array.of(datasetInfo.getOldestAvailableDate()));
                columns.add(END_DATE, Array.of(datasetInfo.getNewestAvailableDate()));
                columns.add(DATASET_TYPE, Array.of(datasetInfo.getType()));
                columns.add(FREQUENCY, Array.of(datasetInfo.getFrequency()));
                columns.add(DATABASE_ID, Array.of(datasetInfo.getDatabaseId()));
                columns.add(DATASET_ID, Array.of(datasetInfo.getId()));
                columns.add(PREMIUM, Array.of(datasetInfo.isPremium()));
                columns.add(COLUMN_NAMES, Array.singleton(datasetInfo.getColumnNames()));
            });
        } catch (Exception ex) {
            throw new QuandlException("Failed to dataset metadata listing from Quandl for " + request, ex);
        }
    }


    /**
     * Returns a DataFrame with a listing of all codes in a dataset along with a description
     * @param request       the request descriptor
     * @return              the resulting DataFrame
     */
    private DataFrame<String,QuandlField> getDatasets(QuandlOptions<R,C> request) {
        try {
            final String database = request.getDatabaseCode().orElse(null);
            if (database == null) {
                throw new QuandlException("No database code specified for Quandl request:" + request);
            } else {
                final String urlString = createUrl(request, "/api/v3/databases/" + database  + "/codes.csv");
                final File localFile = downloadZipFile(new URL(urlString));
                final ZipFile zipfile = new ZipFile(localFile);
                final List<DataFrame<String,String>> frameList = new ArrayList<>();
                zipfile.stream().forEach(entry -> {
                    try {
                        final DataFrame<String,String> frame = DataFrame.read().csv(options -> {
                            try {
                                options.setHeader(false);
                                options.setExcludeColumns("Column-0");
                                options.setRowKeyParser(String.class, row -> row[0]);
                                options.setResource(zipfile.getInputStream(entry));
                            } catch (IOException ex) {
                                throw new RuntimeException("Failed to extract entry from zip file", ex);
                            }
                        });
                        frame.cols().replaceKey("Column-1", QuandlField.DESCRIPTION.name());
                        frame.cols().add(QuandlField.DATABASE_CODE.name(), String.class).applyValues(v -> database);
                        frameList.add(frame);
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to extract content from zip file", ex);
                    }
                });
                final DataFrame<String,String> union = DataFrame.union(frameList);
                return union.cols().mapKeys(column -> QuandlField.of(column.key()));
            }
        } catch (Exception ex) {
            throw new QuandlException("Failed to load dataset listing from Quandl for " + request, ex);
        }
    }


    /**
     * Returns a DataFrame with a full listing of all databases available on Quandl
     * @param request   the request descriptor
     * @return  the DataFrame with a full listing of Quandl databases
     * @throws QuandlException  if this operation fails
     */
    private DataFrame<Integer,QuandlField> getDatabases(QuandlOptions<R,C> request) throws QuandlException {
        try {
            final int maxPages = request.getMaxPages().orElse(100);
            final int pageSize = request.getPageSize().orElse(100);
            final List<DataFrame<Integer,String>> frameList = new ArrayList<>();
            for (int i=0; i<maxPages; ++i) {
                final String url = createUrl(request, "/api/v3/databases.csv", "page=" + i + "&per_page=" + pageSize);
                final DataFrame<Integer,String> frame = DataFrame.read().csv(options -> {
                    options.setResource(url);
                    options.setExcludeColumns("id");
                    options.setColumnType("datasets_count", Long.class);
                    options.setColumnType("downloads", Long.class);
                    options.setRowKeyParser(Integer.class, v -> Integer.parseInt(v[0]));
                });
                if (frame.rowCount() == 0) break;
                frameList.add(frame);
            }
            final DataFrame<Integer,String> union = DataFrame.union(frameList);
            return union.cols().mapKeys(column -> QuandlField.of(column.key()));
        } catch (Exception ex) {
            throw new QuandlException("Failed to load database list from Quandl: " + ex.getMessage(), ex);
        }
    }


    private File downloadZipFile(URL url) throws IOException {
        BufferedInputStream bis = null;
        BufferedOutputStream bos = null;
        try {
            final String tmpDir = System.getProperty("java.io.tmpdir");
            final File file = new File(tmpDir, UUID.randomUUID().toString() + ".zip");
            file.deleteOnExit();
            bis = new BufferedInputStream(url.openStream());
            bos = new BufferedOutputStream(new FileOutputStream(file));
            final byte[] buffer = new byte[1024 * 100];
            while (true) {
                final int read = bis.read(buffer);
                if (read < 0) break;
                bos.write(buffer, 0, read);
            }
            return file;
        } catch (Exception ex) {
            throw new QuandlException("Failed to download zip file from " + url, ex);
        } finally {
            IO.close(bis);
            IO.close(bos);
        }
    }

}
