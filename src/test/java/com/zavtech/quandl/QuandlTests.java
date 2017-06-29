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
import java.util.Arrays;
import java.util.Collections;

import com.zavtech.morpheus.frame.DataFrame;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static com.zavtech.quandl.QuandlField.*;

/**
 * A unit test for testing the Quandl download adapter
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class QuandlTests {

    private Quandl quandl = new Quandl("DrFK1MBShGiB32kCHZXx");

    @DataProvider(name="wiki")
    public Object[][] wiki() {
        return new Object[][] {
                {"AAPL", 12},
                {"MSFT", 12},
                {"GE", 12}
        };
    }

    @DataProvider(name="metadata")
    public Object[][] metadata() {
        return new Object[][] {
                {"WIKI", "AAPL"},
                {"FRED", "GBP3MTD156N"},
                {"CBOE", "VIX"}
        };
    }


    @DataProvider(name="libor")
    public Object[][] fredLibor() {
        return new Object[][] {
                {"CAD3MTD156N", 1},
                {"AUD3MTD156N", 1},
                {"GBP3MTD156N", 1}
        };
    }

    @Test()
    public void testDatabaseListing() {
        final DataFrame<Integer,QuandlField> frame = quandl.getDatabaseListing();
        frame.out().print(100);
        Assert.assertTrue(frame.rowCount() > 0);
        Assert.assertEquals(frame.colCount(), 9);
        Assert.assertTrue(frame.cols().containsAll(Arrays.asList(NAME, DESCRIPTION, DATABASE_CODE, DATASET_COUNT, DOWNLOADS, PREMIUM, IMAGE_URL)));
        Assert.assertEquals(frame.cols().type(NAME), String.class);
        Assert.assertEquals(frame.cols().type(DESCRIPTION), String.class);
        Assert.assertEquals(frame.cols().type(DATABASE_CODE), String.class);
        Assert.assertEquals(frame.cols().type(DATASET_COUNT), Long.class);
        Assert.assertEquals(frame.cols().type(DOWNLOADS), Long.class);
        Assert.assertEquals(frame.cols().type(PREMIUM), Boolean.class);
        Assert.assertEquals(frame.cols().type(IMAGE_URL), String.class);
        Assert.assertTrue(frame.colAt(DATABASE_CODE).values().filter(v -> !v.isNull() && v.getValue().equals("WIKI")).count() > 0);
    }


    @Test()
    public void testDatasetListing() {
        final String databaseCode = "WIKI";
        final DataFrame<String,QuandlField> frame = quandl.getDatasetListing(databaseCode);
        Assert.assertTrue(frame != null);
        Assert.assertTrue(frame.rowCount() > 0);
        Assert.assertEquals(frame.colCount(), 2);
        Assert.assertTrue(frame.cols().containsAll(Arrays.asList(DATABASE_CODE, DESCRIPTION)));
        Assert.assertEquals(frame.cols().type(DATABASE_CODE), String.class);
        Assert.assertEquals(frame.cols().type(DESCRIPTION), String.class);
        Assert.assertTrue(frame.rows().select(row -> row.key().contains("AAPL")).rowCount() > 0);
        frame.colAt(DATABASE_CODE).values().forEach(v -> Assert.assertEquals(v.getValue(), databaseCode));
        frame.out().print();
    }


    @Test(dataProvider = "metadata")
    public void testDatasetMetaData(String database, String dataset) {
        final DataFrame<String,QuandlField> frame = quandl.getMetaData(database, dataset);
        Assert.assertTrue(frame != null);
        Assert.assertTrue(frame.rowCount() > 0);
        Assert.assertEquals(frame.colCount(), 13);
        Assert.assertEquals(frame.cols().type(NAME), String.class);
        Assert.assertEquals(frame.cols().type(DESCRIPTION), String.class);
        Assert.assertEquals(frame.cols().type(DATASET_CODE), String.class);
        Assert.assertEquals(frame.cols().type(DATABASE_CODE), String.class);
        Assert.assertEquals(frame.cols().type(LAST_REFRESH_TIME), ZonedDateTime.class);
        Assert.assertEquals(frame.cols().type(START_DATE), LocalDate.class);
        Assert.assertEquals(frame.cols().type(END_DATE), LocalDate.class);
        Assert.assertEquals(frame.cols().type(DATASET_TYPE), String.class);
        Assert.assertEquals(frame.cols().type(FREQUENCY), String.class);
        Assert.assertEquals(frame.cols().type(DATABASE_ID), Integer.class);
        Assert.assertEquals(frame.cols().type(DATASET_ID), Integer.class);
        Assert.assertEquals(frame.cols().type(PREMIUM), Boolean.class);
        Assert.assertEquals(frame.cols().type(COLUMN_NAMES), String[].class);
        frame.out().print();
    }


    @Test(dataProvider = "wiki")
    public void testDailyWikiData(String dataset, int expectedColCount) {
        final DataFrame<LocalDate,String> frame = quandl.getDailyData("WIKI", dataset, options -> {
            options.setStartDate("2014-01-06");
            options.setEndDate("2014-02-04");
        });
        frame.out().print();
        Assert.assertTrue(frame.rowCount() > 0);
        Assert.assertEquals(frame.colCount(), expectedColCount);
        Assert.assertTrue(frame.cols().containsAll(Arrays.asList("Open", "High", "Low", "Close", "Volume", "Ex-Dividend", "Split Ratio")));
        Assert.assertEquals(frame.cols().type("Open"), Double.class);
        Assert.assertEquals(frame.cols().type("High"), Double.class);
        Assert.assertEquals(frame.cols().type("Low"), Double.class);
        Assert.assertEquals(frame.cols().type("Close"), Double.class);
        Assert.assertEquals(frame.cols().type("Volume"), Double.class);
        Assert.assertEquals(frame.cols().type("Ex-Dividend"), Double.class);
        Assert.assertEquals(frame.cols().type("Split Ratio"), Double.class);
        Assert.assertEquals(frame.rows().firstKey().get(), LocalDate.of(2014, 1, 6));
        Assert.assertEquals(frame.rows().lastKey().get(), LocalDate.of(2014, 2, 4));
        frame.out().print();
    }


    @Test(dataProvider = "libor")
    public void testDailyFredLibor(String dataset, int expectedColCount) {
        final DataFrame<LocalDate,String> frame = quandl.getDailyData("FRED", dataset, options -> {
            options.setStartDate("2000-01-04");
            options.setEndDate("2000-02-02");
        });
        frame.out().print();
        Assert.assertTrue(frame.rowCount() > 0);
        Assert.assertEquals(frame.colCount(), expectedColCount);
        Assert.assertTrue(frame.cols().containsAll(Collections.singleton(("VALUE"))));
        Assert.assertEquals(frame.cols().type("VALUE"), Double.class);
        Assert.assertEquals(frame.rows().firstKey().get(), LocalDate.of(2000, 1, 4));
        Assert.assertEquals(frame.rows().lastKey().get(), LocalDate.of(2000, 2, 2));
        frame.out().print();
    }


    @Test()
    public void ukGDP() {
        final DataFrame<LocalDate,String> frame = quandl.getDailyData("UKONS", "BKVT_A", options -> {
            options.setStartDate("2000-01-04");
            options.setEndDate("2016-01-01");
        });
        frame.out().print();
        Assert.assertTrue(frame.rowCount() > 0);
        Assert.assertEquals(frame.colCount(), 1);
        Assert.assertEquals(frame.rows().firstKey().get(), LocalDate.of(2000, 12, 31));
        Assert.assertEquals(frame.rows().lastKey().get(), LocalDate.of(2015, 12, 31));
        frame.out().print();
    }
}
