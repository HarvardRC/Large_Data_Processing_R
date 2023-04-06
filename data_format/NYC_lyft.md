Big Data, Data Format, and Streaming
================
Naeem Khoshnevis
<br> Updated April 06, 2023

- [Summary](#summary)
- [Research Question](#research-question)
- [Data](#data)
- [File Format](#file-format)
  - [Use a fast binary data storage format that enables reading data
    subsets](#use-a-fast-binary-data-storage-format-that-enables-reading-data-subsets)
  - [Partition the data on disk to facilitate chunked access and
    computation](#partition-the-data-on-disk-to-facilitate-chunked-access-and-computation)
  - [Only read in the data you need](#only-read-in-the-data-you-need)
  - [Use streaming data tools and
    algorithms](#use-streaming-data-tools-and-algorithms)
  - [Avoid unnecessarily storing or duplicating data in
    memory](#avoid-unnecessarily-storing-or-duplicating-data-in-memory)
- [Solution 1: Using `data.table` with csv
  file](#solution-1-using-datatable-with-csv-file)
- [Solution 2: Using `data.table` with csv file in
  Parallel](#solution-2-using-datatable-with-csv-file-in-parallel)
- [Solution 3: Convert .csv to
  parquet](#solution-3-convert-csv-to-parquet)
  - [Read and count Lyft records with
    arrow](#read-and-count-lyft-records-with-arrow)
- [Solution 4: Streaming data with
  duckdb](#solution-4-streaming-data-with-duckdb)

|                                                                                                                             |
|-----------------------------------------------------------------------------------------------------------------------------|
| The original example is provided by Ben Sabath and Ista Zahn. Read more [here](https://github.com/hbs-rcs/large_data_in_R). |

## Summary

We present how choosing good format and right technology can address big
data challenges. These days systems with large computational resources
is easier to access. However, because of advanced technology in sensors
and data collection, there is always possible to have a need to work
with data that does not fit into the system’s memory. In this report, we
use binary structure and streaming package to process relatively large
data.

## Research Question

**How many Lyft rides were taken in New York City during 2020?**

This report, covers three different approaches in addressing data
analyses challenges. In handling big data,

## Data

The data required to address this question is publicly available at New
York City Taxi & Limousine Commission.

<br>

<img src="figures/nyc-tlc-logo.png" width="40%" style="display: block; margin: auto;" />

<br>

Download Data from
[here](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

*note*: After 05/13/2022, TLC reports data in PARQUET format. You can
download csv files from
[here](https://drive.google.com/drive/folders/1d-r0uEtRaUMSiEzkUvKJZ01CBYg6z3-O?usp=sharing).

Data Dictionary is available from
[here](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf).

According to the data dictionary, Lyft license number
(`Hvfhs_license_num`) is (`HV0005`).

## File Format

File format has an important role in flexibility and read, write, and
query performance. The following figure shows different categories for
files.

<br>

<img src="figures/file_format.png" width="100%" style="display: block; margin: auto;" />

<br>

Let’s take a look at the file names and sizes:

``` r
fhvhv_csv_files <- list.files("data/original_csv", recursive=TRUE, full.names = TRUE)
data.frame(file = fhvhv_csv_files, size_Mb = file.size(fhvhv_csv_files) / 1024^2)
```

    ##                                                    file   size_Mb
    ## 1  data/original_csv/2020/01/fhvhv_tripdata_2020-01.csv 1243.4975
    ## 2  data/original_csv/2020/02/fhvhv_tripdata_2020-02.csv 1313.2442
    ## 3  data/original_csv/2020/03/fhvhv_tripdata_2020-03.csv  808.5597
    ## 4  data/original_csv/2020/04/fhvhv_tripdata_2020-04.csv  259.5806
    ## 5  data/original_csv/2020/05/fhvhv_tripdata_2020-05.csv  366.5430
    ## 6  data/original_csv/2020/06/fhvhv_tripdata_2020-06.csv  454.5977
    ## 7  data/original_csv/2020/07/fhvhv_tripdata_2020-07.csv  599.2560
    ## 8  data/original_csv/2020/08/fhvhv_tripdata_2020-08.csv  667.6880
    ## 9  data/original_csv/2020/09/fhvhv_tripdata_2020-09.csv  728.5463
    ## 10 data/original_csv/2020/10/fhvhv_tripdata_2020-10.csv  798.4743
    ## 11 data/original_csv/2020/11/fhvhv_tripdata_2020-11.csv  698.0638
    ## 12 data/original_csv/2020/12/fhvhv_tripdata_2020-12.csv  700.6804

The most traditional way to find the number of Lyft rides is to load the
data, bind the rows together, and then filter based on the Lyft license
code.

``` r
library(tidyverse)
fhvhv_data <- map(fhvhv_csv_files, read_csv) %>% bind_rows(show_col_types=FALSE)
```

However, this will raise an error (if data is large enough and system’s
RAM is small enough).

``` r
## Error in eval(expr, envir, enclos): cannot allocate vector of size xx Mb.
```

This means data cannot fit into the memory. In general there are several
approaches that you can take to address this issue.

### Use a fast binary data storage format that enables reading data subsets

CSV and other text-based formats have the advantage of being both human
and machine-readable. However, they are an inefficient way to store
data, and loading them into memory requires a time-consuming parsing
process to separate fields and records.

Structured formats, like binary formats, have the advantage of being
more space-efficient on disk and faster to read. They often employ
advanced compression techniques, store metadata, and allow fast,
selective access to data subsets. These substantial advantages come at
the cost of human readability; you cannot easily inspect the contents of
binary data files directly. If you are concerned with reducing memory
use or data processing time, this is probably a trade-off you are happy
to make.

The Parquet binary storage format is among the best currently available.
Support in R is provided by the Arrow package.

### Partition the data on disk to facilitate chunked access and computation

Memory requirements can be reduced by partitioning the data and
computation into chunks, processing each one sequentially, and combining
the results at the end. It is common practice to partition the data on
disk storage to make this computational strategy more natural and
efficient. For instance, the taxi data is already partitioned by year
and month.

### Only read in the data you need

f we think carefully about it, we’ll see that our previous attempt to
process the taxi data by reading in all the data at once was wasteful.
Not all rows represent Lyft rides, and the only column we really need is
the one that tells us if the ride was operated by Lyft or not. We can
perform the necessary computation by reading in only that one column and
only the rows for which the `hvfhs_license_num` column is equal to
`HV0005` (Lyft).

### Use streaming data tools and algorithms

It’s all fine and good to say “only read the data you need”, but how do
you actually do that? Unless you have full control over the data
collection and storage process, chances are good that your data provider
included a bunch of stuff you don’t need. The key is to find a data
selection and filtering tool that works in a streaming fashion so that
you can access subsets without ever loading data you don’t need into
memory. Both the arrow and duckdb R packages support this type of
workflow and can dramatically reduce the time and hardware requirements
for many computations.

Moreover, processing data in a streaming fashion without needing to load
it into memory is a general technique that can be applied to other tasks
as well. For example the duckdb package allows you to carry out data
aggregation in a streaming fashion, meaning that you can compute summary
statistics for data that is too large to fit in memory.

### Avoid unnecessarily storing or duplicating data in memory

It is also important to pay some attention to storing and processing
data efficiently once we have it loaded in memory. R likes to make
copies of the data, and while it does try to avoid unnecessary
duplication this process can be unpredictable. At a minimum you can
remove or avoid storing intermediate results you don’t need and take
care not to make copies of your data structures unless you have to. The
data.table package additionally makes it easier to efficiently modify R
data objects in-place, reducing the risk of accidentally or unknowingly
duplicating large data structures.

## Solution 1: Using `data.table` with csv file

The `data.table` package can be used to selectively read only the
necessary column(s) instead of loading the entire dataset. This feature
can be leveraged to load only the required subset of data for analysis,
improving efficiency.

``` r
library(data.table)
st <- proc.time()

count_lyft_rides <- function(file) {
  # Read CSV file using fread from data.table package
  dt <- fread(file, select = c("hvfhs_license_num"))

  # Filter the rows where hvfhs_license_num is equal to "HV0005" (Lyft)
  lyft_rides <- dt[hvfhs_license_num == "HV0005"]

  # Return the number of Lyft rides
  return(nrow(lyft_rides))
}

# List all CSV files in the folder
csv_files <- list.files("data/original_csv", recursive = TRUE, full.names = TRUE)

# Count Lyft rides in each CSV file using the count_lyft_rides function
lyft_rides_counts <- lapply(csv_files, count_lyft_rides)

# Sum the counts to get the total number of Lyft rides
total_lyft_rides <- sum(unlist(lyft_rides_counts))

# Print the total number of Lyft rides
print(total_lyft_rides)
```

    ## [1] 37250101

``` r
et <- proc.time()
wc_1 <- (et - st)[[3]]

wc_df <- data.frame(name = "csv + data.table", wc = wc_1)
```

Wall clock time with the Solution 1: 56.722 seconds.

## Solution 2: Using `data.table` with csv file in Parallel

By combining Solution 1 with the parallel capabilities of the R
language, we can enhance the efficiency of the process. To do this we
can use the `parallel` internal R package.

``` r
library(data.table)
library(parallel)

count_lyft_rides <- function(file) {
  # Read CSV file using fread from data.table package
  dt <- data.table::fread(file, select = c("hvfhs_license_num"))

  # Filter the rows where hvfhs_license_num is equal to "HV0005" (Lyft)
  lyft_rides <- dt[hvfhs_license_num == "HV0005"]

  # Return the number of Lyft rides
  return(nrow(lyft_rides))
}

st <- proc.time()

# Get the number of available cores
num_cores <- detectCores()

# Create a parallel cluster with the available cores
cl <- makeCluster(num_cores)

# Export the count_lyft_rides function to the cluster
clusterExport(cl, "count_lyft_rides")

# List all CSV files in the folder
csv_files <- list.files("data/original_csv", recursive = TRUE, full.names = TRUE)

# Count Lyft rides in each CSV file using the count_lyft_rides function and parallel processing
lyft_rides_counts <- parLapply(cl, csv_files, count_lyft_rides)

# Sum the counts to get the total number of Lyft rides
total_lyft_rides <- sum(unlist(lyft_rides_counts))

stopCluster(cl)

# Print the total number of Lyft rides
print(total_lyft_rides)
```

    ## [1] 37250101

``` r
et <- proc.time()
wc_2 <- (et - st)[[3]]
proc_name <- paste0("csv + data.table + parallel(", num_cores, " cores)")
wc_df <- rbind(wc_df, data.frame(name = proc_name, wc = wc_2))
```

Wall clock time with the Solution 2: 17.246 seconds.

## Solution 3: Convert .csv to parquet

Converting unstructured `.csv` file into structured binary `parquet`
file can be done by the `arrow` package. This is a one-time conversion
that allows faster read and efficient memory management.

``` r
library(arrow)
```

    ## 
    ## Attaching package: 'arrow'

    ## The following object is masked from 'package:utils':
    ## 
    ##     timestamp

``` r
if(!dir.exists("data/converted_parquet")) {
  
  dir.create("data/converted_parquet")
  
  ## this doesn't yet read the data in, it only creates a connection
  csv_ds <- open_dataset("data/original_csv", 
                         format = "csv",
                         partitioning = c("year", "month"))
  
  ## this reads each csv file in the csv_ds dataset and converts it to a .parquet file
  write_dataset(csv_ds, 
                "data/converted_parquet", 
                format = "parquet",
                partitioning = c("year", "month"))
}
```

The partitioning that is used here is called “hive-style” partitioning,
i.e., including both the variable names and values in the directory
names. `arrow` automatically recognize the partitions.

We can look at the converted files and compare the naming scheme and
storage requirements to the original CSV data.

``` r
fhvhv_csv_files <- list.files("data/original_csv", recursive=TRUE, full.names = TRUE)
fhvhv_files <- list.files("data/converted_parquet", full.names = TRUE, recursive = TRUE)

data.frame(csv_file = fhvhv_csv_files, 
           parquet_file = fhvhv_files, 
           csv_size_Mb = file.size(fhvhv_csv_files) / 1024^2, 
           parquet_size_Mb = file.size(fhvhv_files) / 1024^2)
```

    ##                                                csv_file
    ## 1  data/original_csv/2020/01/fhvhv_tripdata_2020-01.csv
    ## 2  data/original_csv/2020/02/fhvhv_tripdata_2020-02.csv
    ## 3  data/original_csv/2020/03/fhvhv_tripdata_2020-03.csv
    ## 4  data/original_csv/2020/04/fhvhv_tripdata_2020-04.csv
    ## 5  data/original_csv/2020/05/fhvhv_tripdata_2020-05.csv
    ## 6  data/original_csv/2020/06/fhvhv_tripdata_2020-06.csv
    ## 7  data/original_csv/2020/07/fhvhv_tripdata_2020-07.csv
    ## 8  data/original_csv/2020/08/fhvhv_tripdata_2020-08.csv
    ## 9  data/original_csv/2020/09/fhvhv_tripdata_2020-09.csv
    ## 10 data/original_csv/2020/10/fhvhv_tripdata_2020-10.csv
    ## 11 data/original_csv/2020/11/fhvhv_tripdata_2020-11.csv
    ## 12 data/original_csv/2020/12/fhvhv_tripdata_2020-12.csv
    ##                                                parquet_file csv_size_Mb
    ## 1   data/converted_parquet/year=2020/month=1/part-0.parquet   1243.4975
    ## 2  data/converted_parquet/year=2020/month=10/part-0.parquet   1313.2442
    ## 3  data/converted_parquet/year=2020/month=11/part-0.parquet    808.5597
    ## 4  data/converted_parquet/year=2020/month=12/part-0.parquet    259.5806
    ## 5   data/converted_parquet/year=2020/month=2/part-0.parquet    366.5430
    ## 6   data/converted_parquet/year=2020/month=3/part-0.parquet    454.5977
    ## 7   data/converted_parquet/year=2020/month=4/part-0.parquet    599.2560
    ## 8   data/converted_parquet/year=2020/month=5/part-0.parquet    667.6880
    ## 9   data/converted_parquet/year=2020/month=6/part-0.parquet    728.5463
    ## 10  data/converted_parquet/year=2020/month=7/part-0.parquet    798.4743
    ## 11  data/converted_parquet/year=2020/month=8/part-0.parquet    698.0638
    ## 12  data/converted_parquet/year=2020/month=9/part-0.parquet    700.6804
    ##    parquet_size_Mb
    ## 1        190.26387
    ## 2        125.17837
    ## 3        110.92144
    ## 4        111.67697
    ## 5        198.87074
    ## 6        127.53637
    ## 7         48.32047
    ## 8         64.17768
    ## 9         76.45972
    ## 10        97.99151
    ## 11       107.80694
    ## 12       115.25221

As expected, the binary parquet storage format is much more compact than
the text-based CSV format. Now, let’s compare time of reading the data.

``` r
## tidyverse csv reader
system.time(invisible(readr::read_csv(fhvhv_csv_files[[1]])))
```

    ## Rows: 20569325 Columns: 7
    ## ── Column specification ────────────────────────────────────────────────────────
    ## Delimiter: ","
    ## chr  (2): hvfhs_license_num, dispatching_base_num
    ## dbl  (3): PULocationID, DOLocationID, SR_Flag
    ## dttm (2): pickup_datetime, dropoff_datetime
    ## 
    ## ℹ Use `spec()` to retrieve the full column specification for this data.
    ## ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.

    ##    user  system elapsed 
    ##  51.095   2.307   9.634

``` r
## arrow package parquet reader
system.time(invisible(read_parquet(fhvhv_files[[1]])))
```

    ##    user  system elapsed 
    ##   3.099   1.319   1.194

### Read and count Lyft records with arrow

The arrow package makes it easy to read and process only the data we
need for a particular calculation. It allows us to use the partitioned
data directories we created earlier as a single dataset and to query it
using the dplyr verbs many R users are already familiar with.

Start by creating a dataset representation from the partitioned data
directory:

``` r
st <- proc.time()
fhvhv_ds <- open_dataset("data/converted_parquet",
                         schema = schema(hvfhs_license_num=string(),
                                         dispatching_base_num=string(),
                                         pickup_datetime=string(),
                                         dropoff_datetime=string(),
                                         PULocationID=int64(),
                                         DOLocationID=int64(),
                                         SR_Flag=int64(),
                                         year=int32(),
                                         month=int32()))

et <- proc.time()
wc_opening_db <- (et - st)[[3]]
```

Wall clock time to open database: 0.0489999999999999 seconds.

Because we have hive-style directory names open_dataset automatically
recognizes the partitions.

Importantly, open_dataset doesn’t actually read the data into memory. It
just opens a connection to the dataset and makes it easy for us to query
it. Finally, we can compute the number of NYC Lyft trips in 2020, even
on a machine with limited memory:

``` r
library(dplyr, warn.conflicts = FALSE)

st <- proc.time()

fhvhv_ds %>%
  filter(hvfhs_license_num == "HV0005") %>%
  select(hvfhs_license_num) %>%
  collect() %>%
  summarize(total_Lyft_trips = n())
```

    ## # A tibble: 1 × 1
    ##   total_Lyft_trips
    ##              <int>
    ## 1         37250101

``` r
et <- proc.time()
wc_3 <- (et - st)[[3]]

wc_df <- rbind(wc_df, data.frame(name = "arrow + parquet", wc = wc_3))
```

Wall clock time with the Solution 3: 1.291 seconds.

Note that arrow datasets do not support summarize natively, that is why
we call collect first to actually read in the data.

The arrow package makes it fast and easy to query on-disk data and read
in only the fields and records needed for a particular computation. This
is a tremendous improvement over the typical R workflow, and may well be
all you need to start using your large datasets more quickly and
conveniently, even on modest hardware.

## Solution 4: Streaming data with duckdb

If you need even more speed and convenience you can use the duckdb
package. It allows you to query the same parquet datasets partitioned on
disk as we did above. You can use either SQL statements via the DBI
package or tidyverse style verbs using dbplyr. Let’s see how it works.

First we create a duckdb table from our arrow dataset.

``` r
library(duckdb)
```

    ## Loading required package: DBI

``` r
library(dplyr)

con <- DBI::dbConnect(duckdb::duckdb())
fhvhv_tbl <- to_duckdb(fhvhv_ds, con, "fhvhv")
```

The duckdb table can be queried using tidyverse style verbs or SQL.

``` r
## number of Lyft trips, tidyverse style
st <- proc.time()

fhvhv_tbl %>%
  filter(hvfhs_license_num == "HV0005") %>%
  select(hvfhs_license_num) %>%
  count()
```

    ## # Source:   SQL [1 x 1]
    ## # Database: DuckDB 0.7.1 [root@Darwin 20.3.0:R 4.2.1/:memory:]
    ##          n
    ##      <dbl>
    ## 1 37250101

``` r
et <- proc.time()
wc_4 <- (et - st)[[3]]
wc_df <- rbind(wc_df, data.frame(name = "dplyr + duckdb", wc = wc_4))

dbDisconnect(con, shutdown=TRUE)
```

Wall clock time with the Solution 4: 1.801 seconds.

The `duckdb` package supports aggregating data in a streaming fashion,
allows you to set memory limits, and is optimized for speed. The way I
think about the relationship between arrow and duckdb is that arrow is
primarily about reading and writing data as fast and efficiently as
possible, with some built-in analysis capabilities, while duckdb is a
database engine with more complete data manipulation and aggregation
capabilities.

``` r
library(ggplot2)

wc_df <- wc_df[order(as.numeric(wc_df$wc)), ]
rownames(wc_df) <- NULL
wc_df$name <- factor(wc_df$name, levels = wc_df$name)

# Define the plot
ggplot(wc_df, aes(x = wc, y = name)) + 
  geom_bar(stat = "identity", fill = "lightcoral", width = 0.5) +
  xlab("Wall Clock Time (Seconds)") + 
  ylab("Method") + 
  ggtitle("Processing Time by Method") +
  theme(plot.title = element_text(hjust = 0.5, size = 14),
        axis.text.y = element_text(size = 10))
```

![](NYC_lyft_files/figure-gfm/unnamed-chunk-16-1.png)<!-- -->

``` r
performance_imp <- wc_df$wc[4] / wc_df$wc 
wc_df <- cbind(wc_df, performance_imp)
wc_df
```

    ##                                    name     wc performance_imp
    ## 1                       arrow + parquet  1.291       43.936483
    ## 2                        dplyr + duckdb  1.801       31.494725
    ## 3 csv + data.table + parallel(12 cores) 17.246        3.288995
    ## 4                      csv + data.table 56.722        1.000000

In summary, we explored various methods to handle large data in R.
First; we discussed the traditional approach of loading all the data at
once, which can be inefficient and time-consuming. Next, we looked at
the `data.table` package’s capability to read a single column instead of
loading the entire dataset. We also discussed the benefits of using
binary storage formats such as Parquet to improve processing speed and
reduce disk space usage. Finally, we looked at parallel computing and
streaming processing as methods to handle large datasets more
efficiently. By employing these various techniques, we can process and
analyze large datasets with greater efficiency and accuracy in R.
