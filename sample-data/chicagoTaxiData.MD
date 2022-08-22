# Chicago Taxi Data

Contains a large volume of Taxi Data for Chicago.

See https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew for the metadata.

## Download

Download the data set from:

* https://data.cityofchicago.org/api/views/wrvz-psew/rows.csv?accessType=DOWNLOAD

Note that you may need to split the CSV files up using a command like this:

```
something something something darkside
```

## Ingesting the data

### What's in the data?

There are two timestamps to choose from - see [Timestamp](#timestamp) below.

#### Attributes

Attributes describe the taxi:

* Taxi Id
* Company

And the journey:

* Payment Type
* Dropoff Census Tract
* Pickup Census Tract
* Dropoff Community Area
* Pickup Community Area

#### Measures

Some measures for each trip are financial:

* Fare
* Extras
* Tips
* Tolls
* Trip Total

And others are about the trip itself:

* Trip Miles
* Trip Seconds

### Timestamps

The data contains a `Trip Start Timestamp` that you can use for `__time`.

Alternatively, apply the RADStack paper principle that event data is "about [when the event occured](http://static.druid.io/docs/radstack.pdf)" by using the `End Timestamp` as your `__time` field value, as follows:

```
"timestampSpec": {
        "column": "Trip End Timestamp",
        "format": "MM/dd/yyyy HH:mm:ss aa"
      }
```

### Dimensions

Ingest all the measures as `float` - all the rest can be kept as `string`.

### Transforms

As noted in [Timestamp](#timestamp) above, the dataset contains two timestamps.  Whichever you decide to use, remember to include the resulting field of your transformation as a `long` in the `dimensionsSpec`.

If using the `Trip Start Timestamp` for `__time`, use the transformation below to convert the `Trip End Timestamp` into a `long`.

```
"transformSpec": {
        "filter": null,
        "transforms": [
          {
            "type": "expression",
            "name": "__time_end_long",
            "expression": "timestamp_parse(\"Trip End Timestamp\",'MM/dd/yyyy hh:mm:ss a')"
          }
        ]
      }
```

Alternatively, use the following to do the same but to the `Trip Start Timestamp`:

```
"transformSpec": {
        "filter": null,
        "transforms": [
          {
            "type": "expression",
            "name": "__time_start_long",
            "expression": "timestamp_parse(\"Trip Start Timestamp\",'MM/dd/yyyy hh:mm:ss a')"
          }
        ]
      }
```

It's also possible to dereference the codes in `Community Area` to actual names using the Community Area Lookup at ingestion time.