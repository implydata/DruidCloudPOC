# Chicago Taxi Data

## What is it?

Contains a large volume of Taxi Data for Chicago.

See https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew for the metadata.

## Source

Download the data set from:

* https://data.cityofchicago.org/api/views/wrvz-psew/rows.csv?accessType=DOWNLOAD

Note that you may need to split the CSV files up using a command like this:

```
something something something darkside
```

## Specification

### Timestamp

```
"timestampSpec": {
        "column": "Trip End Timestamp",
        "format": "MM/dd/yyyy HH:mm:ss aa"
      }
```

### Transforms

```
"transformSpec": {
        "filter": null,
        "transforms": [
          {
            "type": "expression",
            "name": "__time_start_long",
            "expression": "timestamp_parse(\"Trip End Timestamp\",'MM/dd/yyyy hh:mm:ss a')"
          }
        ]
      }
```

### Dimensions

```
"dimensionsSpec": {
        "dimensions": [
          {
            "type": "string",
            "name": "Company",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Payment Type",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "string",
            "name": "Dropoff Census Tract",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "long",
            "name": "Pickup Census Tract",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "string",
            "name": "Dropoff Community Area",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          },
          {
            "type": "long",
            "name": "Pickup Community Area",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "float",
            "name": "Extras",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "float",
            "name": "Fare",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "float",
            "name": "Tips",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "float",
            "name": "Tolls",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "float",
            "name": "Trip Miles",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "float",
            "name": "Trip Seconds",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "long",
            "name": "Trip Total",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "long",
            "name": "__time_start_long",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": false
          },
          {
            "type": "string",
            "name": "Taxi ID",
            "multiValueHandling": "SORTED_ARRAY",
            "createBitmapIndex": true
          }
        ]
      },
```