# Chicago Taxi Data

Contains a large volume of Taxi Data for Chicago.

See https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew for the metadata.

## Download

Download the data set from:

* https://data.cityofchicago.org/api/views/wrvz-psew/rows.csv?accessType=DOWNLOAD

Note that you may need to split the CSV files up using a command like this, which will create files of `-l` size out of the `splitme.csv` file.

```
#!/bin/bash
echo Splitting splitme.csv
echo Generating header...
head -n 1 splitme.csv > header.csv
echo Removing old splits...
rm part-*.csv
rm part-*
echo Splitting files...
split -a 4 -l 500000 splitme.csv part-
echo Creating new CSV files...
for i in part-*
do
  echo Working on $i...
  cp header.csv working.csv
  cat "$i" >> working.csv
  mv working.csv "$i.csv"
  rm "$i"
done
echo Done.
```

## Ingesting the data

### What's in the data?

There are two timestamps to choose from - see [Timestamps](#timestamps) below.

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

As noted in [Timestamps](#timestamps) above, the dataset contains two timestamps.  Whichever you decide to use, remember to include the resulting field of your transformation as a `long` in the `dimensionsSpec`.

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

Here is the `lookup` map to create in the Server lookups UI:

```
{
  "type": "map",
  "map": {
    "1": "ROGERS PARK",
    "2": "WEST RIDGE",
    "3": "UPTOWN",
    "4": "LINCOLN SQUARE",
    "5": "NORTH CENTER",
    "6": "LAKE VIEW",
    "7": "LINCOLN PARK",
    "8": "NEAR NORTH SIDE",
    "9": "EDISON PARK",
    "10": "NORWOOD PARK",
    "11": "JEFFERSON PARK",
    "12": "FOREST GLEN",
    "13": "NORTH PARK",
    "14": "ALBANY PARK",
    "15": "PORTAGE PARK",
    "16": "IRVING PARK",
    "17": "DUNNING",
    "18": "MONTCLARE",
    "19": "BELMONT CRAGIN",
    "20": "HERMOSA",
    "21": "AVONDALE",
    "22": "LOGAN SQUARE",
    "23": "HUMBOLDT PARK",
    "24": "WEST TOWN",
    "25": "AUSTIN",
    "26": "WEST GARFIELD PARK",
    "27": "EAST GARFIELD PARK",
    "28": "NEAR WEST SIDE",
    "29": "NORTH LAWNDALE",
    "30": "SOUTH LAWNDALE",
    "31": "LOWER WEST SIDE",
    "32": "LOOP",
    "33": "NEAR SOUTH SIDE",
    "34": "ARMOUR SQUARE",
    "35": "DOUGLAS",
    "36": "OAKLAND",
    "37": "FULLER PARK",
    "38": "GRAND BOULEVARD",
    "39": "KENWOOD",
    "40": "WASHINGTON PARK",
    "41": "HYDE PARK",
    "42": "WOODLAWN",
    "43": "SOUTH SHORE",
    "44": "CHATHAM",
    "45": "AVALON PARK",
    "46": "SOUTH CHICAGO",
    "47": "BURNSIDE",
    "48": "CALUMET HEIGHTS",
    "49": "ROSELAND",
    "50": "PULLMAN",
    "51": "SOUTH DEERING",
    "52": "EAST SIDE",
    "53": "WEST PULLMAN",
    "54": "RIVERDALE",
    "55": "HEGEWISCH",
    "56": "GARFIELD RIDGE",
    "57": "ARCHER HEIGHTS",
    "58": "BRIGHTON PARK",
    "59": "MCKINLEY PARK",
    "60": "BRIDGEPORT",
    "61": "NEW CITY",
    "62": "WEST ELSDON",
    "63": "GAGE PARK",
    "64": "CLEARING",
    "65": "WEST LAWN",
    "66": "CHICAGO LAWN",
    "67": "WEST ENGLEWOOD",
    "68": "ENGLEWOOD",
    "69": "GREATER GRAND CROSSING",
    "70": "ASHBURN",
    "71": "AUBURN GRESHAM",
    "72": "BEVERLY",
    "73": "WASHINGTON HEIGHTS",
    "74": "MOUNT GREENWOOD",
    "75": "MORGAN PARK",
    "76": "OHARE",
    "77": "EDGEWATER"
  }
}
```