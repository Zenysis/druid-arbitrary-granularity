## Druid Arbitrary Granularity Extension
The arbitrary granularity extension is used for running [Druid](http://druid.io/)
queries over custom time intervals. Currently, Druid requires a user to
aggregate over fixed time windows (month, day, 36 hours, etc.). While easy to
understand, this approach is fairly inflexible. This extension hopes to solve
that problem.


### Installation
You can either use the prebuilt jar located in `dist/` or build from source
using the directions below.

Once you have the compiled `jar`, copy it to your druid installation and
follow the
[including extension](http://druid.io/docs/latest/operations/including-extensions.html)
druid documentation. You should end up adding a line similar to this to your
`common.runtime.properties` file:

`druid.extensions.loadList=["druid-arbitrary-granularity"]`

##### Building from source
Clone the druid repo and add this line to `pom.xml` in the "Community extensions"
section:

```xml
<module>${druid-arbitrary-granularity-src-root}/arbitrary-granularity</module>
```
replacing `${druid-arbitrary-granularity-src-root}` with your path to this
repo.

Then, inside the druid repo, run:
`mvn package -DskipTests=true -rf :druid-arbitrary-granularity`
This will build the arbitrary granularity extension and place it in
`${druid-arbitrary-granularity-src-root}/arbitrary-granularity/target`


### Use
The arbitrary granularity takes an array of time intervals which will define
the buckets used during querying. It uses the same `intervals` format as the
normal query interval.
```javascript
{
  ...
  "granularity": {
    "type": "arbitrary",
    "intervals": <Array of time intervals>
  },
  ...
}
```

Only events that fall in one of the defined intervals will be added to the
bucket. If the query interval is longer than any of the granularity intervals,
events outside the granularity intervals (but within the query interval) will
be dropped. If the query interval partially covers one of the granularity
intervals, only the data that is both within the query interval AND within the
granularity interval will be included.

### Example

##### Quarters that do not start at the beginning of the calendar year
Often, a business's fiscal year does not align with a calendar year.
Grouping by the builtin `quarter` would not work since it starts with
Jan 1, and using the builtin `period` granularity won't work since
each quarter is not the same length. The arbitrary granularity extension
can fix this:

Quarter start: October 1, 2015
```javascript
{
  ...
  "intervals": [
    "2015-10-01/2016-01-01",
    "2016-01-01/2016-04-01",
    "2016-04-01/2016-07-01",
    "2016-07-01/2016-10-01",
  ],
  "granularity": {
    "type": "arbitrary",
    "intervals": [
      "2015-10-01/2016-01-01",
      "2016-01-01/2016-04-01",
      "2016-04-01/2016-07-01",
      "2016-07-01/2016-10-01",
    ]
  },
  ...
}
```

##### Weekday vs Weekend
Month: October 2016
```javascript
{
  ...
  "intervals": [
    "2016-10-01/2016-11-05",
  ],
  "granularity": {
    "type": "arbitrary",
    "intervals": [
      "2016-10-01/2016-10-03",
      "2016-10-03/2016-10-08",
      "2016-10-08/2016-10-10",
      "2016-10-10/2016-10-15",
      "2016-10-15/2016-10-17",
      "2016-10-17/2016-10-22",
      "2016-10-22/2016-10-24",
      "2016-10-24/2016-10-29",
      "2016-10-29/2016-10-31",
      "2016-10-31/2016-11-05",
    ]
  },
  ...
}
```
