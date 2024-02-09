# SparkORM ✨

[![PyPI version](https://badge.fury.io/py/SparkORM.svg)](https://badge.fury.io/py/SparkORM)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Python Spark SQL & DataFrame schema management and basic Object Relational Mapping.

## Why use SparkORM

`SparkORM` takes the pain out of working with DataFrame schemas in PySpark.
It makes schema definition more Pythonic. And it's
particularly useful you're dealing with structured data.

In plain old PySpark, you might find that you write schemas
[like this](https://github.com/asuiu/SparkORM/tree/master/examples/conferences_comparison/plain_schema.py):

```python
CITY_SCHEMA = StructType()
CITY_NAME_FIELD = "name"
CITY_SCHEMA.add(StructField(CITY_NAME_FIELD, StringType(), False))
CITY_LAT_FIELD = "latitude"
CITY_SCHEMA.add(StructField(CITY_LAT_FIELD, FloatType()))
CITY_LONG_FIELD = "longitude"
CITY_SCHEMA.add(StructField(CITY_LONG_FIELD, FloatType()))

CONFERENCE_SCHEMA = StructType()
CONF_NAME_FIELD = "name"
CONFERENCE_SCHEMA.add(StructField(CONF_NAME_FIELD, StringType(), False))
CONF_CITY_FIELD = "city"
CONFERENCE_SCHEMA.add(StructField(CONF_CITY_FIELD, CITY_SCHEMA))
```

And then plain old PySpark makes you deal with nested fields like this:

```python
dframe.withColumn("city_name", df[CONF_CITY_FIELD][CITY_NAME_FIELD])
```

Instead, with `SparkORM`, schemas become a lot
[more literate](https://github.com/asuiu/SparkORM/tree/master/examples/conferences_comparison/sparkorm_schema.py):

```python
class City(Struct):
    name = String()
    latitude = Float()
    longitude = Float()
    date_created = Date()

class Conference(TableModel):
    class Meta:
        name = "conference_table"
    name = String(nullable=False)
    city = City()

class LocalConferenceView(ViewModel):
    class Meta:
        name = "city_table"

Conference(spark).create()

Conference(spark).ensure_exists()  # Creates the table, and if it already exists - validates the scheme and throws an exception if it doesn't match

LocalConferenceView(spark).create_or_replace(select_statement=f"SELECT * FROM {Conference.get_name()}")

Conference(spark).insert([("Bucharest", 44.4268, 26.1025, date(2020, 1, 1))])

Conference(spark).drop()
```

As does dealing with nested fields:

```python
dframe.withColumn("city_name", Conference.city.name.COL)
```

Here's a summary of `SparkORM`'s features.

- ORM-like class-based Spark schema definitions.
- Automated field naming: The attribute name of a field as it appears
  in its `Struct` is (by default) used as its field name. This name can
  be optionally overridden.
- Programatically reference nested fields in your structs with the
  `PATH` and `COL` special properties. Avoid hand-constructing strings
  (or `Column`s) to reference your nested fields.
- Validate that a DataFrame matches a `SparkORM` schema.
- Reuse and build composite schemas with `inheritance`, `includes`, and
  `implements`.
- Get a human-readable Spark schema representation with `pretty_schema`.
- Create an instance of a schema as a dictionary, with validation of
  the input values.

Read on for documentation on these features.

## Defining a schema

Each Spark atomic type has a counterpart `SparkORM` field:

| PySpark type | `SparkORM` field |
|---|---|
| `ByteType` | `Byte` |
| `IntegerType` | `Integer` |
| `LongType` | `Long` |
| `ShortType` | `Short` |
| `DecimalType` | `Decimal` |
| `DoubleType` | `Double` |
| `FloatType` | `Float` |
| `StringType` | `String` |
| `BinaryType` | `Binary` |
| `BooleanType` | `Boolean` |
| `DateType` | `Date` |
| `TimestampType` | `Timestamp` |

`Array` (counterpart to `ArrayType` in PySpark) allows the definition
of arrays of objects. By creating a subclass of `Struct`, we can
define a custom class that will be converted to a `StructType`.

For
[example](https://github.com/asuiu/SparkORM/tree/master/examples/arrays/arrays.py),
given the `SparkORM` schema definition:

```python
from SparkORM import TableModel, String, Array

class Article(TableModel):
    title = String(nullable=False)
    tags = Array(String(), nullable=False)
    comments = Array(String(nullable=False))
```

Then we can build the equivalent PySpark schema (a `StructType`)
with:

```python

pyspark_struct = Article.get_schema()
```

Pretty printing the schema with the expression
`SparkORM.pretty_schema(pyspark_struct)` will give the following:

```text
StructType([
    StructField('title', StringType(), False),
    StructField('tags',
        ArrayType(StringType(), True),
        False),
    StructField('comments',
        ArrayType(StringType(), False),
        True)])
```

## Features

Many examples of how to use `SparkORM` can be found in
[`examples`](https://github.com/asuiu/SparkORM/tree/master/examples).
### ORM-like class-based schema definitions
The `SparkORM` table schema definition is based on classes. Each column is a class and accepts a number of arguments that will be used to generate the schema.

The following arguments are supported:
- `nullable` - if the column is nullable or not (default: `True`)
- `name` - the name of the column (default: the name of the attribute)
- `comment` - the comment of the column (default: `None`)
- `auto_increment` - if the column is auto incremented or not (default: `False`) Note: applicable only for `Long` columns
- `sql_modifiers` - the SQL modifiers of the column (default: `None`)
- `partitioned_by` - if the column is partitioned by or not (default: `False`)

Examples:
```python
class City(TableModel):
    name = String(nullable=False)
    latitude = Long(auto_increment=True) # auto_increment is a special property that will generate a unique value for each row
    longitude = Float(comment="Some comment")
    date_created = Date(sql_modifiers="GENERATED ALWAYS AS (CAST(birthDate AS DATE))") # sql_modifiers will be added to the CREATE clause for the column
    birthDate = Date(nullable=False, partitioned_by=True) # partitioned_by is a special property that will generate a partitioned_by clause for the column
```

### Automated field naming

By default, field names are inferred from the attribute name in the
struct they are declared.

For example, given the struct

```python
class Geolocation(TableModel):
    latitude = Float()
    longitude = Float()
```

the concrete name of the `Geolocation.latitude` field is `latitude`.

Names also be overridden by explicitly specifying the field name as an
argument to the field

```python
class Geolocation(TableModel):
    latitude = Float(name="lat")
    longitude = Float(name="lon")
```

which would mean the concrete name of the `Geolocation.latitude` field
is `lat`.

### Field paths and nested objects

Referencing fields in nested data can be a chore. `SparkORM` simplifies this
with path referencing.

[For example](https://github.com/asuiu/SparkORM/tree/master/examples/nested_objects/SparkORM_example.py), if we have a
schema with nested objects:

```python
class Address(Struct):
    post_code = String()
    city = String()


class User(Struct):
    username = String(nullable=False)
    address = Address()


class Comment(Struct):
    message = String()
    author = User(nullable=False)


class Article(TableModel):
    title = String(nullable=False)
    author = User(nullable=False)
    comments = Array(Comment())
```

We can use the special `PATH` property to turn a path into a
Spark-understandable string:

```python
author_city_str = Article.author.address.city.PATH
"author.address.city"
```

`COL` is a counterpart to `PATH` that returns a Spark `Column`
object for the path, allowing it to be used in all places where Spark
requires a column.

Function equivalents `path_str`, `path_col`, and `name` are also available.
This table demonstrates the equivalence of the property styles and the function
styles:

| Property style | Function style | Result (both styles are equivalent) |
| --- | --- | --- |
| `Article.author.address.city.PATH` | `SparkORM.path_str(Article.author.address.city)` | `"author.address.city"` |
| `Article.author.address.city.COL` | `SparkORM.path_col(Article.author.address.city)` | `Column` pointing to `author.address.city` |
| `Article.author.address.city.NAME` | `SparkORM.name(Article.author.address.city)` | `"city"` |

For paths that include an array, two approaches are provided:

```python
comment_usernames_str = Article.comments.e.author.username.PATH
"comments.author.username"

comment_usernames_str = Article.comments.author.username.PATH
"comments.author.username"
```

Both give the same result. However, the former (`e`) is more
type-oriented. The `e` attribute corresponds to the array's element
field. Although this looks strange at first, it has the advantage of
being inspectable by IDEs and other tools, allowing goodness such as
IDE auto-completion, automated refactoring, and identifying errors
before runtime.

### Field metadata

Field [metadata](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructField.html) can be specified with the `metadata` argument to a field, which accepts a dictionary
of key-value pairs.

```python
class Article(TableModel):
    title = String(nullable=False,
                   metadata={"description": "The title of the article", "max_length": 100})
```

The metadata can be accessed with the `METADATA` property of the field:

```python
Article.title.METADATA
{"description": "The title of the article", "max_length": 100}
```

### DataFrame validation

Struct method `validate_data_frame` will verify if a given DataFrame's
schema matches the Struct.
[For example](https://github.com/asuiu/SparkORM/tree/master/examples/validation/test_validation.py),
if we have our `Article`
struct and a DataFrame we want to ensure adheres to the `Article`
schema:

```python
dframe = spark_session.createDataFrame([{"title": "abc"}])

class Article(TableModel):
    title = String()
    body = String()
```

Then we can can validate with:

```python
validation_result = Article.validate_data_frame(dframe)
```

`validation_result.is_valid` indicates whether the DataFrame is valid
(`False` in this case), and `validation_result.report` is a
human-readable string describing the differences:

```text
Struct schema...

StructType([
    StructField('title', StringType(), True),
    StructField('body', StringType(), True)])

DataFrame schema...

StructType([
    StructField('title', StringType(), True)])

Diff of struct -> data frame...

  StructType([
-     StructField('title', StringType(), True)])
+     StructField('title', StringType(), True),
+     StructField('body', StringType(), True)])
```

For convenience,

```python
Article.validate_data_frame(dframe).raise_on_invalid()
```

will raise a `InvalidDataFrameError` (see `SparkORM.exceptions`) if the
DataFrame is not valid.

### Creating an instance of a schema

`SparkORM` simplifies the process of creating an instance of a struct.
You might need to do this, for example, when creating test data, or
when creating an object (a dict or a row) to return from a UDF.

Use `Struct.make_dict(...)` to instantiate a struct as a dictionary.
This has the advantage that the input values will be correctly
validated, and it will convert schema property names into their
underlying field names.

For
[example](https://github.com/asuiu/SparkORM/tree/master/examples/struct_instantiation/instantiate_as_dict.py),
given some simple Structs:

```python
class User(TableModel):
    id = Integer(name="user_id", nullable=False)
    username = String()

class Article(TableModel):
    id = Integer(name="article_id", nullable=False)
    title = String()
    author = User()
    text = String(name="body")
```

Here are a few examples of creating dicts from `Article`:

```python
Article.make_dict(
    id=1001,
    title="The article title",
    author=User.make_dict(
        id=440,
        username="user"
    ),
    text="Lorem ipsum article text lorem ipsum."
)

# generates...
{
    "article_id": 1001,
    "author": {
        "user_id": 440,
        "username": "user"},
    "body": "Lorem ipsum article text lorem ipsum.",
    "title": "The article title"
}
```

```python
Article.make_dict(
    id=1002
)

# generates...
{
    "article_id": 1002,
    "author": None,
    "body": None,
    "title": None
}
```

See
[this example](https://github.com/asuiu/SparkORM/tree/master/examples/conferences_extended/conferences.py)
for an extended example of using `make_dict`.

### Composite schemas

It is sometimes useful to be able to re-use the fields of one struct
in another struct. `SparkORM` provides a few features to enable this:

- _inheritance_: A subclass inherits the fields of a base struct class.
- _includes_: Incorporate fields from another struct.
- _implements_: Enforce that a struct must implement the fields of
  another struct.

See the following examples for a better explanation.

#### Using inheritance

For [example](https://github.com/asuiu/SparkORM/tree/master/examples/composite_schemas/inheritance.py), the following:

```python
class BaseEvent(TableModel):
    correlation_id = String(nullable=False)
    event_time = Timestamp(nullable=False)

class RegistrationEvent(BaseEvent):
    user_id = String(nullable=False)
```

will produce the following `RegistrationEvent` schema:

```text
StructType([
    StructField('correlation_id', StringType(), False),
    StructField('event_time', TimestampType(), False),
    StructField('user_id', StringType(), False)])
```

#### Using an `includes` declaration

For [example](https://github.com/asuiu/SparkORM/tree/master/examples/composite_schemas/includes.py), the following:

```python
class EventMetadata(Struct):
    correlation_id = String(nullable=False)
    event_time = Timestamp(nullable=False)

class RegistrationEvent(TableModel):
    class Meta:
        includes = [EventMetadata]
    user_id = String(nullable=False)
```

will produce the `RegistrationEvent` schema:

```text
StructType(List(
    StructField('user_id', StringType(), False),
    StructField('correlation_id', StringType(), False),
    StructField('event_time', TimestampType(), False)))
```

#### Using an `implements` declaration

`implements` is similar to `includes`, but does not automatically
incorporate the fields of specified structs. Instead, it is up to
the implementor to ensure that the required fields are declared in
the struct.

Failing to implement a field from an `implements` struct will result in
a `StructImplementationError` error.

[For example](https://github.com/asuiu/SparkORM/tree/master/examples/composite_schemas/implements.py):

```
class LogEntryMetadata(TableModel):
    logged_at = Timestamp(nullable=False)

class PageViewLogEntry(TableModel):
    class Meta:
        implements = [LogEntryMetadata]
    page_id = String(nullable=False)

# the above class declaration will fail with the following StructImplementationError error:
#   Struct 'PageViewLogEntry' does not implement field 'logged_at' required by struct 'LogEntryMetadata'
```


### Prettified Spark schema strings

Spark's stringified schema representation isn't very user-friendly, particularly for large schemas:


```text
StructType([StructField('name', StringType(), False), StructField('city', StructType([StructField('name', StringType(), False), StructField('latitude', FloatType(), True), StructField('longitude', FloatType(), True)]), True)])
```

The function `pretty_schema` will return something more useful:

```text
StructType([
    StructField('name', StringType(), False),
    StructField('city',
        StructType([
            StructField('name', StringType(), False),
            StructField('latitude', FloatType(), True),
            StructField('longitude', FloatType(), True)]),
        True)])
```

### Merge two Spark `StructType` types

It can be useful to build a composite schema from two `StructType`s. SparkORM provides a
`merge_schemas` function to do this.

[For example](https://github.com/asuiu/SparkORM/tree/master/examples/merge_struct_types/merge_struct_types.py):

```python
schema_a = StructType([
    StructField("message", StringType()),
    StructField("author", ArrayType(
        StructType([
            StructField("name", StringType())
        ])
    ))
])

schema_b = StructType([
    StructField("author", ArrayType(
        StructType([
            StructField("address", StringType())
        ])
    ))
])

merged_schema = merge_schemas(schema_a, schema_b)
```

results in a `merged_schema` that looks like:

```text
StructType([
    StructField('message', StringType(), True),
    StructField('author',
        ArrayType(StructType([
            StructField('name', StringType(), True),
            StructField('address', StringType(), True)]), True),
        True)])
```

## Contributing

Contributions are very welcome. Developers who'd like to contribute to
this project should refer to [CONTRIBUTING.md](./CONTRIBUTING.md).

## References:
Note: this library is a Fork from https://github.com/mattjw/sparkql
