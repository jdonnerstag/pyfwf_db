==========================================
FWF - Fixed-Width-Field file format tools
==========================================

UNDER RECONSTRUCTION


A python library that provides (very) fast, read-only, NOSQL-like, access
to (very) large, multi-partitioned, files with fixed-width-fields.

Files that look like this:
::

  USAR19570526Fbe56008be36eDianne Mcintosh WhateverMedic
  USMI19940213M706a6e0afc3dRosalyn Clark   WhateverComedian
  USWI19510403M451ed630accbShirley Gray    WhateverComedian
  USMD20110508F7e5cd7324f38Georgia Frank   WhateverComedian
  USPA19930404Mecc7f17c16a6Virginia LambertWhateverShark tammer
  USVT19770319Fd2bd88100facRichard Botto   WhateverTime traveler
  USOK19910917F9c704139a6e3Alberto Giel    WhateverStudent
  USNV20120604F5f02187599d7Mildred Henke   WhateverSuper hero
  USRI19820125Fcf54b2eb5219Marc Kidd       WhateverMedic
  USME20080503F0f51da89a299Kelly Crose     WhateverComedian
  ...

Where each line represents one dataset and every field, respectively
line, has a fixed length, without explicit separator between the fields.

Key Features
============

This lib is especially targetted for the following use cases:

- Ultra-fast: no more lunch breaks for whatever ingest or import job to finish
- Large files: Files can be larger then memory
- Multi-files: every file is considered a partition and multiple files can be
  combined into a single dataset
- File replacement: sometimes files are redelivered, e.g. because the original one
  needed corrections. Replacing these files is fast and effortless.
- Evolving file structures: The exact field structure of a file type might change
  over time. With this library, no transformations or migrations are required.
- Views: without modifying the underlying file, views may contain only a subset
  (filters) of the data, or in a different order (e.g. sorted)
- Filters: Often not all records are required. E.g. some data are like change
  records (CDC) and only the ones received before a certain point in time are
  relevant. The library provides flexible and fast filters.
- Lookups: Fast nosql-like lookups with indexes is a priority. But no analytics,
  reporting or number crunching. Data can be exported into Pandas, Vaex, etc.
- Support for arbitrary line-endings: it's unbelievable how often we receive files
  with none-standard line-endings, such as \x00 or similar, or no newline char at all.
- Persisted indexes: Not a huge gain, but saving few more seconds
- Casts and transformations to convert field data (bytes) into strings, ints,
  dates or anything you want
- Files which are compressed or from a (remote) object-store can be processed, but
  must fit into memory (or uncompressed and locally cached; not in scope of this lib)
- Field length is in bytes rather then chars. UTF-8 chars consume 1-6 bytes, which
  leads to variable line lengths in bytes. The lib however relies on a constant line
  length in bytes (except for leading comment lines)
- Pretty tables: During development it is often necessary to take a quick look
  into the file (data are not as expected, file specification wrong, etc.). This
  is why we have some support for "debugging" the files.
- Large servers are expensive: Database-like systems consume more or less resources
  (e.g. CPU, storage) and require to injest the data (which takes time and consumes
  more resources). This lib allowed us to develop our applications on our
  laptops with full production datasets (anonymized).


How did we get here?
====================

Building this lib wasn't our first thought:

- We needed lots of lookups, but no analytics, across multiple tables, all provided
  as files. And because we have been using RDBMS and Nosql systems quite a bit, we
  had good and experienced people. But ingesting and preparing (staging) the data
  took ages. We applied partitioning, and all sort of tricks to speed up ingest
  and lookups, but it remained painful, slow and also comparatively expensive.
  We've tested it on-premise and in public clouds, including rather big boxes with
  lots of I/O and network bandwidth.
- We tried NoSql but following best pratices, it is adviced to create a
  schema that matches your queries best. Hence complexity in the ingest
  layer, and more storage needed. This and because network latency for queries
  didn't go away, it did not make us happy.
- We also tried converting the source files into hdf5 and similar formats, but
  (a) it still requires injest, including the hassle with redelivered files,
  and (b) many (not all) of these formats are columnar. Which is good for analytics,
  but doesn't help with our use case.
- Several of us have laptops with 24GB RAM and we initially started small with
  a 5GB data set of uncompressed fixed-width files. We tried to load them with
  Pandas, but quickly run into out-of-memory exceptions, even with in-stream
  filtering of records upon ingest. There are several blogs alluding to a
  factor 5 between raw data and memory consumption. Once loaded, the performance
  was perfect.
- With this little lib,

   - We almost avoid load or ingest jobs. There is not enough time to grab another
     coffee, to make the data accessible to your business logic. Redelivered files
     and file schema evolution is no problem any more.
   - An index scan on a full production data set, takes less then 2 mins on our
     standard business laptops (with SDD), which is many times faster than the
     other options we tested, and on low-cost hardware (vs big boxes and
     high-speed networks).
   - We've tested it with 100GB data sets (our individual file size usually is <10GB),
     gradually approaching memory limits for (in-memory) indexes.
   - We happily develop, debug and test our applications on our laptop, with
     full size (anonmized) data sets.


Installation
============

Standard python `pip`.

.. code-block:: Python

  pip install git+https://github.com/jdonnerstag/pyfwf_db.git


Setting up your parser
======================

First thing you need to know is the width of each column in your file.
There's no magic here. You need to find out.

Lets take `this file`_ as an example. The first line looks like:

.. _this file: https://raw.githubusercontent.com/nano-labs/pyfwf3/master/sample_data/humans.txt

::

  1234567890123456789012345678901234567890123456789012345678901234567890123
  US       AR19570526Fbe56008be36eDianne Mcintosh         Whatever    Medic

- 9 bytes: location
- 2 bytes: state
- 8 bytes: birthdate
- 1 byte: gender
- 12 bytes: don't know
- 24 bytes: name
- \.\. and so on

In our examples below, we only use 'name', 'birthday' and 'gender'. So let's write the model:

.. code-block:: Python

  class HumanFileSpec:
      FIELDSPECS = [
          {"name": "birthday", "slice": (11, 19)},
          {"name": "gender"  , "slice": (19, 20)},
          {"name": "name"    , "slice": (32, 56)},
      ]

The slices represent the first and last positions of each information
within the line. Alternatively you may provide combinations of 'start', 'len' and
'stop'.

The sequence of fields is only relevant for (pretty) printing the dataset,
or exporting it.

Now, lets open the file.

.. code-block:: Python

  from fwf_db import fwf_open

  data = fwf_open(HumanFileSpec, "sample_data/humans.txt")

That's it. The records are now accessible. Together it looks like this:

.. code-block:: Python

  from fwf_db import fwf_open, op

  class HumanFileSpec:
      FIELDSPECS = [
          {"name": "birthday", "slice": (11, 19)},
          {"name": "gender"  , "slice": (19, 20)},
          {"name": "name"    , "slice": (32, 56)},
      ]

  data = fwf_open(HumanFileSpec, "sample_data/humans.txt")


Views
======

`data`, in the example above, makes all records and fields from the file available,
and is accessible almost like a standard python list. You may consider it the
root-view, as it doesn't have another parent view.

Slices, filters, etc. create views on top of their parent view.
Views are very light-weight and do not copy any data from the file.
They basically only maintain indexes into their parent view.

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> # slices provide a view (subset) onto the full data set
  >>> data[0:5]
  +----------+--------+--------------------------+
  | birthday | gender |           name           |
  +----------+--------+--------------------------+
  | 19570526 |   F    | Dianne Mcintosh          |
  | 19940213 |   M    | Rosalyn Clark            |
  | 19510403 |   M    | Shirley Gray             |
  | 20110508 |   F    | Georgia Frank            |
  | 19930404 |   M    | Virginia Lambert         |
  +----------+--------+--------------------------+
  len: 5/5

  >>> # You want to change field order?
  >>> data[0:5].print("name", "birthday", "gender")
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Dianne Mcintosh  | 19570526 | F      |
  | Rosalyn Clark    | 19940213 | M      |
  | Shirley Gray     | 19510403 | M      |
  | Georgia Frank    | 20110508 | F      |
  | Virginia Lambert | 19930404 | M      |
  +------------------+----------+--------+

  >>> # May be you want to change it for the view?
  >>> data[0:5].set_header("name", "birthday", "gender")

  >>> # Getting a specific item returns a line instance
  >>> data[327]
  +------------+----------+--------+
  | name       | birthday | gender |
  +------------+----------+--------+
  | Jack Brown | 19490106 | M      |
  +------------+----------+--------+

  >>> # Note that the table is only a shell representation of the objects
  >>> data[327].name
  'Jack Brown'
  >>> data[327].birthday
  '19490106'
  >>> data[327].gender
  'M'
  >>> tuple(data[327])
  ('Jack Brown', '19490106', 'M')
  >>> list(data[327])
  ['Jack Brown', '19490106', 'M']


.filter(\*\*kwargs)
===================

Any view can be filtered and returns a new view.
Which again can be filtered and so on.

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> data.set_header("name", "birthday", "gender")
  >>> first5 = data[:5]
  >>> first5
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Dianne Mcintosh  | 19570526 | F      |
  | Rosalyn Clark    | 19940213 | M      |
  | Shirley Gray     | 19510403 | M      |
  | Georgia Frank    | 20110508 | F      |
  | Virginia Lambert | 19930404 | M      |
  +------------------+----------+--------+

  >>> first5.filter(op("gender") == b"F")
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Dianne Mcintosh  | 19570526 | F      |
  | Georgia Frank    | 20110508 | F      |
  +------------------+----------+--------+

  >>> # with multiple filters and support to 'and'/'or' the individual results
  >>> first5.filter(op("gender") == b"M", op("birthday").bytes() >= b"19900101", is_or=True)
  +--------------------------+----------+--------+
  |           name           | birthday | gender |
  +--------------------------+----------+--------+
  | Rosalyn Clark            | 19940213 |   M    |
  | Shirley Gray             | 19510403 |   M    |
  | Georgia Frank            | 20110508 |   F    |
  | Virginia Lambert         | 19930404 |   M    |
  +--------------------------+----------+--------+

  >>> # or chained filters
  >>> first5.filter(op("name").str().strip().endswith("k")).filter(op("gender")==b"F")
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Georgia Frank    | 20110508 | F      |
  +------------------+----------+--------+

  >>> # Filters conditions are function invoked for each record.
  >>> first5.filter(lambda line: op("birthday").str().date().get(line).year == 1957)
  >>> # Which could be rewritten as:
  >>> first5.filter(op("birthday").bytes().startswith(b"1957"))
  >>> # Or
  >>> first5.filter(op("birthday")[0:4] == b"1957")
  >>> # Or with an additional field
  >>> first5.add_header("birthday_year", start=11, len=4)
  >>> first5.filter(op("birthday_year") == b"1957")
  +------------------+----------+--------+---------------+
  | name             | birthday | gender | birthday_year |
  +------------------+----------+--------+---------------+
  | Dianne Mcintosh  | 19570526 | F      | 1957          |
  +------------------+----------+--------+---------------+


Indices
========

As mentioned previously the main use case for this library is
  - (very) fast nosql-like access
  - data-sets potentially larger then memory

The 2nd point is covered my means of memory-mapping the file.
The 1st one requires to support indexes, unique and none-unique ones.

Unique index:

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>>
  >>> # Create an unique index over column 'state'
  >>> index = FWFUniqueIndexDict(data, {})
  >>> FWFCythonIndexBuilder(rtn).index(data, "state")
  >>> index


  >>> # The index is dict-like, and the dict-value represent a single line
  >>> # in the file. Only the index itself consumes memory.
  >>> index[b"AR"]


In case a value is not unique, the last one will be stored in the index.
Which is quite handy: consider a CDC use case (change data capture), where
the file contains potentially several records with the same ID and you only
need the last one. Or a multi-file scenario where the first file every month
is few a full exports, whereas the daily ones are delta exports. In SQL and
Pandas you need `group_by` operations, which are much more expensive (memory,
time).


None-unique index:

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>>

  >>> # Create a none-unique index over column 'state'. The difference compared
  >>> # to the unique-index, is the dict-like object to maintain the index.
  >>> index = FWFIndexDict(data)
  >>> FWFCythonIndexBuilder(rtn).index(data, "state")
  >>> index


  >>> # The dict-values are views. Exactly the ones we've seen in the previous
  >>> # section. Only the index itself consumes memory.
  >>> index[b"AR"]


Multi-File
===========

Events and streaming is the future, but we often receive files
in regular time intervals. Every file might be considered a partition,
and the sum of several of these files make up a dataset. All operations
possible on a single file, should transparently be possible on Multi-files
as well. Including redelivered files, and including file schema evolution.


.. code-block:: Python

  >>> # Create a multi-file dataset, but passing all the file names to fwf_open()
  >>> # In this example it is twice the same file, only for demonstration purposes.
  >>> files = ["sample_data/humans.txt", "sample_data/humans.txt"]
  >>> data = fwf_open(HumanFileSpec, files)


Everything else remains the same: views, filters, indexes

More on Views
==============

This section shows more examples of what can be done with views.


.exclude(\*\*kwargs)
====================

Pretty much the opposite of `.filter()`

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> data.set_header("name", "birthday", "gender")
  >>> first5 = data[:5]
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Dianne Mcintosh  | 19570526 | F      |
  | Rosalyn Clark    | 19940213 | M      |
  | Shirley Gray     | 19510403 | M      |
  | Georgia Frank    | 20110508 | F      |
  | Virginia Lambert | 19930404 | M      |
  +------------------+----------+--------+
  >>> first5.exclude(op("gender")=="F")
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Rosalyn Clark    | 19940213 | M      |
  | Shirley Gray     | 19510403 | M      |
  | Virginia Lambert | 19930404 | M      |
  +------------------+----------+--------+


.order_by(field_name(s))
============================

Create a new view with the field(s) being sorted. Default sorting
is ascending. For descending sorting prepend the field name with
'-', e.g. '-birthday'.

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> data.set_header("name", "birthday", "gender")
  >>> first5 = data[:5]
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Dianne Mcintosh  | 19570526 | F      |
  | Rosalyn Clark    | 19940213 | M      |
  | Shirley Gray     | 19510403 | M      |
  | Georgia Frank    | 20110508 | F      |
  | Virginia Lambert | 19930404 | M      |
  +------------------+----------+--------+
  >>> data[:5].order_by("gender")
  +------------------+--------+----------+
  | name             | gender | birthday |
  +------------------+--------+----------+
  | Dianne Mcintosh  | F      | 19570526 |
  | Georgia Frank    | F      | 20110508 |
  | Rosalyn Clark    | M      | 19940213 |
  | Shirley Gray     | M      | 19510403 |
  | Virginia Lambert | M      | 19930404 |
  +------------------+--------+----------+
  >>> data[:5].order_by("gender", "-birthday")
  +------------------+--------+----------+
  | name             | gender | birthday |
  +------------------+--------+----------+
  | Virginia Lambert | M      | 19930404 |
  | Shirley Gray     | M      | 19510403 |
  | Rosalyn Clark    | M      | 19940213 |
  | Georgia Frank    | F      | 20110508 |
  | Dianne Mcintosh  | F      | 19570526 |
  +------------------+--------+----------+


.unique(field_name)
====================

Return a list of unique values for that field.

.. code-block:: Python

  from fwf_db import fwf_open, op

  class HumanFileSpec:
      FIELDSPECS = [
              {"name": "name",       "slice": (32, 56)},
              {"name": "gender",     "slice": (19, 20)},
              {"name": "birthday",   "slice": (11, 19)},
              {"name": "location",   "slice": ( 0,  9)},
              {"name": "state",      "slice": ( 9, 11)},
              {"name": "universe",   "slice": (56, 68)},
              {"name": "profession", "slice": (68, 81)},
          ]

  data = fwf_open(HumanFileSpec, "sample_data/humans.txt")

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> data[:5]
  +------------------+--------+----------+----------+-------+----------+--------------+
  | name             | gender | birthday | location | state | universe | profession   |
  +------------------+--------+----------+----------+-------+----------+--------------+
  | Dianne Mcintosh  | F      | 19570526 | US       | AR    | Whatever | Medic        |
  | Rosalyn Clark    | M      | 19940213 | US       | MI    | Whatever | Comedian     |
  | Shirley Gray     | M      | 19510403 | US       | WI    | Whatever | Comedian     |
  | Georgia Frank    | F      | 20110508 | US       | MD    | Whatever | Comedian     |
  | Virginia Lambert | M      | 19930404 | US       | PA    | Whatever | Shark tammer |
  +------------------+--------+----------+----------+-------+----------+--------------+
  >>> # Looking into all objects
  >>> data.unique("gender")
  ['F', 'M']
  >>> data.unique("profession")
  ['', 'Time traveler', 'Student', 'Berserk', 'Hero', 'Soldier', 'Super hero', 'Shark tammer', 'Artist', 'Hunter', 'Cookie maker', 'Comedian', 'Mecromancer', 'Programmer', 'Medic', 'Siren']
  >>> data.unique("state")
  ['', 'MT', 'WA', 'NY', 'AZ', 'MD', 'LA', 'IN', 'IL', 'WY', 'OK', 'NJ', 'VT', 'OH', 'AR', 'FL', 'DE', 'KS', 'NC', 'NM', 'MA', 'NH', 'ME', 'CT', 'MS', 'RI', 'ID', 'HI', 'NE', 'TN', 'AL', 'MN', 'TX', 'WV', 'KY', 'CA', 'NV', 'AK', 'IA', 'PA', 'UT', 'SD', 'CO', 'MI', 'VA', 'GA', 'ND', 'OR', 'SC', 'WI', 'MO']

TODO: Unique by special field
TODO: Need to explain computed fields first

.count()
========

Return how many records are in a view: `len(data) == data.count()`


Computed fields
================

By default the following fields are supported:

- "_lineno": The line number (record number) within the original file, excluding leading comments
- "_file": The file name, e.g. as in a multi-file scenario
- "_line": The unchanged and unparsed original line including newline

For how to add your own computed fields, please see further down below.

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> first5 = data[:5]
  >>> first5.print("_lineno", "name")
  +--------------+------------------+
  | _lineno      | name             |
  +--------------+------------------+
  | 4328         | John Cleese      |
  | 9282         | Johnny Andres    |
  | 8466         | Oscar Callaghan  |
  | 3446         | Gilbert Garcia   |
  | 6378         | Helen Villarreal |
  +--------------+------------------+

  >>> first5.print("_lineno", *first5.headers())
  +--------------+------------------+--------+----------+----------+-------+--------------+------------+
  | _line_number | name             | gender | birthday | location | state | universe     | profession |
  +--------------+------------------+--------+----------+----------+-------+--------------+------------+
  | 4328         | John Cleese      | M      | 19391027 | UK       |       | Monty Python | Comedian   |
  | 9282         | Johnny Andres    | F      | 19400107 | US       | TX    | Whatever     | Student    |
  | 8466         | Oscar Callaghan  | M      | 19400121 | US       | ID    | Whatever     | Comedian   |
  | 3446         | Gilbert Garcia   | M      | 19400125 | US       | NC    | Whatever     | Student    |
  | 6378         | Helen Villarreal | F      | 19400125 | US       | MD    | Whatever     |            |
  +--------------+------------------+--------+----------+----------+-------+--------------+------------+

  >>> # Note the trailing whitespaces and breakline on __line
  >>> first5.set_header("_lineno", "_line")
  +--------------+-----------------------------------------------------------------------------------+
  | _lineno      | _line                                                                             |
  +--------------+-----------------------------------------------------------------------------------+
  | 1            | US       AR19570526Fbe56008be36eDianne Mcintosh         Whatever    Medic         |
  |              |                                                                                   |
  | 2            | US       MI19940213M706a6e0afc3dRosalyn Clark           Whatever    Comedian      |
  |              |                                                                                   |
  | 3            | US       WI19510403M451ed630accbShirley Gray            Whatever    Comedian      |
  |              |                                                                                   |
  | 4            | US       MD20110508F7e5cd7324f38Georgia Frank           Whatever    Comedian      |
  |              |                                                                                   |
  | 5            | US       PA19930404Mecc7f17c16a6Virginia Lambert        Whatever    Shark tammer  |
  |              |                                                                                   |
  +--------------+-----------------------------------------------------------------------------------+

  >>> first5.to_list()
  [(1, 'US       AR19570526Fbe56008be36eDianne Mcintosh         Whatever    Medic        \n'),
      (2, 'US       MI19940213M706a6e0afc3dRosalyn Clark           Whatever    Comedian     \n'),
      (3, 'US       WI19510403M451ed630accbShirley Gray            Whatever    Comedian     \n'),
      (4, 'US       MD20110508F7e5cd7324f38Georgia Frank           Whatever    Comedian     \n'),
      (5, 'US       PA19930404Mecc7f17c16a6Virginia Lambert        Whatever    Shark tammer \n')]


Additional computed fields:

.. code-block:: Python

  class HumanFileSpec:
      FIELDSPECS = [
              {"name": "name",       "slice": (32, 56)},
              {"name": "gender",     "slice": (19, 20)},
              {"name": "birthday",   "slice": (11, 19)},
          ]

The reason why a file specification is a class like the one above, is because
methods can be added to it, e.g:

.. code-block:: Python

  class HumanFileSpec:
      FIELDSPECS = [
              {"name": "name",       "slice": (32, 56)},
              {"name": "gender",     "slice": (19, 20)},
              {"name": "birthday",   "slice": (11, 19)},
          ]

      def __headers__(self) -> list[str]:   # TODO
          # Define the default for headers
          return ["name, "gender", "birthday", "birthday_year", "age"]

      def __parse__(self, line: FWFLine) -> bool:
          line.birthday_year = int(line.birthday[0:4])
          self.age = datetime.today().year - self.birthday_year(line)

          TODO Throw exception to stop processing !!!

          return True  # False => Filter out

      def my_comment_filter(self, line: FWFLine) -> bool:
          return line[0] != ord("#")


.. code-block:: Python

  >>> data.filter(data.filespec.my_comment_filter)
  >>> data[:5]    # Will print headers as defined in __headers__()


Development
============

We are using a virtual env (`.venv`) for dependencies. And given the chosen
file structure (`./src` directory; `./tests` directory without `__init__.py`), we do
`pip install -e .` to install the project in '.' as a local package, with
development enabled (-e).

Test execution: `pytest -sx tests\...`

Build the cython exentions only: ./build_ext.bat
