=================================================
FWF - Python Fixed-Width-Field file format tools
=================================================

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
- File replacement: sometimes files get redelivered, e.g. because the original one
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
- Files which are compressed or from a (remote) object-store can still be processed, but
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
- We also tried converting the data files into hdf5 and similar formats, but
  (a) it still requires injest, including the hassle with redelivered files,
  and (b) many (not all) of these formats are columnar and thus require
  transformations. Columnar is good for analytics, but doesn't help with our use case.
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

In our examples below, we only use 'name', 'birthday' and 'gender'. So let's write the model.
`./sample_data/intro.ipynb`` contains a jupyter notebook with all the snippets from
below and can be used to follow along easily.

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

The sequence of fields is relevant for exporting and (pretty) printing
the dataset.

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

Slices, filters, etc. create views on top of their parent views.
Views are very light-weight and do not copy any data from the file.
They basically only maintain indexes into their parent view.

Views inherit the header (fields) from their parent, but maintain their
own copy. It can be modified without affecting the parents header.

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> # slices provide a view (subset) on the full data set
  >>> data[0:5].print(pretty=True)
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

  >>> # You want to change the field order?
  >>> data[0:5].print("name", "birthday", "gender", pretty=True)
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

  >>> # Indivial lines can be requested as well
  >>> data[327].print(pretty=True)
  +------------+----------+--------+
  | name       | birthday | gender |
  +------------+----------+--------+
  | Jack Brown | 19490106 | M      |
  +------------+----------+--------+

  >>> # The table is only a shell representation of the objects
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
  >>> first5.print(pretty=True)
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Dianne Mcintosh  | 19570526 | F      |
  | Rosalyn Clark    | 19940213 | M      |
  | Shirley Gray     | 19510403 | M      |
  | Georgia Frank    | 20110508 | F      |
  | Virginia Lambert | 19930404 | M      |
  +------------------+----------+--------+

  >>> first5.filter(op("gender") == b"F").print(pretty=True)
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Dianne Mcintosh  | 19570526 | F      |
  | Georgia Frank    | 20110508 | F      |
  +------------------+----------+--------+

  >>> # Multiple combinations (and/or) of filters
  >>> first5.filter(op("gender") == b"M", op("birthday").bytes() >= b"19900101", is_or=True).print(pretty=True)
  +--------------------------+----------+--------+
  |           name           | birthday | gender |
  +--------------------------+----------+--------+
  | Rosalyn Clark            | 19940213 |   M    |
  | Shirley Gray             | 19510403 |   M    |
  | Georgia Frank            | 20110508 |   F    |
  | Virginia Lambert         | 19930404 |   M    |
  +--------------------------+----------+--------+

  >>> # or chained filters
  >>> first5.filter(op("name").str().strip().endswith("k")).filter(op("gender")==b"F").print(pretty=True)
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Georgia Frank    | 20110508 | F      |
  +------------------+----------+--------+

  >>> # Filters are function invoked for each record.
  >>> first5.filter(lambda line: op("birthday").str().date().get(line).year == 1957)
  >>> # Which could be rewritten as:
  >>> first5.filter(op("birthday").bytes().startswith(b"1957"))
  >>> # Or
  >>> first5.filter(op("birthday")[0:4] == b"1957")
  >>> # Or with an additional field added to the view
  >>> first5.add_field("birthday_year", start=11, len=4)
  >>> first5.filter(op("birthday_year") == b"1957").print(pretty=True)
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

  >>> import fwf_db
  >>> from fwf_db import fwf_open, op

  >>> class HumanFileSpec:
      FIELDSPECS = [
              {"name": "name",       "slice": (32, 56)},
              {"name": "gender",     "slice": (19, 20)},
              {"name": "birthday",   "slice": (11, 19)},
              {"name": "location",   "slice": ( 0,  9)},
              {"name": "state",      "slice": ( 9, 11)},
              {"name": "universe",   "slice": (56, 68)},
              {"name": "profession", "slice": (68, 81)},
          ]

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")

  >>> # Create a unique index over column 'state'.
  >>> index = fwf_db.FWFUniqueIndexDict(data)
  >>> fwf_db.FWFCythonIndexBuilder(index).index(data, "state")
  >>> index.print("name", "state", "birthday", pretty=True, stop=5)
  +-------+-----------------------------+-------------+
  | state |             name            |   birthday  |
  +-------+-----------------------------+-------------+
  | b'AR' | b'Paul Dash               ' | b'19710316' |
  | b'MI' | b'Alex Taylor             ' | b'19420108' |
  | b'WI' | b'Terry Shelton           ' | b'19900906' |
  | b'MD' | b'James Clark             ' | b'20090909' |
  | b'PA' | b'Margaret Radford        ' | b'20130316' |
  +-------+-----------------------------+-------------+
    len: 5/51

  >>> # The index is dict-like, and the dict-value represent a single line
  >>> # in the file. Only the index itself consumes memory.
  >>> index[b"AR"].print(pretty=True)
  +--------------------------+--------+----------+-----------+-------+--------------+---------------+
  |           name           | gender | birthday |  location | state |   universe   |   profession  |
  +--------------------------+--------+----------+-----------+-------+--------------+---------------+
  | Paul Dash                |   F    | 19710316 | US        |   AR  | Whatever     | Student       |
  +--------------------------+--------+----------+-----------+-------+--------------+---------------+


In case a value is not unique, the last one will be stored in the index.
Which comes quite handy: consider a CDC use case (change data capture), where
the file contains potentially several records with the same ID and you only
need the last one. Or a multi-file scenario where in every month the first file
is a full export, whereas the remaining daily ones are delta exports. In SQL and
Pandas you need `group_by` operations, which are much more expensive (memory,
time).

The library does not support multi-level indexes. You may have recognized,
that we avoid to eagerly load all lines, parse all values, and so on. Same
for multi-level indexes. Because it is so fast to create an index, we rather
create the 2nd-level index if and when needed on the relevant subset. We
found it saves a lot of memory and has not shown up as performance bottleneck
so far.


None-unique index:

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")

  >>> # Create a none-unique index over column 'state'. The difference compared
  >>> # to the unique-index, is the dict-like object to maintain the index.
  >>> index = fwf_db.FWFIndexDict(data)
  >>> fwf_db.FWFCythonIndexBuilder(index).index(data, "state")
  >>> # There is no sensible
  >>> index
  FWFIndexDict(count=51): [b'AR': len(195), b'MI': len(222), b'WI': len(191), ...

  >>> # The dict-values are views. Exactly the ones we've seen in the previous
  >>> # section. Only the index itself consumes memory.
  >>> index[b"AR"].print(pretty=True)
  +--------------------------+--------+----------+-----------+-------+--------------+---------------+
  |           name           | gender | birthday |  location | state |   universe   |   profession  |
  +--------------------------+--------+----------+-----------+-------+--------------+---------------+
  | Dianne Mcintosh          |   F    | 19570526 | US        |   AR  | Whatever     | Medic         |
  | Karl Carney              |   M    | 19640508 | US        |   AR  | Whatever     | Shark tammer  |
  | Betsy Shipley            |   M    | 19950925 | US        |   AR  | Whatever     | Super hero    |
  | Elizabeth Lewis          |   F    | 20100330 | US        |   AR  | Whatever     | Time traveler |
  | Rosalyn Gamache          |   M    | 20030912 | US        |   AR  | Whatever     | Artist        |
  +--------------------------+--------+----------+-----------+-------+--------------+---------------+
  len: 5/195


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
  >>> data = fwf_open(HumanFileSpec, "sample_data/humans-subset.txt", "sample_data/humans.txt")
  >>> # We'll get to hidden and computed fields a little later
  >>> data[8:12].print("_lineno", "_file", pretty=True)
  +---------+-------------------------------+
  | _lineno |             _file             |
  +---------+-------------------------------+
  |    8    | sample_data/humans-subset.txt |
  |    9    | sample_data/humans-subset.txt |
  |    0    |     sample_data/humans.txt    |
  |    1    |     sample_data/humans.txt    |
  +---------+-------------------------------+

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
  >>> data[:5].print(pretty=True)
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Dianne Mcintosh  | 19570526 | F      |
  | Rosalyn Clark    | 19940213 | M      |
  | Shirley Gray     | 19510403 | M      |
  | Georgia Frank    | 20110508 | F      |
  | Virginia Lambert | 19930404 | M      |
  +------------------+----------+--------+

  >>> data[:5].exclude(op("gender")==b"F").print(pretty=True)
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
  >>> data[:5].print(pretty=True)
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Dianne Mcintosh  | 19570526 | F      |
  | Rosalyn Clark    | 19940213 | M      |
  | Shirley Gray     | 19510403 | M      |
  | Georgia Frank    | 20110508 | F      |
  | Virginia Lambert | 19930404 | M      |
  +------------------+----------+--------+

  >>> data[:5].order_by("gender").print(pretty=True)
  +------------------+--------+----------+
  | name             | gender | birthday |
  +------------------+--------+----------+
  | Dianne Mcintosh  | F      | 19570526 |
  | Georgia Frank    | F      | 20110508 |
  | Rosalyn Clark    | M      | 19940213 |
  | Shirley Gray     | M      | 19510403 |
  | Virginia Lambert | M      | 19930404 |
  +------------------+--------+----------+

  >>> data[:5].order_by("gender", "-birthday").print(pretty=True)
  +--------------------------+----------+--------+
  |           name           | birthday | gender |
  +--------------------------+----------+--------+
  | Georgia Frank            | 20110508 |   F    |
  | Dianne Mcintosh          | 19570526 |   F    |
  | Rosalyn Clark            | 19940213 |   M    |
  | Virginia Lambert         | 19930404 |   M    |
  | Shirley Gray             | 19510403 |   M    |
  +--------------------------+----------+--------+


.unique(field_name)
====================

Return a list of unique values for that field.

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> data[:5].print(pretty=True)
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
  >>> sorted(data.unique("gender"))
  [b'F', b'M']
  >>> sorted(data.unique("profession"))[0:5]
  [b'             ', b'Artist       ', b'Berserk      ', b'Comedian     ', b'Cookie maker ']
  >>> sorted(data.unique("state"))[0:10]
  [b'  ', b'AK', b'AL', b'AR', b'AZ', b'CA', b'CO', b'CT', b'DE', b'FL']


.count()
========

Return how many records are in a view: `len(data) == data.count()`


Computed fields
================

By default the following fields are available in all views:

- "_lineno": The line number (record number) within the original file, excluding leading comments
- "_file": The file name, e.g. as in a multi-file scenario
- "_line": The unchanged and unparsed original line including newline

For how to add your own computed fields, please see further down below.

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> data[10:15].print("_lineno", "name")
  +---------+--------------------------+
  | _lineno |           name           |
  +---------+--------------------------+
  |    10   | Robert Carolina          |
  |    11   | Gladys Martin            |
  |    12   | Jason Stinebaugh         |
  |    13   | Kenneth Provines         |
  |    14   | James Mcgloster          |
  +---------+--------------------------+

  >>> data[10:15].print("_lineno", data.header())
  +---------+--------------------------+--------+----------+-----------+-------+--------------+---------------+
  | _lineno |           name           | gender | birthday |  location | state |   universe   |   profession  |
  +---------+--------------------------+--------+----------+-----------+-------+--------------+---------------+
  |    10   | Robert Carolina          |   M    | 20090527 | US        |   AL  | Whatever     | Time traveler |
  |    11   | Gladys Martin            |   F    | 19990123 | US        |   WY  | Whatever     | Medic         |
  |    12   | Jason Stinebaugh         |   M    | 19610219 | US        |   FL  | Whatever     | Comedian      |
  |    13   | Kenneth Provines         |   F    | 19911219 | US        |   HI  | Whatever     | Super hero    |
  |    14   | James Mcgloster          |   M    | 19741114 | US        |   AL  | Whatever     | Programmer    |
  +---------+--------------------------+--------+----------+-----------+-------+--------------+---------------+

  >>> # Note the trailing whitespaces and breakline on __line
  >>> data[10:15].print("_lineno", "_line", pretty=True)
  +---------+------------------------------------------------------------------------------------+
  | _lineno |                                       _line                                        |
  +---------+------------------------------------------------------------------------------------+
  |    10   | US       AL20090527M771b0ad5b70fRobert Carolina         Whatever    Time traveler# |
  |         |                                                                                    |
  |    11   | US       WY19990123Fad2d64883e15Gladys Martin           Whatever    Medic        # |
  |         |                                                                                    |
  |    12   | US       FL19610219Ma701d784bc77Jason Stinebaugh        Whatever    Comedian     # |
  |         |                                                                                    |
  |    13   | US       HI19911219Fe301c6ea97b9Kenneth Provines        Whatever    Super hero   # |
  |         |                                                                                    |
  |    14   | US       AL19741114M4f56d046e3b5James Mcgloster         Whatever    Programmer   # |
  |         |                                                                                    |
  +---------+------------------------------------------------------------------------------------+

  >>> list(data[10:15].to_list("_lineno", "_line"))
  [
    (10, b'US       AL20090527M771b0ad5b70fRobert Carolina         Whatever    Time traveler#\n'),
    (11, b'US       WY19990123Fad2d64883e15Gladys Martin           Whatever    Medic        #\n'),
    (12, b'US       FL19610219Ma701d784bc77Jason Stinebaugh        Whatever    Comedian     #\n'),
    (13, b'US       HI19911219Fe301c6ea97b9Kenneth Provines        Whatever    Super hero   #\n'),
    (14, b'US       AL19741114M4f56d046e3b5James Mcgloster         Whatever    Programmer   #\n')
  ]


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

  class ExtendedHumanFileSpec:
      FIELDSPECS = [
              {"name": "name",       "slice": (32, 56)},
              {"name": "gender",     "slice": (19, 20)},
              {"name": "birthday",   "slice": (11, 19)},
          ]

      def __header__(self) -> list[str]:
          # Define the default header
          return ["name", "gender", "birthday", "birthday_year", "age"]

      def birthday_year(self, line: FWFLine):
          return int(line.birthday[0:4])

      def age(self, line: FWFLine):
          return datetime.today().year - self.birthday_year(line)

      def __validate__(self, line: FWFLine) -> bool:
          return True  # False => Error

      def my_comment_filter(self, line: FWFLine) -> bool:
          return line[0] != ord("#")


.. code-block:: Python

  >>> # Filter with a user defined method
  >>> data.filter(data.filespec.my_comment_filter)

  >>> # Print headers as defined in __headers__()
  >>> # And including user-defined computed fields
  >>> data[:5].print(pretty=True)

  >>> # Test every line on your own criteria and list the errornous lines
  >>> data.validate().print("_lineno", "_lineno", pretty=True)


More on "debugging" fwf files
==============================

Development
============

We are using a virtual env (`.venv`) for dependencies. And given the chosen
file structure (`./src` directory; `./tests` directory without `__init__.py`), we do
`pip install -e .` to install the project in '.' as a local package, with
development enabled (-e).

Test execution: `pytest -sx tests\...`

Build the cython exentions only: ./build_ext.bat
