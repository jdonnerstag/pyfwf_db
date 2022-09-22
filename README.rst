==========================================
FWF - Fixed-Width-Field file format tools
==========================================

THIS IS WORK IN PROGRESS


A python library that provides performant, read-only, NOSQL-like access
to (very) large files with fixed-width-fields. The file size is not limited
by the amount of memory available. Very fast index creation for NOSQL-like
lookups, with filters and multi-file/-partition support.

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
- Data sets (files) which can be larger then the memory available
- New files of the same kind arrive regulary and must be considered like partions of
  one large data set.
- Individual files might get replaced with updated ones, e.g. because of corrections
- The exact field structure of a file type might change over time. A data set may
  consist of multiple files, possibly based on a different version of the structure.
- Filters and Views (subsets) are required. We opted for django-like filters.
- Support for indexes and very fast lookups. But no analytics, reporting or number crunching.
- Easy export of views into Pandas, Vaex and other tools if needed
- Some data are like change records (CDC) and only the one received before a certain
  point in time is relevant. Filtering them should be easy and fast. All fields
  must be provided in each record (not just the fields that have changed).
- Persisted indexes to avoid rebuilding them unnecessarily
- Casts and transformations to convert field data (bytes) into strings, ints,
  dates or anything you want
- Support for arbitrary line-endings: it's unbelievable how often we receive files
  with none-standard line-endings, such as \x00 or similar.
- Files which are compressed or from a (remote) object store can be processed, but
  must fit into memory (or uncompressed and locally cached; not in this package)
- Field length is in bytes rather then chars. UTF-8 chars consume 1-6 bytes, which
  leads to variable line lengths in bytes. The lib however relies on a constant line
  length in bytes (except for leading comment lines)

<< Moved into separate package?? >>
- CLI visualization as table (thanks to `prettytable`_)
- To refine the visualisation, virtually change the order of the columns
- Virtually remove columns not needed
- Add virtual (computed) columns (in-memory). The files remain read-only and unchanged.
- Sorting and uniqueness filters based on column data
- Entry count in a view (subset)

.. _prettytable: https://github.com/jazzband/prettytable

There are many tools around to explore (fixed-width) data files. But this little
tool has been very handy for us.

How did we get here?
====================

Building this lib wasn't our first thought:

- We needed lots of lookups, but no analytics, across multiple tables, all provided
  as files. And because we have been using RDBMS and Nosql systems quite a bit, we
  had good and experienced people. But ingesting and preparing (staging) the data
  took ages. We applied partitioning, and all sort of tricks to speed up ingest
  and lookups, but it remained painful, slow and also comparatively expensive.
  We've tested it on-premise and in public clouds, including rather big boxes with
  sufficient I/O and network bandwidth.
- We tried NoSql but following best pratices, it is adviced to create a
  schema that matches your queries best. Hence more complexity in the ingest
  layer. This and because network latency for queries didn't go away, did not
  make us happy.
- We also tried converting the source files into hdf5 and similar formats, but
  (a) it still requires injest and it wasn't especially fast, and (b) many of
  these formats are columnar. Which is good for analytics, but doesn't help with
  our use case.
- Several of us have laptops with 24GB RAM and we initially started with
  a 5GB data set of uncompressed fixed-width files. We tried to load them with
  Pandas, but quickly run into out-of-memory exceptions, even with in-stream
  filtering of records upon ingest. There are several blogs alluding to a
  factor 5 between raw data and memory consumption. Once loaded, the performance
  was perfect.
- With our little lib,

   - we almost avoid load or ingest jobs. We can start using new files immediately
     when they arrive (not enough time for grab another coffee)
   - A full index scan, takes less then 2 mins on our standard business
     laptops (with SDD), which is many times faster than the other options, and on
     low-cost hardware (vs big boxes and high-speed networks).
   - With Numpy based indexes, the solution is very fast to determine the line number.
     Loading the line from disc and converting the required fields / columns from bytes
     into consumable data types, is a bit slower compared to in-memory preprocessed
     Pandas dataframes. But we need to do millions of lookups, and in our use cases,
     we don't need to re-read lines that often. Where required, we cache the
     converted object. Numpy is good, but not a good fit for our problem.
   - We've tested it with 100GB data sets (our individual file size usually is <10GB),
     approaching memory limits for full table (in-memory) indexes. Obviously depending
     on the number of rows and keys.

Installation
============

Just use pip. You must have 'git' installed as well.

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

For the examples, we only use name, birthday and gender. So let's write the model:

.. code-block:: Python

  class HumanFileSpec:
      FIELDSPECS = [
          {"name": "birthday", "slice": (11, 19)},
          {"name": "gender"  , "slice": (19, 20)},
          {"name": "name"    , "slice": (32, 56)},
      ]

The slices represent the first and last positions of each information
in the line. Alternatively you may provide combinations of 'start', 'len' and
'stop'. Now we are going to use it with the file parser.

.. code-block:: Python

  from fwf_db import fwf_open

  data = fwf_open(HumanFileSpec, "sample_data/humans.txt")

That's it. The records are now accessible. Togther it looks like this:

.. code-block:: Python

  from fwf_db import fwf_open

  class HumanFileSpec:
      FIELDSPECS = [
          {"name": "birthday", "slice": (11, 19)},
          {"name": "gender"  , "slice": (19, 20)},
          {"name": "name"    , "slice": (32, 56)},
      ]

  data = fwf_open(HumanFileSpec, "sample_data/humans.txt")


Queryset
========

`FWFFile` makes all records and fields from the file available,
like a standard python list:

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

  >>> # The field order may not want we want. Lets change it.
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

  >>> # May be you want to change it permanently for the view?
  >>> data[0:5].set_headers("name", "birthday", "gender")

  >>> # while getting a specific item returns a parsed line instance
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

Here is where the magic happens. A filtered queryset will always return
a new queryset that can be filtered again and so on.

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> data.header("name", "birthday", "gender")
  >>> first5 = data[:5]
  >>> # 'first5' is a Queryset instance just as 'data' but with only a few objects
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

  >>> # And it still can be filtered
  >>> first5.filter(op("gender")=="F")
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Dianne Mcintosh  | 19570526 | F      |
  | Georgia Frank    | 20110508 | F      |
  +------------------+----------+--------+

  >>> # with multiple keywords arguments
  >>> first5.filter(op("gender")=="M", op("birthday") >= "19900101")
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Rosalyn Clark    | 19940213 | M      |
  | Virginia Lambert | 19930404 | M      |
  +------------------+----------+--------+

  >>> # or chained filters
  >>> first5.filter(op("name").endswith("k")).filter(op("gender")="F")
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Georgia Frank    | 20110508 | F      |
  +------------------+----------+--------+

  >>> # Convert values first
  >>> first5.filter(op("birthday").date().year == 1957)
  +------------------+----------+--------+
  | name             | birthday | gender |
  +------------------+----------+--------+
  | Dianne Mcintosh  | 19570526 | F      |
  +------------------+----------+--------+


.exclude(\*\*kwargs)
====================

Pretty much the opposite of `.filter()`

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> data.header("name", "birthday", "gender")
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

.order_by(field_name, reverse=False)
====================================

Reorder the whole queryset sorting by that given field

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> data.header("name", "birthday", "gender")
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
  >>> data[:5].order_by("name")
  +------------------+--------+----------+
  | name             | gender | birthday |
  +------------------+--------+----------+
  | Dianne Mcintosh  | F      | 19570526 |
  | Georgia Frank    | F      | 20110508 |
  | Rosalyn Clark    | M      | 19940213 |
  | Shirley Gray     | M      | 19510403 |
  | Virginia Lambert | M      | 19930404 |
  +------------------+--------+----------+
  >>> data[:5].order_by("name", reverse=True)
  +------------------+--------+----------+
  | name             | gender | birthday |
  +------------------+--------+----------+
  | Virginia Lambert | M      | 19930404 |
  | Shirley Gray     | M      | 19510403 |
  | Rosalyn Clark    | M      | 19940213 |
  | Georgia Frank    | F      | 20110508 |
  | Dianne Mcintosh  | F      | 19570526 |
  +------------------+--------+----------+

TODO: Order by more than one field via chaining order_by

.unique(field_name)
====================

Return a list of unique values for that field.

.. code-block:: Python

  from collections import OrderedDict
  from fwf import BaseLineParser, BaseFileParser

  class CompleteHuman(BaseLineParser):
      """Complete line parser for humans.txt example file."""

      _map = OrderedDict(
          [
              ("name", slice(32, 56)),
              ("gender", slice(19, 20)),
              ("birthday", slice(11, 19)),
              ("location", slice(0, 9)),
              ("state", slice(9, 11)),
              ("universe", slice(56, 68)),
              ("profession", slice(68, 81)),
          ]
      )

  class CompleteHumanFileParser(BaseFileParser):
      """Complete file parser for humans.txt example file."""

      _line_parser = CompleteHuman

.. code-block:: Python

  >>> parsed = CompleteHumanFileParser.open("sample_data/humans.txt")
  >>> parsed.objects[:5]
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
  >>> parsed.objects.unique("gender")
  ['F', 'M']
  >>> parsed.objects.unique("profession")
  ['', 'Time traveler', 'Student', 'Berserk', 'Hero', 'Soldier', 'Super hero', 'Shark tammer', 'Artist', 'Hunter', 'Cookie maker', 'Comedian', 'Mecromancer', 'Programmer', 'Medic', 'Siren']
  >>> parsed.objects.unique("state")
  ['', 'MT', 'WA', 'NY', 'AZ', 'MD', 'LA', 'IN', 'IL', 'WY', 'OK', 'NJ', 'VT', 'OH', 'AR', 'FL', 'DE', 'KS', 'NC', 'NM', 'MA', 'NH', 'ME', 'CT', 'MS', 'RI', 'ID', 'HI', 'NE', 'TN', 'AL', 'MN', 'TX', 'WV', 'KY', 'CA', 'NV', 'AK', 'IA', 'PA', 'UT', 'SD', 'CO', 'MI', 'VA', 'GA', 'ND', 'OR', 'SC', 'WI', 'MO']

TODO: Unique by special field
TODO: Need to explain computed fields first

.count()
========

Return how many objects are there on that queryset

.. code-block:: Python

  >>> data = fwf_open(HumanFileSpec, "sample_data/humans.txt")
  >>> # Total
  >>> len(data)
  10012
  >>> data.count()  # Sames as len(data)
  10012
  >>> # How many are women
  >>> data.filter(op("gender")=="F").count()
  4979
  >>> # How many womans from New York or California
  >>> data.filter(op("gender")=="F", op("state") in ["NY", "CA"]).count()
  197
  >>> # How many mens born on 1960 or later
  >>> data.filter(op("gender")=="M").exclude(op("birthday") < "19600101").count()
  4321

.values(\*fields)
=================

Like we changed the order of the header fields, the same approach applies to
selecting which fields to print.

.. code-block:: Python

  >>> TODO parsed = CompleteHumanFileParser.open("sample_data/humans.txt")
  >>> first5 = data[:5]
  >>> first5.header("name")
  +------------------+
  | name             |
  +------------------+
  | Dianne Mcintosh  |
  | Rosalyn Clark    |
  | Shirley Gray     |
  | Georgia Frank    |
  | Virginia Lambert |
  +------------------+
  >>> first5.header("name", "state")
  +------------------+-------+
  | name             | state |
  +------------------+-------+
  | Dianne Mcintosh  | AR    |
  | Rosalyn Clark    | MI    |
  | Shirley Gray     | WI    |
  | Georgia Frank    | MD    |
  | Virginia Lambert | PA    |
  +------------------+-------+
  >>> # If no field is specified it will reset it
  >>> first5.header()
  +------------------+--------+----------+----------+-------+----------+--------------+
  | name             | gender | birthday | location | state | universe | profession   |
  +------------------+--------+----------+----------+-------+----------+--------------+
  | Dianne Mcintosh  | F      | 19570526 | US       | AR    | Whatever | Medic        |
  | Rosalyn Clark    | M      | 19940213 | US       | MI    | Whatever | Comedian     |
  | Shirley Gray     | M      | 19510403 | US       | WI    | Whatever | Comedian     |
  | Georgia Frank    | F      | 20110508 | US       | MD    | Whatever | Comedian     |
  | Virginia Lambert | M      | 19930404 | US       | PA    | Whatever | Shark tammer |
  +------------------+--------+----------+----------+-------+----------+--------------+

# TODO
There are also 2 hidden fields that may be used, if needed:

- "_lineno": The line number (record number) within the original file, excluding leading comments
- "_line": The unchanged and unparsed original line, with original
  line breakers at the end

.. code-block:: Python

  >>> TODO parsed = CompleteHumanFileParser.open("sample_data/humans.txt")
  >>> sorted = data.order_by("birthday")[:5].header("_line_number", "name")
  +--------------+------------------+
  | _line_number | name             |
  +--------------+------------------+
  | 4328         | John Cleese      |
  | 9282         | Johnny Andres    |
  | 8466         | Oscar Callaghan  |
  | 3446         | Gilbert Garcia   |
  | 6378         | Helen Villarreal |
  +--------------+------------------+
  >>> # or a little hacking to add it
  >>> data.order_by("birthday")[:5].header("_line_number", *data.fields.names)
  +--------------+------------------+--------+----------+----------+-------+--------------+------------+
  | _line_number | name             | gender | birthday | location | state | universe     | profession |
  +--------------+------------------+--------+----------+----------+-------+--------------+------------+
  | 4328         | John Cleese      | M      | 19391027 | UK       |       | Monty Python | Comedian   |
  | 9282         | Johnny Andres    | F      | 19400107 | US       | TX    | Whatever     | Student    |
  | 8466         | Oscar Callaghan  | M      | 19400121 | US       | ID    | Whatever     | Comedian   |
  | 3446         | Gilbert Garcia   | M      | 19400125 | US       | NC    | Whatever     | Student    |
  | 6378         | Helen Villarreal | F      | 19400125 | US       | MD    | Whatever     |            |
  +--------------+------------------+--------+----------+----------+-------+--------------+------------+
  >>> # Note the trailing whitespaces and breakline on _unparsed_line
  >>> data[:5].header("_line_number", "_unparsed_line")
  +--------------+-----------------------------------------------------------------------------------+
  | _line_number | _unparsed_line                                                                    |
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
  >>> data[:5].header("_line_number", "_unparsed_line").to_list()
  [(1, 'US       AR19570526Fbe56008be36eDianne Mcintosh         Whatever    Medic        \n'),
      (2, 'US       MI19940213M706a6e0afc3dRosalyn Clark           Whatever    Comedian     \n'),
      (3, 'US       WI19510403M451ed630accbShirley Gray            Whatever    Comedian     \n'),
      (4, 'US       MD20110508F7e5cd7324f38Georgia Frank           Whatever    Comedian     \n'),
      (5, 'US       PA19930404Mecc7f17c16a6Virginia Lambert        Whatever    Shark tammer \n')]


fwf.BaseLineParser
===================

This is the class responsible for the actual parsing which has to be
extended to set its parsing map, as explained in [Setting up your
parser](#setting_up_your_parser). It's also responsible for all the
magic before and after parsing by means of the `_before_parse()` and
`_after_parse()` methods

_before_parse()
===============

This method is called before the line is parsed. At this point `self` has:

- self._unparsed_line: Original unchanged line
- self._parsable_line: Line to be parsed. If None then self._unparsed_line wil be used
- self._line_number: File line number
- self._headers: Name of all soon-to-be-available fields
- self._map: The field mapping for the parsing

Use it to pre-filter, pre-validate or process the line before parsing.

.. code-block:: Python

  from collections import OrderedDict
  from fwf import BaseLineParser, InvalidLineError

  class CustomLineParser(BaseLineParser):
      """Validated, uppercased U.S.A-only humans."""

      _map = OrderedDict(
          [
              ("name", slice(32, 56)),
              ("gender", slice(19, 20)),
              ("birthday", slice(11, 19)),
              ("location", slice(0, 9)),
              ("state", slice(9, 11)),
              ("universe", slice(56, 68)),
              ("profession", slice(68, 81)),
          ]
      )

      def _before_parse(self):
          """Do some pre-processing before the parsing."""
          # Validate line size to avoid malformed lines
          # an InvalidLineError will make this line to be skipped.
          # Any other error will break the parsing
          if not len(self._unparsed_line) == 82:
              raise InvalidLineError()

          # Since we know that the first characters are reserved for location, we
          # pre-filter any person that is not from US even before parsing the line.
          # Which is very efficient.
          if not self._unparsed_line.startswith("US"):
              raise InvalidLineError()

          # Then put everything uppercased
          self._parsable_line = self._unparsed_line.upper()

          # Note that instead of changing self._unparsed_line, self._parsable_line
          # is update. Preferably the unparsed value should be read-only. This is
          # useful e.g. for debugging.

Then use it as you like:

.. code-block:: Python

  >>> parsed = BaseFileParser.open("sample_data/humans.txt", CustomLineParser)
  >>> parsed.objects[:5]
  +------------------+--------+----------+----------+-------+----------+--------------+
  | name             | gender | birthday | location | state | universe | profession   |
  +------------------+--------+----------+----------+-------+----------+--------------+
  | DIANNE MCINTOSH  | F      | 19570526 | US       | AR    | WHATEVER | MEDIC        |
  | ROSALYN CLARK    | M      | 19940213 | US       | MI    | WHATEVER | COMEDIAN     |
  | SHIRLEY GRAY     | M      | 19510403 | US       | WI    | WHATEVER | COMEDIAN     |
  | GEORGIA FRANK    | F      | 20110508 | US       | MD    | WHATEVER | COMEDIAN     |
  | VIRGINIA LAMBERT | M      | 19930404 | US       | PA    | WHATEVER | SHARK TAMMER |
  +------------------+--------+----------+----------+-------+----------+--------------+
  >>> # Note that everything is uppercased
  >>> # And there is nobody who is not from US
  >>> # And almost without performance impact.
  >>> parsed.objects.exclude(location="US").count()
  0
  >>> parsed.objects.unique("location")
  ['US']

_after_parse()
==============

This method is called after the line is parsed. At this point line has been parsed
and it users may create new fields, alter some existing ones or combine them.
Filtering is also also still possible.

.. code-block:: Python

  from datetime import datetime
  from collections import OrderedDict
  from fwf import BaseLineParser, InvalidLineError


  class CustomLineParser(BaseLineParser):
      """Age-available, address-set employed human."""

      _map = OrderedDict(
          [
              ("name", slice(32, 56)),
              ("gender", slice(19, 20)),
              ("birthday", slice(11, 19)),
              ("location", slice(0, 9)),
              ("state", slice(9, 11)),
              ("universe", slice(56, 68)),
              ("profession", slice(68, 81)),
          ]
      )

      def _after_parse(self):
          """Customization on parsed line object."""
          try:
              # Parse birthday as datetime.date object
              self.birthday = datetime.strptime(self.birthday, "%Y%m%d").date()
          except ValueError:
              # There is some "unknown" values on my example file so I decided to
              # set birthday to 1900-01-01 as fail-over. I also could just skip
              # those lines by raising InvalidLineError
              self.birthday = datetime(1900, 1, 1).date()

          # Set a new attribute 'age'
          # Yeah, I know, it's not the proper way to calc someone's age but ...
          self.age = datetime.today().year - self.birthday.year

          # Combine 'location' and 'state' to create 'address' field
          self.address = "{}, {}".format(self.location, self.state)
          # and remove location and state
          del self.location
          del self.state

          # then update table headers so 'age' and 'address' become available and
          # 'location' and 'state' are removed.
          self._update_headers()
          # Please note that the new columns have been added at the end of the
          # table. If you want some specific column order just set self._headers
          # manually

          # And also skip those who does not have a profession
          if not self.profession:
              raise InvalidLineError()

Then just use as you like

.. code-block:: Python

  >>> parsed = BaseFileParser.open("sample_data/humans.txt", CustomLineParser)
  >>> parsed.objects[:5]
  +------------------+--------+------------+----------+--------------+---------+-----+
  | name             | gender | birthday   | universe | profession   | address | age |
  +------------------+--------+------------+----------+--------------+---------+-----+
  | Dianne Mcintosh  | F      | 1957-05-26 | Whatever | Medic        | US, AR  | 60  |
  | Rosalyn Clark    | M      | 1994-02-13 | Whatever | Comedian     | US, MI  | 23  |
  | Shirley Gray     | M      | 1951-04-03 | Whatever | Comedian     | US, WI  | 66  |
  | Georgia Frank    | F      | 2011-05-08 | Whatever | Comedian     | US, MD  | 6   |
  | Virginia Lambert | M      | 1993-04-04 | Whatever | Shark tammer | US, PA  | 24  |
  +------------------+--------+------------+----------+--------------+---------+-----+
  >>> # Note that birthday is now a datetime.date instance
  >>> parsed.objects[0].birthday
  datetime.date(1957, 5, 26)
  >>> # and you can use datetime attributes as special filters
  >>> parsed.objects.filter(birthday__day=4, birthday__month=7)[:5]
  +--------------------+--------+------------+----------+------------+---------+-----+
  | name               | gender | birthday   | universe | profession | address | age |
  +--------------------+--------+------------+----------+------------+---------+-----+
  | Christopher Symons | M      | 2006-07-04 | Whatever | Comedian   | US, LA  | 11  |
  | Thomas Hughes      | F      | 2012-07-04 | Whatever | Medic      | US, PA  | 5   |
  | Anthony French     | F      | 2012-07-04 | Whatever | Student    | US, ND  | 5   |
  | Harry Carson       | M      | 1989-07-04 | Whatever | Student    | US, AK  | 28  |
  | Margaret Walks     | M      | 2012-07-04 | Whatever | Comedian   | US, AZ  | 5   |
  +--------------------+--------+------------+----------+------------+---------+-----+
  >>> parsed.objects.filter(birthday__gte=datetime(2000, 1, 1).date()).order_by("birthday")[:5]
  +---------------+--------+------------+----------+--------------+---------+-----+
  | name          | gender | birthday   | universe | profession   | address | age |
  +---------------+--------+------------+----------+--------------+---------+-----+
  | Peggy Brinlee | M      | 2000-01-01 | Whatever | Programmer   | US, CO  | 17  |
  | Tamara Kidd   | M      | 2000-01-03 | Whatever | Artist       | US, MN  | 17  |
  | Victor Fraley | M      | 2000-01-04 | Whatever | Shark tammer | US, IL  | 17  |
  | Joyce Lee     | F      | 2000-01-05 | Whatever | Programmer   | US, KY  | 17  |
  | Leigh Harley  | M      | 2000-01-06 | Whatever | Programmer   | US, NM  | 17  |
  +---------------+--------+------------+----------+--------------+---------+-----+
  >>> # And age is also usable
  >>> parsed.objects.filter(age=18)[:5]
  +------------------+--------+------------+----------+--------------+---------+-----+
  | name             | gender | birthday   | universe | profession   | address | age |
  +------------------+--------+------------+----------+--------------+---------+-----+
  | Gladys Martin    | F      | 1999-01-23 | Whatever | Medic        | US, WY  | 18  |
  | Justin Salinas   | M      | 1999-07-03 | Whatever | Shark tammer | US, ND  | 18  |
  | Sandra Carrousal | F      | 1999-10-09 | Whatever | Super hero   | US, NH  | 18  |
  | Edith Briggs     | F      | 1999-04-05 | Whatever | Medic        | US, AL  | 18  |
  | Patrick Mckinley | F      | 1999-03-18 | Whatever | Comedian     | US, ME  | 18  |
  +------------------+--------+------------+----------+--------------+---------+-----+
  >>> parsed.objects.filter(age__lt=18).order_by("age", reverse=True)[:5]
  +--------------------+--------+------------+----------+--------------+---------+-----+
  | name               | gender | birthday   | universe | profession   | address | age |
  +--------------------+--------+------------+----------+--------------+---------+-----+
  | Angela Armentrout  | F      | 2000-12-21 | Whatever | Artist       | US, MO  | 17  |
  | Christine Strassel | F      | 2000-10-22 | Whatever | Medic        | US, NE  | 17  |
  | Christopher Pack   | M      | 2000-03-22 | Whatever | Student      | US, IN  | 17  |
  | Manuela Lytle      | M      | 2000-12-18 | Whatever | Shark tammer | US, NV  | 17  |
  | Tamara Kidd        | M      | 2000-01-03 | Whatever | Artist       | US, MN  | 17  |
  +--------------------+--------+------------+----------+--------------+---------+-----+

fwf.BaseFileParser
====================

This class will read all file data and needs a line parser to do the
actual parsing. So you will need a class extended from
`BaseLineParser`. I'll consider that you
already have your CustomLineParser class so:

.. code-block:: Python

  >>> from fwf import BaseFileParser
  >>> # Let's say that you already created your CustomLineParser class
  >>> parsed = BaseFileParser.open("sample_data/humans.txt", CustomLineParser)
  >>> parsed.objects[:5]
  +------------------+--------+----------+----------+-------+----------+--------------+
  | name             | gender | birthday | location | state | universe | profession   |
  +------------------+--------+----------+----------+-------+----------+--------------+
  | Dianne Mcintosh  | F      | 19570526 | US       | AR    | Whatever | Medic        |
  | Rosalyn Clark    | M      | 19940213 | US       | MI    | Whatever | Comedian     |
  | Shirley Gray     | M      | 19510403 | US       | WI    | Whatever | Comedian     |
  | Georgia Frank    | F      | 20110508 | US       | MD    | Whatever | Comedian     |
  | Virginia Lambert | M      | 19930404 | US       | PA    | Whatever | Shark tammer |
  +------------------+--------+----------+----------+-------+----------+--------------+

Or you may extend BaseFileParser for semantics sake

.. code-block:: Python

  from fwf import BaseFileParser

  class HumanParser(BaseFileParser):
      """File parser for humans.txt example file."""

      # Let's say that you already created your CustomLineParser class
      _line_parser = CustomLineParser

Now you just

.. code-block:: Python

  >>> parsed = HumanParser.open("sample_data/humans.txt")
  >>> parsed.objects[:5]
  +------------------+--------+----------+----------+-------+----------+--------------+
  | name             | gender | birthday | location | state | universe | profession   |
  +------------------+--------+----------+----------+-------+----------+--------------+
  | Dianne Mcintosh  | F      | 19570526 | US       | AR    | Whatever | Medic        |
  | Rosalyn Clark    | M      | 19940213 | US       | MI    | Whatever | Comedian     |
  | Shirley Gray     | M      | 19510403 | US       | WI    | Whatever | Comedian     |
  | Georgia Frank    | F      | 20110508 | US       | MD    | Whatever | Comedian     |
  | Virginia Lambert | M      | 19930404 | US       | PA    | Whatever | Shark tammer |
  +------------------+--------+----------+----------+-------+----------+--------------+

.open(filename, line_parser=None)
==================================

This class method opens the given file, parses it, closes it and
returns a parsed file instance. Pretty much every example here is using
`.open()`

You may define your line parser class here, if you want, but I suggest you
extend BaseFileParser to set you line parser there.

Parse an already opened file
----------------------------

You may also parse an already opened file, StringIO, downloaded file or
any IO instance that you have:

.. code-block:: Python

  >>> from fwf import BaseFileParser
  >>> # Let's say that you already created your CustomLineParser class
  >>> f = open("sample_data/humans.txt", "r")
  >>> parsed = BaseFileParser(f, CustomLineParser)
  >>> # Always remember to close your files or use "with" statement to do so
  >>> f.close()
  >>> parsed.objects[:5]
  +------------------+--------+----------+----------+-------+----------+--------------+
  | name             | gender | birthday | location | state | universe | profession   |
  +------------------+--------+----------+----------+-------+----------+--------------+
  | Dianne Mcintosh  | F      | 19570526 | US       | AR    | Whatever | Medic        |
  | Rosalyn Clark    | M      | 19940213 | US       | MI    | Whatever | Comedian     |
  | Shirley Gray     | M      | 19510403 | US       | WI    | Whatever | Comedian     |
  | Georgia Frank    | F      | 20110508 | US       | MD    | Whatever | Comedian     |
  | Virginia Lambert | M      | 19930404 | US       | PA    | Whatever | Shark tammer |
  +------------------+--------+----------+----------+-------+----------+--------------+

.objects attribute
====================

Your parsed file has an `.objects` attribute. Which is a `queryset` consisting
of all record, excluding the ones filtered in-line.

Development
============

We are using a virtual env (`.venv`) for dependencies. And given the chosen
file structure (`./src` directory; `./tests` directory without `__init__.py`), we do
`pip install -e .` to install the project in '.' as a local package, with
development enabled (-e).

Test execution: `pytest tests\...`
