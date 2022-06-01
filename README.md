FWF - Fixed-Width-Field File Format parser and tools
=======================================================

A lib to help you handle fixed-width files that join all data relying only on
the length of each field. This lib is especially targetted to users that

- don't want to wait anymore for whatever ingest or import job to finish
- need to handle data sets which are too large to fit into memory
- receive new files of the same kind regulary and need to access them
  as if it were one large file.
- need to lookup rows within a single virtual table and across multiple
  virtual tables of different kind. But no analytics, reporting or
  number crunching. You can though export filtered subsets into specialised
  libs and tools if needed.
- want indexes for speedy access because executing large number of lookups
  against these data sets are your main concern
- want to filter the data because you don't need them all, and you want
  to index just the filtered data
- need group-by etc. to prepare your index. E.g. just keep the lastest
  entry from each group
- want to persist the indexes easily and quickly to avoid always rebuilding
  them.
- want casts and transformations to convert only the bytes needed to
  strings, ints or dates
- occassionally need to process files that don't have standard line endings,
  but \x00 or similar
- are ok with the constraint that the files must be in a file-system (rather
  then object store or similar) or fit into memory. File from a file system
  can be much larger then memory, because the lib memory maps the file. Files
  which are compressed or from an object store can still be processed, but
  must fit into memory.
- are ok with field lengths in bytes rather then chars. UTF-8 chars require
  1-6 bytes, which leads to variable line lengths in bytes. The lib
  however relies on a constant line length in bytes.

How did we get here? Building this lib wasn't our first thought:
- We need lots of lookups across multiple tables. We are using traditional
  RDBMS systems a lot and have really experienced people. But ingesting and
  preparing (staging) the data took ages. We applied partitioning, and all kind
  of tricks to speed up ingest and lookups, but it remained painful, slow and
  also comparatively expensive. We've tested it on-premise and in public clouds,
  including rather big boxes with sufficient I/O and network bandwidth.
- We tried NoSql but following best pratices, it is adviced to create a
  schema that matches your queries best. Hence more complexity in the ingest
  layer. This and because network latency for queries don't go away, did not
  make us happy.
- Several of us have laptops with 24GB RAM and we initially started with
  a 5GB data set in uncompressed fixed-width files. We tried to load them with
  Pandas, but quickly run into out-of-memory exceptions, even with in-stream
  filtering of records upon ingest. There are several blogs alluding to a
  factor 5 between raw data and memory consumption. Once loaded, the
  performance was perfect.
- With our little lib,
  - we completely avoid any load or ingest job. We can start using new
    files as soon as they arrive.
  - A full index scan, takes less then 2 mins on our standard business
    laptops (with SDD). Multiple times faster than the other
    options, and on low-cost hardware (vs big boxes and high-speed networks)
  - With Pandas based indexes it is very fast to identify the line, but
    potentially loading it from disc and converting the line and required
    fields / columns from bytes into consumable data types, is a bit slower
    compared to in-memory preprocessed Pandas dataframes. But we need to do
    millions of lookups and it is working well for us.
 - We've tested it with 100GB data sets, slowly approach memory limits
   for full table (in-memory) indexes. Obviously depending on the number
   of rows and size of the index key.


Table of Contents
-----------------

1. `When Should I NOT Use This? <#when-should-i-not-use-this>`__
2. `Why Should I Use This? <#why-should-i-use-this?>`__
3. `Features <#features>`__
4. `Instalation <#instalation>`__
5. `Usage <#usage>`__

   1. `Setting up your parser <#setting-up-your-parser>`__
   2. `Queryset <#queryset>`__

      -  `.filter() <#filterkwargs>`__
      -  `.exclude() <#excludekwargs>`__
      -  `.order\_by() <#order_byfield_name-reversefalse>`__
      -  `.unique() <#uniquefield_name>`__
      -  `.count() <#count>`__
      -  `.values() <#valuesfields>`__

   3. `Models <#models>`__

      1. `BaseLineParser <#fwfbaselineparser>`__

         -  `\_before\_parse() <#_before_parse>`__
         -  `\_after\_parse() <#_after_parse>`__

      2. `BaseFileParser <#fwfbasefileparser>`__

         -  `.open() <#openfilename-line_parsernone>`__
         -  `.objects <#objects-attribute>`__

6. `TODOs <#todos>`__

When should I use this?
-----------------------

When you have files like this:

::

    USAR19570526Fbe56008be36eDianne Mcintosh WhateverMedic        \n
    USMI19940213M706a6e0afc3dRosalyn Clark   WhateverComedian     \n
    USWI19510403M451ed630accbShirley Gray    WhateverComedian     \n
    USMD20110508F7e5cd7324f38Georgia Frank   WhateverComedian     \n
    USPA19930404Mecc7f17c16a6Virginia LambertWhateverShark tammer \n
    USVT19770319Fd2bd88100facRichard Botto   WhateverTime traveler\n
    USOK19910917F9c704139a6e3Alberto Giel    WhateverStudent      \n
    USNV20120604F5f02187599d7Mildred Henke   WhateverSuper hero   \n
    USRI19820125Fcf54b2eb5219Marc Kidd       WhateverMedic        \n
    USME20080503F0f51da89a299Kelly Crose     WhateverComedian     \n
    ...

where each line represents one dataset and the data is concatenated on
that line.

Why should I use this?
----------------------

Even though parsing this kind of file is pretty easy you may want to
filter some of its data. Also this lib helps you to visualize your data
and play with it directly on python shell. So that the file will easily
become this:

::

    +------------------+--------+------------+----------+-------+----------+---------------+-----+
    | name             | gender | birthday   | location | state | universe | profession    | age |
    +------------------+--------+------------+----------+-------+----------+---------------+-----+
    | Dianne Mcintosh  | F      | 1957-05-26 | US       | AR    | Whatever | Medic         | 60  |
    | Rosalyn Clark    | M      | 1994-02-13 | US       | MI    | Whatever | Comedian      | 23  |
    | Shirley Gray     | M      | 1951-04-03 | US       | WI    | Whatever | Comedian      | 66  |
    | Georgia Frank    | F      | 2011-05-08 | US       | MD    | Whatever | Comedian      | 6   |
    | Virginia Lambert | M      | 1993-04-04 | US       | PA    | Whatever | Shark tammer  | 24  |
    | Richard Botto    | F      | 1977-03-19 | US       | VT    | Whatever | Time traveler | 40  |
    | Alberto Giel     | F      | 1991-09-17 | US       | OK    | Whatever | Student       | 26  |
    | Mildred Henke    | F      | 2012-06-04 | US       | NV    | Whatever | Super hero    | 5   |
    | Marc Kidd        | F      | 1982-01-25 | US       | RI    | Whatever | Medic         | 35  |
    | Kelly Crose      | F      | 2008-05-03 | US       | ME    | Whatever | Comedian      | 9   |
    +------------------+--------+------------+----------+-------+----------+---------------+-----+

Features
--------

-  Parse and objectify your file
-  Filter objects using a django-like syntax
-  Reorganize your data
-  Vizualization as table (thanks to
   `terminaltables <https://robpol86.github.io/terminaltables/>`__)
-  Order by column
-  Add or remove columns
-  Count a subset
-  Uniqueness of data on a column

Instalation
===========

Just use pip

::

    pip install fwf


Usage
=====

Setting up your parser
----------------------

First thing you need to know is the width of each column on your file.
There's no magic here. You will need to find out.

Lets take `this
file <https://raw.githubusercontent.com/nano-labs/pyfwf3/master/examples/humans.txt>`__
as example. Here its first line:

::

    'US       AR19570526Fbe56008be36eDianne Mcintosh         Whatever    Medic        \n'

By testing, splitting, trying or whatever I know that: - First 9
characters are the location - Next 2 characters are the state - Next 8 are the
birthday - Next 1 is the gender - Next 12 I dont have a clue and I dont
care - Next 24 are the name

and so on. But I only want name, birthday and gender so let's write it's
model

.. code:: python

    from fwf import BaseLineParser

    class Human(BaseLineParser):
        """Parser for each line of that humans.txt file."""

        _map = {"name": slice(32, 56),
                "gender": slice(19, 20),
                "birthday": slice(11, 19)}

The slices represents the first and last positions of each information
in the line and that's the most basic line parser you need. Now we are
going to use it with the file parser.

.. code:: python

    from pyfwf import BaseFileParser

    parsed = BaseFileParser.open("examples/humans.txt", line_parser=Human)

That's it. Your file is parsed and now usable but let's put it together:

.. code:: python

    from fwf import BaseLineParser, BaseFileParser

    class Human(BaseLineParser):
        """Parser for each line of that humans.txt file."""

        _map = {"name": slice(32, 56),
                "gender": slice(19, 20),
                "birthday": slice(11, 19)}


    parsed = BaseFileParser.open("examples/humans.txt", line_parser=Human)

or even

.. code:: python

    from fwf import BaseLineParser, BaseFileParser

    class Human(BaseLineParser):
        """Parser for each line of that humans.txt file."""

        _map = {"name": slice(32, 56),
                "gender": slice(19, 20),
                "birthday": slice(11, 19)}


    class HumanFileParser(BaseFileParser):
        """Parser for that humans.txt file."""

        _line_parser = Human

    parsed = HumanFileParser.open("examples/humans.txt")

We will discuss those classes in the `future <#fwfbaselineparser>`__

Queryset
--------

With your parsed file as a BaseFileParser instance you have all objects
stored as a Queryset instance in the "objects" attribute. So:

.. code:: pycon

    >>> parsed = HumanFileParser.open("examples/humans.txt")
    >>> # slices returns a smaller queryset instance
    >>> parsed.objects[0:5]
    +------------------+----------+--------+
    | name             | birthday | gender |
    +------------------+----------+--------+
    | Dianne Mcintosh  | 19570526 | F      |
    | Rosalyn Clark    | 19940213 | M      |
    | Shirley Gray     | 19510403 | M      |
    | Georgia Frank    | 20110508 | F      |
    | Virginia Lambert | 19930404 | M      |
    +------------------+----------+--------+
    >>> # while getting a specific item returns a parsed line instance
    >>> parsed.objects[327]
    +------------+----------+--------+
    | name       | birthday | gender |
    +------------+----------+--------+
    | Jack Brown | 19490106 | M      |
    +------------+----------+--------+
    >>> # Note that the table is only a shell representation of the objects
    >>> parsed.objects[327].name
    'Jack Brown'
    >>> parsed.objects[327].birthday
    '19490106'
    >>> parsed.objects[327].gender
    'M'
    >>> tuple(parsed.objects[327])
    ('M', 'Jack Brown', '19490106')
    >>> list(parsed.objects[327])
    ['M', 'Jack Brown', '19490106']
    >>> # To prevent the fields from changing order use OrderedDict instead of dict on _map. More about that later

.filter(\*\*kwargs)
-------------------

Here is where the magic happens. A filtered queryset will always return
a new queryset that can be filtered too and so on

.. code:: pycon

    >>> parsed = HumanFileParser.open("examples/humans.txt")
    >>> first5 = parsed.objects[:5]
    >>> # 'first5' is a Queryset instance just as 'parsed.objects' but with only a few objects
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
    >>> first5.filter(gender="F")
    +------------------+----------+--------+
    | name             | birthday | gender |
    +------------------+----------+--------+
    | Dianne Mcintosh  | 19570526 | F      |
    | Georgia Frank    | 20110508 | F      |
    +------------------+----------+--------+
    >>> # with multiple keywords arguments
    >>> first5.filter(gender="M", birthday__gte="19900101")
    +------------------+----------+--------+
    | name             | birthday | gender |
    +------------------+----------+--------+
    | Rosalyn Clark    | 19940213 | M      |
    | Virginia Lambert | 19930404 | M      |
    +------------------+----------+--------+
    >>> # or chained filters
    >>> first5.filter(name__endswith="k").filter(gender=F)
    +------------------+----------+--------+
    | name             | birthday | gender |
    +------------------+----------+--------+
    | Georgia Frank    | 20110508 | F      |
    +------------------+----------+--------+

Some special filters may be used with \_\_ notation. Here are some but
not limited to:

- \_\_in: value is in a list
- \_\_lt: less than
- \_\_lte: less than or equals
- \_\_gt: greater than
- \_\_gte: greater than or equals
- \_\_ne: not equals
- \_\_len: field lenght (without trailing spaces)
- \_\_startswith: value starts with that string
- \_\_endswith: value ends with that string

It will actually look for any attribute or method of the field object
that matches with **'object.somefilter'** or
**'object.\_\_somefilter\_\_'** and call it or compare with it. So let's
say that you use the `\_after\_parse() <#_after_parse>`__ method to
convert the **'birthday'** field into **datetime.date** instances you
can now filter using, for example, **.filter(birthday\_\_year=1957)**

.exclude(\*\*kwargs)
--------------------

Pretty much the opposite of `.filter() <#filterkwargs>`__

.. code:: pycon

    >>> parsed = HumanFileParser.open("examples/humans.txt")
    >>> first5 = parsed.objects[:5]
    >>> firts5
    +------------------+----------+--------+
    | name             | birthday | gender |
    +------------------+----------+--------+
    | Dianne Mcintosh  | 19570526 | F      |
    | Rosalyn Clark    | 19940213 | M      |
    | Shirley Gray     | 19510403 | M      |
    | Georgia Frank    | 20110508 | F      |
    | Virginia Lambert | 19930404 | M      |
    +------------------+----------+--------+
    >>> first5.exclude(gender="F")
    +------------------+----------+--------+
    | name             | birthday | gender |
    +------------------+----------+--------+
    | Rosalyn Clark    | 19940213 | M      |
    | Shirley Gray     | 19510403 | M      |
    | Virginia Lambert | 19930404 | M      |
    +------------------+----------+--------+

.order\_by(field\_name, reverse=False)
--------------------------------------

Reorder the whole queryset sorting by that given field

.. code:: pycon

    >>> parsed = HumanFileParser.open("examples/humans.txt")
    >>> parsed.objects[:5]
    +------------------+----------+--------+
    | name             | birthday | gender |
    +------------------+----------+--------+
    | Dianne Mcintosh  | 19570526 | F      |
    | Rosalyn Clark    | 19940213 | M      |
    | Shirley Gray     | 19510403 | M      |
    | Georgia Frank    | 20110508 | F      |
    | Virginia Lambert | 19930404 | M      |
    +------------------+----------+--------+
    >>> parsed.objects[:5].order_by("name")
    +------------------+--------+----------+
    | name             | gender | birthday |
    +------------------+--------+----------+
    | Dianne Mcintosh  | F      | 19570526 |
    | Georgia Frank    | F      | 20110508 |
    | Rosalyn Clark    | M      | 19940213 |
    | Shirley Gray     | M      | 19510403 |
    | Virginia Lambert | M      | 19930404 |
    +------------------+--------+----------+
    >>> parsed.objects[:5].order_by("name", reverse=True)
    +------------------+--------+----------+
    | name             | gender | birthday |
    +------------------+--------+----------+
    | Virginia Lambert | M      | 19930404 |
    | Shirley Gray     | M      | 19510403 |
    | Rosalyn Clark    | M      | 19940213 |
    | Georgia Frank    | F      | 20110508 |
    | Dianne Mcintosh  | F      | 19570526 |
    +------------------+--------+----------+

TODO: Order by more than one field and order by special field

.unique(field\_name)
--------------------

Return a list o unique values for that field. For this example I will
use complete line parser for that humans.txt file

.. code:: python

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

.. code:: pycon

    >>> parsed = CompleteHumanFileParser.open("examples/humans.txt")
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

.count()
--------

Return how many objects are there on that queryset

.. code:: pycon

    >>> parsed = CompleteHumanFileParser.open("examples/humans.txt")
    >>> # Total
    >>> parsed.objects.count()
    10012
    >>> # How many are women
    >>> parsed.objects.filter(gender="F").count()
    4979
    >>> # How many womans from New York or California
    >>> parsed.objects.filter(gender="F", state__in=["NY", "CA"]).count()
    197
    >>> # How many mens born on 1960 or later
    >>> parsed.objects.filter(gender="M").exclude(birthday__lt="19600101").count()
    4321

.values(\*fields)
-----------------

This method should be used to actually return data from a queryset. It
returns the specified fields only or all of them if none is specified.

Returns a **ValuesList** instance which is in fact a extended **list**
object with overwriten **\_\_repr\_\_** method just to look like a table
on shell, so on every other aspect it is a list. May be a list of tuples,
if more the one column is returned, or a simple list if only one field
was specified

.. code:: pycon

    >>> parsed = CompleteHumanFileParser.open("examples/humans.txt")
    >>> parsed.objects[:5].values("name")
    +------------------+
    | name             |
    +------------------+
    | Dianne Mcintosh  |
    | Rosalyn Clark    |
    | Shirley Gray     |
    | Georgia Frank    |
    | Virginia Lambert |
    +------------------+
    >>> # even though it looks like a table it is actually a list
    >>> parsed.objects[:5].values("name")[:]
    ['Dianne Mcintosh',
     'Rosalyn Clark',
     'Shirley Gray',
     'Georgia Frank',
     'Virginia Lambert']
    >>> parsed.objects[:5].values("name", "state")
    +------------------+-------+
    | name             | state |
    +------------------+-------+
    | Dianne Mcintosh  | AR    |
    | Rosalyn Clark    | MI    |
    | Shirley Gray     | WI    |
    | Georgia Frank    | MD    |
    | Virginia Lambert | PA    |
    +------------------+-------+
    >>> # or a list o tuples
    >>> parsed.objects[:5].values("name", "state")[:]
    [('Dianne Mcintosh', 'AR'),
     ('Rosalyn Clark', 'MI'),
     ('Shirley Gray', 'WI'),
     ('Georgia Frank', 'MD'),
     ('Virginia Lambert', 'PA')]
    >>> # If no field is specified it will return all
    >>> parsed.objects[:5].values()
    +------------------+--------+----------+----------+-------+----------+--------------+
    | name             | gender | birthday | location | state | universe | profession   |
    +------------------+--------+----------+----------+-------+----------+--------------+
    | Dianne Mcintosh  | F      | 19570526 | US       | AR    | Whatever | Medic        |
    | Rosalyn Clark    | M      | 19940213 | US       | MI    | Whatever | Comedian     |
    | Shirley Gray     | M      | 19510403 | US       | WI    | Whatever | Comedian     |
    | Georgia Frank    | F      | 20110508 | US       | MD    | Whatever | Comedian     |
    | Virginia Lambert | M      | 19930404 | US       | PA    | Whatever | Shark tammer |
    +------------------+--------+----------+----------+-------+----------+--------------+
    >>> parsed.objects[:5].values()[:]
    [('Dianne Mcintosh', 'F', '19570526', 'US', 'AR', 'Whatever', 'Medic'),
     ('Rosalyn Clark', 'M', '19940213', 'US', 'MI', 'Whatever', 'Comedian'),
     ('Shirley Gray', 'M', '19510403', 'US', 'WI', 'Whatever', 'Comedian'),
     ('Georgia Frank', 'F', '20110508', 'US', 'MD', 'Whatever', 'Comedian'),
     ('Virginia Lambert', 'M', '19930404', 'US', 'PA', 'Whatever', 'Shark tammer')]
    >>> # Note that you dont need to slice the result with '[:]'. I am only doing it to show the response structure behind the table representation

There are also 2 hidden fields that may be used, if needed:

- \_line\_number: The line number on the original file, counting even if some line is skipped during parsing
- \_unparsed\_line: The unchanged and unparsed original line, with original line breakers at the end

.. code:: pycon

    >>> parsed = CompleteHumanFileParser.open("examples/humans.txt")
    >>> parsed.objects.order_by("birthday")[:5].values("_line_number", "name")
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
    >>> parsed.objects.order_by("birthday")[:5].values("_line_number", *parsed._line_parser._map.keys())
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
    >>> parsed.objects[:5].values("_line_number", "_unparsed_line")
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
    >>> parsed.objects[:5].values("_line_number", "_unparsed_line")[:]
    [(1, 'US       AR19570526Fbe56008be36eDianne Mcintosh         Whatever    Medic        \n'),
     (2, 'US       MI19940213M706a6e0afc3dRosalyn Clark           Whatever    Comedian     \n'),
     (3, 'US       WI19510403M451ed630accbShirley Gray            Whatever    Comedian     \n'),
     (4, 'US       MD20110508F7e5cd7324f38Georgia Frank           Whatever    Comedian     \n'),
     (5, 'US       PA19930404Mecc7f17c16a6Virginia Lambert        Whatever    Shark tammer \n')]

TODO: Allow special fields to be used

Models
======

fwf.BaseLineParser
---------------------

This is the class responsible for the actual parsing which has to be
extended to set its parsing map, as explained in `Setting up your
parser <#setting-up-your-parser>`__. It's also responsible for all the
magic before and after parsing by the use of
`\_before\_parse() <#_before_parse>`__ and
`\_after\_parse() <#_after_parse>`__ methods

\_before\_parse()
-----------------

This method is called before the line is parsed. At this point **self** has:

- self.\_unparsed\_line: Original unchanged line
- self.\_parsable\_line: Line to be parsed. If None given self.\_unparsed\_line wil be used
- self.\_line\_number: File line number
- self.\_headers: Name of all soon-to-be-available fields
- self.\_map: The field mapping for the parsing

Use it to pre-filter, pre-validade or process the line before parsing.

Ex:

.. code:: python

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
            """Do some pre-process before the parsing."""
            # Validate line size to avoid malformed lines
            # an InvalidLineError will make this line to be skipped
            # any other error will break the parsing
            if not len(self._unparsed_line) == 82:
                raise InvalidLineError()

            # As I know that the first characters are reserved for location I will
            # pre-filter any person that are not from US even before parsing it
            if not self._unparsed_line.startswith("US"):
                raise InvalidLineError()

            # Then put everything uppercased
            self._parsable_line = self._unparsed_line.upper()
            # Note that instead of changing self._unparsed_line I've set the new
            # string to self._parsable_line. I don't want to loose the unparsed
            # value because it is useful for further debug

Then use it as you like

.. code:: pycon

    >>> parsed = BaseFileParser.open("examples/humans.txt", CustomLineParser)
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
    >>> parsed.objects.exclude(location="US").count()
    0
    >>> parsed.objects.unique("location")
    ['US']

\_after\_parse()
----------------

This method is called after the line is parsed. At this point you have a already parsed line
and now you may create new fields, alter some existing or combine those. You still may filter
some objects.

Ex:

.. code:: python

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
                # set birthday as 1900-01-01 as failover. I also could just skip
                # those lines by raising InvalidLineError
                self.birthday = datetime(1900, 1, 1).date()

            # Set a new attribute 'age'
            # Yeah, I know, it's not the proper way to calc someone's age but stil...
            self.age = datetime.today().year - self.birthday.year

            # Combine 'location' and 'state' to create 'address' field
            self.address = "{}, {}".format(self.location, self.state)
            # and remove location and state
            del self.location
            del self.state

            # then update table headers so 'age' and 'address' become available and
            # remove 'location' and 'state'
            self._update_headers()
            # You will note that the new columns will be added at the end of the
            # table. If you want some specific column order just set self._headers
            # manually

            # And also skip those who does not have a profession
            if not self.profession:
                raise InvalidLineError()

Then just use as you like

.. code:: pycon

    >>> parsed = BaseFileParser.open("examples/humans.txt", CustomLineParser)
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
---------------------

This class will read all file data and needs a line parser to do the
actual parsing. So you will need a class extended from
`BaseLineParser <#fwfbaselineparser>`__. I'll consider that you
already have your CustomLineParser class so:

.. code:: pycon

    >>> from fwf import BaseFileParser
    >>> # Let's say that you already created your CustomLineParser class
    >>> parsed = BaseFileParser.open("examples/humans.txt", CustomLineParser)
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

.. code:: python

    from fwf import BaseFileParser


    class HumanParser(BaseFileParser):
        """File parser for humans.txt example file."""

        # Let's say that you already created your CustomLineParser class
        _line_parser = CustomLineParser

Now you just

.. code:: pycon

    >>> parsed = HumanParser.open("examples/humans.txt")
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

.open(filename, line\_parser=None)
----------------------------------

This class method opens the given file, parses it, closes it and
returns a parsed file instance. Pretty much every example here is using
**.open()**

You may define your line parser class here, if you want, but for
semantics sake I recommend that you extend BaseFileParser to set you
line parser there.

Parse an already opened file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You also may parse an already opened file, StringIO, downloaded file or
any IO instance that you have. For that just create an instance directly

.. code:: pycon

    >>> from fwf import BaseFileParser
    >>> # Let's say that you already created your CustomLineParser class
    >>> f = open("examples/humans.txt", "r")
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

**.objects** attribute
----------------------

Your parsed file have a **.objects** attribute. That is your complete parsed
`queryset <#queryset>`__
