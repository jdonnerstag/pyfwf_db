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


Jupyter Notebook
=================

The package contains a Jupyter notebook `./sample_data/intro.ipynb`, which
many examples on how to use the library and its most important features.


Installation
============

Standard python `pip`.

.. code-block:: Python

  pip install git+https://github.com/jdonnerstag/pyfwf_db.git


Development
============

We are using a virtual env (`.venv`) for dependencies. And given the chosen
file structure (`./src` directory; `./tests` directory without `__init__.py`), we do
`pip install -e .` to install the project in '.' as a local package, with
development enabled (-e).

Test execution: `pytest -sx tests\...`

Build the cython exentions only: ./build_ext.bat
