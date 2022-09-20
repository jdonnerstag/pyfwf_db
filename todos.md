
# TODOs

- Test setup.py and build process (wheels) with new structure.
   - IMPORTANT: install and test the generated wheel
- Make sure that everything is mentioned in readme, is actually working
- I originally used Pandas, but memory consumption was huge (5x raw data).
  Veax supports memory-mapped files, but I didn't see fixed-width files support.
  And I haven't tested yet, whether it suffers from the same memory problem.
  I have not double. My understanding of Dataframe, and the Dataframe interchange protocol,
   https://labs.quansight.org/blog/2021/10/dataframe-interchange-protocol-and-vaex/
   is, that it is very much for columnar data. Whereas our use case is much more a
   nosql / lookup by ID use case. Vaex/Panda are able to import python lists, which
   fwf_db is able to provide (lazy) in a row-by-row format. Obviously Pandas and
   Vaex will need to transpose the data upon load.
- Use fsspec to support remote files, incl. AWS, GCP, etc.. Should we do local
  file caching in this module, or rather create another re-useable one? Is it
  may be already in fsspec?
- Handle files with no break lines => That is done, isn't it? Test cases?
- Recursive special filters like: birthday\_\_year\_\_lt  <= field "birthday", extract only "year", "less then"
- Filter with same line like: .filter(start\_day=L("end\_day"))
- Multi-column order like: .order\_by("-age", "name")
- Values using special fields like: .values("name\_\_len") <= probably after trimming?
- Order using special fields like: .order\_by("birthday\_\_year") <= field "birthday", extract only "year"
- Export methods like: .sqlite file or .sql file
- Write a fixed-width field file (?)(why would someone write those files?)
- Consider using Nuitka as Python (to C++) Compiler
- Support ignore-case (convert to upper case) key and/or value.
- Use python module "black" for formatting the source code
- We are using setup.py. Do we still need requirements.txt?
- use "_" for protected variables and methods, and __ for private ones
- check that we are using __len__() rather then len(), and __iter__() rather then iter() etc.