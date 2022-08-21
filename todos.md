
# TODOs

- Test setup.py and build process (wheels) with new structure.
   - IMPORTANT: install and test the generated wheel
- Cleanup file structure, tests, etc.
- Make sure that everything is mentioned in readme, is actually working
- I'm wondering whether we could use DataFrame, like Pandas, Veax, etc.
   My understanding of Dataframe, and the Dataframe interchange protocol,
   https://labs.quansight.org/blog/2021/10/dataframe-interchange-protocol-and-vaex/
   is, that it is very much for columnar data. Whereas our use case is much more a
   nosql / lookup by ID use case. Vaex/Panda are able to import python lists, which
   fwf_db is able to provide (lazy) in a row-by-row format. Obviously Pandas and
   Vaex will need to transpose the data upon load.
- Use fsspec to support remote files, incl. AWS, GCP, etc.
- Should we do local file caching in this module, or rather create another
  re-useable one? Is it may be already in fsspec?
- Vaex is using memory mapped files as well. Elaborate whether it makes
  sense to develop a Vaex file reader, and leverage all other nice things
  that Vaex provides (on top of Pandas).
- Handle files with no break lines
- Recursive special filters like: birthday\_\_year\_\_lt
- Filter with same line like: .filter(start\_day=L("end\_day"))
- Multi-column order like: .order\_by("-age", "name")
- Values using special fields like: .values("name\_\_len")
- Order using special fields like: .order\_by("birthday\_\_year")
- Export methods like: .sqlite file or .sql file
- Write a fixed-width field file (?)(why would someone write those files?)
- Consider using Nuitka as Python Compiler
- Support ignore-case (convert to upper case) key and/or value.
- Use python module "black" for formatting the source code