
# TODOs

- Test setup.py and build process (wheels) with new structure.
   - IMPORTANT: install and test the generated wheel
- Cleanup file structure, tests, etc.
- Make sure that everything is mentioned in readme, is actually working
- I'm wondering whether we could use DataFrame, like Pandas, Veax, etc.
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
