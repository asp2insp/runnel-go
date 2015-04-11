# Current
 - Add in-memory storage implementation
 - Redo header.
  - Must exist on per-storage basis
  - Needs to expose:
    - Sizes
    - EntryCount
    - Last Message offset
    - Tail offset
 - Redo File Storage file handling. Keep one reference to the file