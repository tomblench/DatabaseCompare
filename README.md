# DatabaseCompare

Compare two databases: display differing document IDs revision IDs.

Example:

```shell
http://tomblench.cloudant.com animaldb http://tomblench.cloudant.com animaldb_copy
Documents only in db 1:[]
Documents only in db 2:[0a583c7d1bc8e8c44d8aaaeef2024565]
Missing revs in db 1:{_design/views101=[2-aa8801cf6ea6be3542e6eaa6f799290a], aardvark=[6-77a62de4058ae4aec45b8db89e276072]}
Missing revs in db 2:{}
```

TODO: make it go faster, using threading and/or bulk APIs


