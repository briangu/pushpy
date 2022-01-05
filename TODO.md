TODO

- Add support for dynamic hosts
- Move python finder/loader to separate lib and add dict test
- break up examples into topics to illustrate different features
- fix reconnect for base manager
- add task routing based on host requirements
- ReplVersionedDict
  - add ReplVersionedDict event hook to reload modules 
  - add item/value views for versioned dict
- add code import / export tools
  - github
    - pull from github (version should be hash?)
  - pyz
  - zip
  - local dir
  - enumerate all versions and dump
- look into using lock and dict as-is for host resources
  - add a separate dict for host resources
  - lock indicates host presence (requiring a join to the host resources dict)
- add UDP transport for scaling nodes

DONE
- [x] REPL
- [x] work out the task relationship to global context - can they just write into the repl_ structs?
