TODO

On head change event
  Invalidate module cache for push finder

- add push config to store secrets, etc.
- break up examples into topics to illustrate different features
- create docker/monitor folder
- Add support for dynamic hosts
- Move python finder/loader to separate lib and add dict test
- fix reconnect for base manager
- add task routing based on host requirements
- ReplVersionedDict
  - add ReplVersionedDict event hook to reload modules 
    - on_head_change
    - add code reloading capability ?
  - add item/value views for versioned dict
  - implement flatten
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
# TODO: test storing result and using it in a subsequent task
# TODO: add support for daemon deployment via repl task

DONE
- [x] REPL
- [x] work out the task relationship to global context - can they just write into the repl_ structs?
