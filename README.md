# Push

Push (as in "push code") is an experimental Python app-server that combines [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)) based features (using [PySyncObj](https://github.com/bakwc/PySyncObj)) with the 
flexibility of Python, creating simple system that is both dynamic and fault-tolerant.  It's also an answer to the question:
How many of the modern app-stack concerns can be collapsed into a single system, while also being easy to use?  

As an app-server, Push is useful for applications that need web APIs, long-running tasks, streaming and data processing.  Push mostly 
attains its goals by combining the application logic with Raft-based data structures, allowing intrinsic
Raft operations to be used to to manipulate app state as well as provide for fault-tolerance.  That is, internal 
app communication occurs primarily via Raft consensus.  Code deployment also occurs via Raft-based versioned 
dictionary, providing a git-like code store that allows arbitrary version changes (via HEAD changes), making code 
deploys basically instantaneous and instantly reversible.  By piggy backing on implicit Raft mechanics, Push combines many useful features, normally found in separate systems, 
into a simple, dynamic, highly-reliable, application server.  

The following is a list of goals (features) desired while designing Push.  Many exist normally by relying on 
additional systems, but Raft allows for them to occur naturally in a single system.  To be fair, most of these
features are simply restatements of Raft-base data structures, but it's interesting to think of them in terms
of infra features.

Note that Push uses the Python [BaseManager](https://docs.python.org/3/library/multiprocessing.html) extensively when interacting with a node from external system.  
Using BaseManager eliminates the need to write a lot of code and provides its own useful features, such as the 
ability to run client code that references replicated data structures.

# Push features / goals:

- Single server artifact
  - Only need to deploy the Push servers
  - Doesn't need Docker (particularly important for older ARM systems that can't run docker)
  - Configurable 'boot' module that sets up app context
- Simple cluster bootstrapping 
  - Secondary nodes bootstrap off primary's config
  - Minimal configuration required
- Developer productivity
  - Simple self-contained system
  - Instant, zero-down-time, code deploys
  - Extensible with normal Python modules
  - Allow for hot loading of code
  - Allow for LISP REPL / SLIME-like behavior of being able to connect and examine / manipulate cluster state
- Use compiled Python as “containers”
  - Versioned Dictionary allows for git-like versioning and cluster replication
  - Lambda focused - explore compiled Python as the simplest unit
  - Take advantage of Python's dynamic nature
- Zero down time deployment
  - instantly replicate code, advance or rollback versions
  - may deploy new code and enable it later
- Support tasks
  - Allow ad-hoc lambda execution
  - Deploy long running tasks (daemons)
  - Be able to control where the tasks run
- High availability / fault-tolerance
  - Push is effectively a distributed app-server built on Raft
  - Built-in app resilience
  - Ability to dynamically size cluster
  - Shared data structures are implemented in Raft
- Allow mixed / heterogeneous host machine support
  - Supports heterogeneous machines and machine capability registration
  - Easily partition work handlers by machine capabilities
- Support integrating Machine Learning / Training
  - Applications can have tasks which use ML on GPU-capable hosts
  - ML inference engines can publish back into the shared state

# Quick start w/ Docker

To get an idea of how reverse proxy across a Push cluster would behave, startup a 3 node cluster and load a web requests handler.  Each node gets a copy of the compiled Python and serves it up as the root handler.  Using the versioned dictionary, we can change the handler on the fly and see the curl output change.

_Note: this requires docker-compose_

#### console 1
Setup 3 Push nodes w/ nginx reverse proxy.

```
$ cd $PUSH_HOME/deploy/pushpy_examples/compose
$ ./launch.sh push-cluster.yml up
```

#### console 2
Load a handler for the main root and let ngnix hit one of the 3 Push nodes.
```
$ cd $PUSH_HOME/push/pushpy_examples/web
$ python3 c_hello.py
$ curl localhost:4000/
hello, world!!!! [0]
$ python3 c_hello_2.py
$ curl localhost:4000/
Hello, World! (v2) [1]
```

# Versioned Web Handler Example

The following is a simple example of how the versioned dictionary can be used to version the root web handler.
The default tornado router looks into the versioned dictionary to match the /web + path and resolves to /web/.  
Looking into the dictionary it will find /web/ in repl_code_store and execute the handler.  When this handler
is changed, it will automatically be used.

```python
import requests
import tornado.web

from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()

repl_code_store = m.repl_code_store()

web_url = "http://localhost:11000/"


class HelloWorldHandler(tornado.web.RequestHandler):
  def get(self):
    self.write(f"hello, world!!!! head=[{repl_code_store.get_head()}]\n")


repl_code_store.set("/web/", HelloWorldHandler, sync=True)
print(requests.get(web_url).text)


class HelloWorldHandler2(tornado.web.RequestHandler):
  def get(self):
    self.write(f"Hello, World! (v2) head=[{repl_code_store.get_head()}]\n")


# update and change to v2
repl_code_store.set("/web/", HelloWorldHandler2, sync=True)
print(requests.get(web_url).text)

# revert to the first version of HelloWorldHandler
repl_code_store.set_head(version=repl_code_store.get_head() - 1, sync=True)
print(requests.get(web_url).text)
```
[code](pushpy_examples/client/web/c_hello_versioned.py)

```
$ python3 $PUSH_HOME/push/pushpy_examples/web/c_hello_versioned.py
```

Output
```
hello, world!!!! head=[0]

Hello, World! (v2) head=[1]

hello, world!!!! head=[0]
```

# More Examples

_a note on notation: 'repl' is used to denote 'replicated'_

- Web: Tornado based examples showing code loading via code store path
  - [Hello, World](pushpy_examples/client/web/c_hello.py)
  - [Versioned Hello, World](pushpy_examples/client/web/c_hello_versioned.py)
- Code repo: Showing how the vdict is used to store and version code
  - [import](pushpy_examples/client/code_repo/import)
  - [export](pushpy_examples/client/code_repo/export)
  - [versioning](pushpy_examples/client/code_repo/c_versions.py)
  - [module loader](pushpy_examples/client/code_repo/c_module.py)
- [Versioned Dictionary (vdict)](pushpy_examples/client/versioned_dict)
- [Tasks](pushpy_examples/client/tasks)
  - [daemon](pushpy_examples/client/tasks/daemon)
    - [local](pushpy_examples/client/tasks/daemon/local)
      - [Hello, World](pushpy_examples/client/tasks/daemon/local/c_hello.py)
      - [module](pushpy_examples/client/tasks/daemon/local/c_module.py)
    - [replicated](pushpy_examples/client/tasks/daemon/c_repl.py)
  - [lambda](pushpy_examples/client/tasks/lambda)
  - [schedule](pushpy_examples/client/tasks/schedule)
  - [scope](pushpy_examples/client/tasks/scope)
- Queues
- [Timeseries](pushpy_examples/client/timeseries)
  - simple
  - partitioned handlers
- [REPL](pushpy/push_repl.py)



As expected, most of Push's flexibility comes from both the dynamic nature of Python and Raft consensus protocol.
Shared data structures are implemented using Raft as well as the deployment model.  Using a versioned dictionary, Push has a git-like
Code Store that allows for deploying code automatically to a cluster as well as switching between versions on the fly.

The flexible nature of Python enables treating compiled code, lambdas, as the smallest unit of "container."  Push supports loading modules
directly from the versioned code store, so the module code itself can be updated dynamically.

# Dynamic Modules via Versioned Dictionary

The following example shows the dynamic module system being able to load a module from the vdict.  We first set the
code interpreter classes and then update them.  They can be loaded either directly via import or implicitly via the task manager.

```python
from pushpy_examples.client.ex_push_manager import ExamplePushManager
from client.simple_interpreter import Multiplier, Adder, Interpreter

m = ExamplePushManager()
m.connect()

local_tasks = m.local_tasks()

repl_code_store = m.repl_code_store()
repl_code_store.update({
  "interpreter.Interpreter": Interpreter,
  "interpreter.math.Adder": Adder,
  "interpreter.math.Multiplier": Multiplier
}, sync=True)

ops = ['add', 'add', 1, 2, 'mul', 3, 4]

# run task via this client
r = local_tasks.apply("interpreter.Interpreter", ops)[0]
print(r)
assert r == 15


class Adder2(Adder):
  def apply(self, a, b):
    print("using adder v2")
    return (a + b) * 2


repl_code_store.set("interpreter.math.Adder", Adder2, sync=True)
r = local_tasks.apply("interpreter.Interpreter", ops)[0]
print(r)
assert r == 36


class InterpreterWrapper:
  def apply(self, ops):
    from repl_code_store.interpreter import Interpreter
    return Interpreter().apply(ops)


r = local_tasks.apply(InterpreterWrapper, ops)[0]
print(r)
assert r == 36

```

# Docker deploy primary and secondaries on other machines

Push supports auto-config of new nodes by bootstrapping off of a primary (existing) node.  The bootstrap process provides all of the code and network setup for the new node and adds it to the cluster.

deploy primary
```
$ cd $PUSH_HOME/deploy/compose
$ ./primary.sh up
```

deploy a bootstrap node (on another machine)
```
$ cd $PUSH_HOME/deploy/compose
$ ./bootstrap.sh up
```


# Docker multiple nodes on a single machine

deploy primary
```
$ cd $PUSH_HOME/deploy/compose
$ ./primary.sh up
```

deploy a secondary node
```
$ cd $PUSH_HOME/deploy/compose
$ ./secondary.sh up
```

deploy another secondary node
```
$ cd $PUSH_HOME/deploy/compose
$ ./secondary-2.sh up
```


# Quick start (bare)
Start example cluster and launch REPL.
```bash
$ export PUSH_HOME=`pwd`
$ ./pushpy/deploy/pushpy_examples/bare/run_cluster.sh
> hosts
['localhost:50000', 'localhost:50001', 'localhost:50002']
>>> @localhost:50000
localhost:50000 >>> 
```

Now you can run examples against a 3-node cluster.


# Some caveats

- Push services are currently multithreaded, containing the Raft server, which will cause the GIL to be used.
- Python modules either need to be installed beforehand or dynamically (e.g. via shell)
- Python may not behave well with reloading modules on the fly, at least this was reported in older versions.

# License

[MIT](LICENSE.txt)

# References

Raft implementation is based on [PySyncObj](https://github.com/bakwc/PySyncObj).

