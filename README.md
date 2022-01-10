# Push

Push (as in "push code") is an experimental dynamic and fault-tolerant Python application server with a 
focus on applications that need web APIs, long-running tasks, streaming and data processing.  Push mostly 
attains its goals by coexisting the application logic with Raft-based data structures, allowing intrinsic
Raft to manipulate state as well as provide for fault-tolerance.  That is, internal app communication occurs primarily
via Raft consensus.  Code deployment also occurs via Raft-based versioned dictionary, providing a git-like
code store that allows arbitrary version changes (via HEAD changes), making code deploys basically instantaneous
and instantly reversible.  

In short, Push is an experiment about leaning heavily on Raft and answering the question:
How many of the modern app-stack concerns can be collapsed into a single system, while also being easy to use?  

By piggy backing on implicit Raft mechanics, Push combines many useful features, normally found in separate systems, 
into a simple, dynamic, highly-reliable, application server.  

The following is a list of goals (features) desired while designing Push.  Many exist normally by relying on 
additional systems, but Raft allows for them to occur naturally in a single system.  To be fair, most of these
features are simply restatements of Raft-base data structures, but it's interesting to think of them in terms
of infra features.

Note that Push uses the Python BaseManager extensively when interacting with a node from external system.  
Using BaseManager eliminates the need to write a lot of code and provides its own useful features, such as the 
ability to run client code that references replicated data structures.

Push features / goals:

- Single server artifact
  - Only need to deploy the Push servers
  - Doesn't need Docker (particularly important for older ARM systems that can't run docker)
  - Configurable 'boot' module that sets up app context
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




Examples

- Web
  - Hello, World
  - Versioned Hello, World
- Code repo
  - import
  - export
  - versioning
  - module loader
- Versioned Dictionary
- Tasks
  - daemon
  - lambda
  - schedule
  - scope
- Queues
- Timeseries
  - simple
  - partitioned handlers
- REPL
- 






where application code coexists with Raft services,

so that internal 
task communication, shared state, deployment are based on Raft data structures.  Piggy backing on implicit Raft mechanics allows Push to 
combine several features normally found in separate systems.


Push (as in "push code") is a Raft-based application server, where application code is embedded with Raft services so that internal 
task communication, shared state, deployment are based on Raft data structures.  Piggy backing on implicit Raft mechanics allows Push to 
combine several features normally found in separate systems.

creating a dynamic, fault-tolerant app server for quick development and zero-downtime deployment of 
versioned code / artifacts.


It's an experimental project that combines several features normally 
found in separate systems creating a dynamic, fault-tolerant app server for quick development and zero-downtime deployment of 
versioned code / artifacts.

As expected, most of Push's flexibility comes from both the dynamic nature of Python and Raft consensus protocol.
Shared data structures are implemented using Raft as well as the deployment model.  Using a versioned dictionary, Push has a git-like
Code Store that allows for deploying code automatically to a cluster as well as switching between versions on the fly.

The flexible nature of Python enables treating compiled code, lambdas, as the smallest unit of "container."  Push supports loading modules
directly from the versioned code store, so the module code itself can be updated dynamically.  

By taking advantage of Raft implicit mechanics, Push is 



Key features (goals):

- Developer productivity
  - Simple self-contained system
  - Make code deploys as fast as hitting enter
  - Extensible 
- Use compiled Python as “containers”
  - Explore compiled code as the simplest unit
  - Take advantage of 
- Zero down time deployment
  - instantly rollback to any prior version
  - may deploy new code and enable it later
- High availability / fault-tolerance
  - Push is effectively a distributed app-server built on Raft
  - Shared data structures are implemented in Raft
- Allow mixed / heterogeneous host machine support
  - Supports heterogeneous machines and machine capability registration
  - Easily partition work handlers by machine capabilities
- Support integrating Machine Learning / Training
  - Applications can have tasks which use ML on GPU-capable hosts
  - ML inference engines can publish back into the shared state










Some caveats:

- Push services are currently multithreaded, containing the Raft server, which will cause the GIL to be used.
- Python modules either need to be installed beforehand or dynamically
- 



    
