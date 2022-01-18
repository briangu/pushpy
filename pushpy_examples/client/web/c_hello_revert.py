from pushpy_examples.client.ex_push_manager import ExamplePushManager

m = ExamplePushManager()
m.connect()

repl_code_store = m.repl_code_store()

# revert to the first version of HelloWorldHandler
repl_code_store.set_head(version=repl_code_store.get_head()-1, sync=True)
