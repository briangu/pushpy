# TODO: show loading code from github into repl_code_store
# @staticmethod
# def load_github(store, key_prefix, repo):
#     from github import Github
#     g = Github()
#     repo = g.get_repo(repo)
#     contents = repo.get_contents("")
#     while contents:
#         file_content = contents.pop(0)
#         if file_content.type == "dir":
#             contents.extend(repo.get_contents(file_content.path))
#         else:
#             print(file_content.path)
#             # store.set(f"{key_prefix}file_content.path")
