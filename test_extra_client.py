from utils import get_tiled_client

@task
def get_other_docs(uid):
    result = get_tiled_client()[uid]
    for name, doc in result.documents():
        print(name, doc)
