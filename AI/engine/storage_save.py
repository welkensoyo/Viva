import os
from nfty.njson import jc, dc
from pathlib import Path
from llama_index.core import StorageContext, VectorStoreIndex, load_index_from_storage

default_path = '~/cache/llama_index'


def cached_folder(folder, reset=False):
    metadata_file = Path(folder + '/metadata.json')
    directory_path = Path(folder)
    metadata = {}
    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            metadata = dc(f.read()) or dict()
    documents = []
    for file in directory_path.glob('*'):
        if file.is_file():
            file_time = os.path.getmtime(file)
            if (file.name not in metadata or metadata[file.name] != file_time) or reset:
                documents.append(file)
                metadata[file.name] = file_time
    metadata_file.write_text(jc(metadata))
    return documents


def get_index(data, index_type):
    Path(default_path).mkdir(parents=True, exist_ok=True)
    index_path = os.path.join(default_path, index_type)
    if not os.path.exists(index_path):
        print("building index", index_path)
        index = VectorStoreIndex.from_documents(data, show_progress=True)
        index.storage_context.persist(persist_dir=index_path)
    else:
        index = load_index_from_storage(
            StorageContext.from_defaults(persist_dir=index_path)
        )
    return index.as_query_engine()


