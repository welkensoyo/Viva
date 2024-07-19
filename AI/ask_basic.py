from AI.engine.storage_save import get_index, cached_folder
from llama_index.core.tools import QueryEngineTool, ToolMetadata
from llama_index.core.agent import ReActAgent
from llama_index.core import SimpleDirectoryReader
from llama_parse import LlamaParse
from llama_index.llms.ollama import Ollama
from llama_index.embeddings.ollama import OllamaEmbedding
from llama_index.core import Settings
from llama_index.core.node_parser import SentenceSplitter
from AI.engine.pdf2table import pdf2table_engine
# from llama_index.core import SQLDatabase

context = """Purpose: The primary role of this agent is to assist users by analyzing documents. It should be able to answer questions about the documents and produce summary of both text and tables contained in the documents.  You should also be able to produce suggested SQL queries using any data dictionaries provided in other documents. """

Settings.llm = Ollama(model="phi3", request_timeout=120.0)
Settings.embed_model = OllamaEmbedding(model_name="mistral")
Settings.node_parser = SentenceSplitter(chunk_size=512, chunk_overlap=20)
Settings.num_output = 512
Settings.context_window = 3900
key = 'llx-0ve5HJOjfXTNaei0Nhgm0JdmUInivHl137xOKvqnzEtoUUMu'


def get_response(query, directory='./data'):
    template = """
        You are a helpful assistant. Only use tools that exist. Answer the following questions:
        User question: {user_question}
        """.format(user_question=query)
    parser = LlamaParse(api_key=key, result_type="markdown")
    documents = cached_folder(directory, reset=False)
    file_extractor = {".pdf": parser, ".docx": parser}
    documents = SimpleDirectoryReader(directory, input_files=documents, file_extractor=file_extractor).load_data()
    index = get_index(documents, 'data')
    tools = [
        pdf2table_engine,
        QueryEngineTool(
            query_engine=index,
            metadata=ToolMetadata(
                name="kan_viva",
                description="This gives information for the data dictionary and required key peformance indicators that need to be calculated",
            ),
        )
    ]
    agent = ReActAgent.from_tools(tools, llm=Settings.llm, verbose=True, context=context)
    # response = index.as_query_engine(llm=Settings.llm)
    return agent.query(template)

if __name__ == '__main__':

    print(get_response('In the chart for Key performance indicators, list the indicators under the column Metric in the file: Viva Medical Group - Kantime KPIs.pdf?'))
