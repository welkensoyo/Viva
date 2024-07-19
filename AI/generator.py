from llama_index.llms.ollama import Ollama
from llama_parse import LlamaParse
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader, PromptTemplate
from llama_index.core.embeddings import resolve_embed_model
from llama_index.core.tools import QueryEngineTool, ToolMetadata
from llama_index.core.agent import ReActAgent
from pydantic import BaseModel
from llama_index.core.output_parsers import PydanticOutputParser
from llama_index.core.query_pipeline import QueryPipeline
from AI.prompts import context, code_parser_template
from AI.engine.read_code import code_reader
from dotenv import load_dotenv
import os
import ast, traceback

load_dotenv()
key = 'llx-0ve5HJOjfXTNaei0Nhgm0JdmUInivHl137xOKvqnzEtoUUMu'
class CodeOutput(BaseModel):
    code: str
    description: str
    filename: str

class Generator:
    def __init__(self, base_model='mistral', code_model='codellama'):
        self.llm = Ollama(model=base_model, request_timeout=30.0)
        self.parser = LlamaParse(api_key=key, result_type="markdown")
        self.file_extractor = {".pdf": self.parser, ".docx": self.parser}
        self.documents = SimpleDirectoryReader("data", file_extractor=self.file_extractor).load_data()
        self.embed_model = resolve_embed_model("local:BAAI/bge-m3")
        self.vector_index = VectorStoreIndex.from_documents(self.documents, embed_model=self.embed_model)
        self.query_engine = self.vector_index.as_query_engine(llm=self.llm)

        self.tools = [
            QueryEngineTool(
                query_engine=self.query_engine,
                metadata=ToolMetadata(
                    name="api_documentation",
                    description="this gives documentation about code for an API. Use this for reading docs for the API",
                ),
            ),
            code_reader,
        ]

        self.code_llm = Ollama(model=code_model)
        self.agent = ReActAgent.from_tools(self.tools, llm=self.code_llm, verbose=True, context=context)
        self.parser = PydanticOutputParser(CodeOutput)
        self.json_prompt_str = self.parser.format(code_parser_template)
        self.json_prompt_tmpl = PromptTemplate(self.json_prompt_str)
        self.output_pipeline = QueryPipeline(chain=[self.json_prompt_tmpl, self.llm])

    def add_tool(self, tool):
        self.tools.append(tool)

    def code(self, prompt: str):
        print(prompt)
        retries = 0
        cleaned_json = {}
        while retries < 3:
            try:
                result = self.agent.query(prompt)
                next_result = self.output_pipeline.run(response=result)
                cleaned_json.update(ast.literal_eval(str(next_result).replace("assistant:", "")))
                break
            except Exception as e:
                traceback.print_exc()
                retries += 1
                print(f"Error occured, retry #{retries}:", e)

            if retries >= 3:
                print("Unable to process request, try again...")
                continue
        if cleaned_json:
            print("Code generated")
            print(cleaned_json["code"])
            print("\n\nDesciption:", cleaned_json["description"])

            filename = cleaned_json["filename"]

            try:
                with open(os.path.join("output", filename), "w") as f:
                    f.write(cleaned_json["code"])
                print("Saved file", filename)
            except:
                traceback.print_exc()
                print("Error saving file...")

if __name__ == "__main__":
    g = Generator()
    g.code("Create a FASTAPI program in python that builds on the documentation provided and creates all the tools necessary to query the data from snowflake, and to store the data in a postgres database.")



