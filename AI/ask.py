import os, streamlit as st
from langchain_core.messages import AIMessage, HumanMessage
from llama_index.core import SimpleDirectoryReader, PromptHelper, ServiceContext, VectorStoreIndex

from langchain_core.output_parsers import StrOutputParser
from llama_parse import LlamaParse

from llama_index.llms.ollama import Ollama
from llama_index.embeddings.ollama import OllamaEmbedding
from llama_index.core import Settings
from llama_index.core.node_parser import SentenceSplitter

Settings.llm = Ollama(model="mistral", request_timeout=120.0)
Settings.embed_model = OllamaEmbedding(model_name="mxbai-embed-large", base_url="http://localhost:11434", ollama_additional_kwargs={"mirostat": 0},)
Settings.node_parser = SentenceSplitter(chunk_size=512, chunk_overlap=20)
Settings.num_output = 512
Settings.context_window = 3900
key = 'llx-0ve5HJOjfXTNaei0Nhgm0JdmUInivHl137xOKvqnzEtoUUMu'

st.set_page_config(page_title="NFTY bot", page_icon="🤖")
st.title("NFTY bot")

directory_path = st.sidebar.text_input(
    label="#### Your data directory path",
    placeholder="./data",
    type="default")

key = 'llx-0ve5HJOjfXTNaei0Nhgm0JdmUInivHl137xOKvqnzEtoUUMu'

def get_response(query, chat_history, directory='./data', base_model='mistral', embed_model='mxbai-embed-large'):
    Settings.llm = Ollama(model=base_model, request_timeout=120.0)
    Settings.embed_model = OllamaEmbedding(model_name=embed_model, base_url="http://localhost:11434", ollama_additional_kwargs={"mirostat": 0}, )
    Settings.node_parser = SentenceSplitter(chunk_size=512, chunk_overlap=20)
    Settings.num_output = 512
    Settings.context_window = 3900
    template = """
        You are a helpful assistant. Answer the following questions considering the history of the conversation:

        Chat history: {chat_history}

        User question: {user_question}
        """.format(chat_history=chat_history, user_question=query)

    os.mkdir(directory)
    parser = LlamaParse(api_key=key, result_type="markdown")
    file_extractor = {".pdf": parser, ".docx": parser}
    documents = SimpleDirectoryReader(directory, file_extractor=file_extractor).load_data()
    index = VectorStoreIndex.from_documents(documents)
    response = index.as_query_engine(llm=Settings.llm).query(template)
    if response is None:
        st.error("Oops! No result found")
    chain = response | '' | StrOutputParser()
    return chain.stream({
        "chat_history": chat_history,
        "user_question": user_query,
    })


# session state
if "chat_history" not in st.session_state:
    st.session_state.chat_history = [
        AIMessage(content="Hello, I am a bot. How can I help you?"),
    ]


for message in st.session_state.chat_history:
    if isinstance(message, AIMessage):
        with st.chat_message("AI"):
            st.write(message.content)
    elif isinstance(message, HumanMessage):
        with st.chat_message("Human"):
            st.write(message.content)

# user input
user_query = st.chat_input("Type your message here...")
if user_query is not None and user_query != "":
    st.session_state.chat_history.append(HumanMessage(content=user_query))

    with st.chat_message("Human"):
        st.markdown(user_query)

    with st.chat_message("AI"):
        answer = st.write_stream(get_response(user_query, st.session_state.chat_history))

    st.session_state.chat_history.append(AIMessage(content=answer))