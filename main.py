from llama_index.core import VectorStoreIndex, SimpleDirectoryReader
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()


documents = SimpleDirectoryReader("data_tables").load_data()
index = VectorStoreIndex.from_documents(documents)
query_engine = index.as_query_engine()
response = query_engine.query("Some question about the data should go here")
print(response)
