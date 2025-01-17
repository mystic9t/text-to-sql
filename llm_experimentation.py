# importing relevant modules

from os import getenv
import json
from dotenv import load_dotenv
import gradio as gr
from openai import OpenAI
import psycopg2

DATA_PATH = "data_tables/data_extract.json"
DATA_TEXT_PATH = "data_tables/data_extract_text"
CONFIG_PATH = "config/config.json"
ENV_PATH = ".env"


def load_json_data(data_path):
    with open(data_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data


def json_to_text(data, level=0):
    text = ""
    indent = "  " * level  # Indentation to reflect the level of nesting
    if isinstance(data, dict):
        for key, value in data.items():
            text += f"{indent}{key}: "
            if isinstance(value, dict) or isinstance(value, list):
                if isinstance(value, list) and value:  # Ensure the list is not empty
                    text += f"{value[0]}\n"  # First element of the list in front of the column name
                    if len(value) > 1:
                        text += json_to_text(
                            value[1:], level + 1
                        )  # Process the rest of the list
                else:
                    text += "\n" + json_to_text(value, level + 1)
            else:
                text += f"{value}\n"
    elif isinstance(data, list):
        for item in data:
            text += f"{indent}- {item}\n"
    return text


def question_refactor(input_text):
    load_dotenv(ENV_PATH)
    client = OpenAI(base_url=getenv("LLM_API_BASE"), api_key=getenv("LLM_API_KEY"))
    completion = client.chat.completions.create(
        model=getenv("LLM_MODEL2"),
        messages=[
            {
                "role": "system",
                "content": """
                        You are an expert business analyst, working with the CEO of Rand Global. 
                        You take in any question and make it more understandable.
                        Reframe the user input by correcting any possible spelling and grammer issues.
                        Make the input more verbose.
                        Do not add anything out of context.
                        Do not ask for clarifications.
                        Whenever not specified constrain the question to current year.
                        Do not include any of this in the output""",
            },
            {"role": "user", "content": f"{input_text}"},
        ],
        temperature=0.1,
    )

    #   Sample of values in completion: {
    #   "id": "chatcmpl-f1n6lygvwxsx3ck418fym",
    #   "object": "chat.completion",
    #   "created": 1719838117,
    #   "model": "Qwen/Qwen2-7B-Instruct-GGUF/qwen2-7b-instruct-q5_k_m.gguf",
    #   "choices": [
    #     {
    #       "index": 0,
    #       "message": {
    #         "role": "assistant",
    #         "content": ""
    #       },
    #       "finish_reason": "stop"
    #     }
    #   ],
    #   "usage": {
    #     "prompt_tokens": 865,
    #     "completion_tokens": 213,
    #     "total_tokens": 1078
    #   }
    # }

    return completion.choices[0].message.content


def generate_sql(refactored):
    json_data = load_json_data(DATA_PATH)
    table_info = json_to_text(json_data)
    with open(DATA_TEXT_PATH, "w", encoding="utf-8") as file:
        file.write(table_info)
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = json.load(file)
        schema_name = config.get("schema_name", "")
    # function that takes in a question and asks an LLM to create a corresponding query
    load_dotenv(ENV_PATH)
    client = OpenAI(base_url=getenv("LLM_API_BASE"), api_key=getenv("LLM_API_KEY"))

    completion = client.chat.completions.create(
        model=getenv("LLM_MODEL2"),
        messages=[
            {
                "role": "system",
                "content": f"""
                        You are an expert data analyst, specialied in SQL.
                        You take in a question and come up with a SQL query that could answer it.
                        You output as succinctly as possible and provide only SQL queries.
                        While creating SQL queries always remember the following:
                            Use schema name while reffering tables
                            Use table name when refering columns
                            Use UPPER() while trying to match strings in WHERE
                            Limit the output to 10 results unless otherwise specified
                        Do not make any assumptions about the database
                        Do not include this context in your output 
                        Below is some helpful information about the database
                        schema: {schema_name}
                        table info 
                        (formatted table name:
                                    column name: DATA TYPE
                                        Cardinal Values)
                        {table_info}
                        Do not provide any other output except the SQL query""",
            },
            {"role": "user", "content": f"{refactored}"},
        ],
        temperature=0.1,
    )

    return completion.choices[0].message.content


def execute_sql(query):
    load_dotenv(ENV_PATH)
    # connection parameters
    db_params = {
        "dbname": getenv("db_name"),
        "user": getenv("db_user"),
        "password": getenv("db_password"),
        "host": getenv("db_host"),
        "port": getenv("db_port"),
    }
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    rows = cur.fetchall()
    column_names = [description[0] for description in cur.description]
    # Convert rows to a list of dictionaries
    result = []
    for row in rows:
        row_dict = dict(zip(column_names, row))
        result.append(row_dict)
    cur.close()
    conn.close()
    return result


# Define a function that uses Gradio's input and output widgets
def process_input(input_text):
    # Reframe the user's input using your `reframe_question` function
    refactored = question_refactor(input_text)
    print(f"Question refactored: {refactored}")
    query = generate_sql(refactored)
    print(f"Query generated: {query}")
    result = execute_sql(query)
    print(f"Fetched results: {result}")
    # Return the result as a string for display in Gradio interface
    return query


def main():
    # Create an instance of the Gradio app
    iface = gr.Interface(
        fn=process_input,
        inputs="text",
        outputs="text",
        title="Data Visualizer",
        description="Hi There! How may I help?",
    )

    # Launch the app
    iface.launch()


if __name__ == main():
    main()
