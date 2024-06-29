# importing relevant modules

from os import getenv
import json
from dotenv import load_dotenv
import gradio as gr
from openai import OpenAI

DATA_PATH = "data_tables/data_extract.json"
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
                text += "\n" + json_to_text(value, level + 1)
            else:
                text += f"{value}\n"
    elif isinstance(data, list):
        list_items = ", ".join(map(str, data))
        text += f"{indent}{list_items}\n"
    return text


def question_refactor(input_text):
    load_dotenv(ENV_PATH)
    client = OpenAI(base_url=getenv("OPENAI_BASE"), api_key=getenv("OPENAI_API_KEY"))
    completion = client.chat.completions.create(
        model=getenv("LLM_MODEL2"),
        messages=[
            {
                "role": "system",
                "content": """
                        You are an expert business analyst, working with the CEO of Rand chain of motor stores. 
                        You take in any question and make it more understandable.
                        Reframe the user input by correcting any possible spelling and grammer issues.
                        Make the input more verbose.
                        Do not add anything out of context.
                        Do not ask for clarifications.
                        Whenever not specified constrain to question to current year.
                        Do not include any of this in the output""",
            },
            {"role": "user", "content": f"{input_text}"},
        ],
        temperature=0.1,
    )

    return completion.choices[0].message.content


def generatesql(refactored):
    json_data = load_json_data(DATA_PATH)
    table_info = json_to_text(json_data)
    # function that takes in a question and asks an LLM to create a corresponding query
    load_dotenv(ENV_PATH)
    client = OpenAI(base_url=getenv("OPENAI_BASE"), api_key=getenv("OPENAI_API_KEY"))

    completion = client.chat.completions.create(
        model=getenv("LLM_MODEL2"),
        messages=[
            {
                "role": "system",
                "content": f"""
                        You are an expert data analyst, specialied in SQL.
                        You take in a question and come up with a SQL query that could answer it.
                        You output only SQL queries.
                        While creating SQL queries always remember the following:
                            Use schema name while reffering tables when possible
                            Use table name when refering columns
                            Use UPPER() while trying to match strings in WHERE
                            Limit the output to 10 results unless otherwise
                        Do not make any assumptions about the database
                        Do not include this context in your output 
                        Below is some helpful information about the database
                        schema: motor_store
                        table info 
                        (formatted table name:
                                    column name: useful information)
                        {table_info}""",
            },
            {"role": "user", "content": f"{refactored}"},
        ],
        temperature=0.1,
    )

    return completion.choices[0].message.content


# Define a function that uses Gradio's input and output widgets
def process_input(input_text):
    # Reframe the user's input using your `reframe_question` function
    refactored = question_refactor(input_text)
    print(f"Question refactored: {refactored}")
    query = generatesql(refactored)
    print("Query generated")
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
