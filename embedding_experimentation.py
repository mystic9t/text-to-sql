from openai import OpenAI

client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")


def get_embedding(text, model="nomic-ai/nomic-embed-text-v1.5-GGUF"):
    text = text.replace("\n", " ")
    return client.embeddings.create(input=[text], model=model).data[0].embedding


text = [
    """
brands: 
  brand_id: VARCHAR
  brand_name: VARCHAR
    - Sun Bicycles
    - Ritchey
    - Electra
    - Haro
    - Trek
    - Surly
    - Pure Cycles
    - Heller
    - Strider
""",
    """
categories: 
  category_id: VARCHAR
  category_name: VARCHAR
    - Electric Bikes
    - Comfort Bicycles
    - Cruisers Bicycles
    - Mountain Bikes
    - Cyclocross Bicycles
    - Road Bikes
    - Children Bicycles
""",
]
embed = []
for i in range(len(text)):
    val = get_embedding(text[i])
    embed.append(val)

test_text = "How many brands do we offer?"
test_embed = get_embedding(test_text)
value = []
for i in range(len(embed)):
    check_val = 0
    for j in range(len(test_embed)):
        check_val += embed[i][j] * test_embed[j]
    value.append(check_val)

print(value)
