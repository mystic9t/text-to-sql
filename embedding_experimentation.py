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
    """customers: 
  customer_id: VARCHAR
  first_name: VARCHAR
  last_name: VARCHAR
  phone: VARCHAR
  email: VARCHAR
  street: VARCHAR
  city: VARCHAR
  state: VARCHAR
    - NY
    - TX
    - CA
  zip_code: INTEGER""",
    """orders: 
  order_id: VARCHAR
  customer_id: VARCHAR
  order_status: INTEGER
  order_date: TIMESTAMP
  required_date: TIMESTAMP
  shipped_date: TIMESTAMP
  store_id: VARCHAR
  staff_id: VARCHAR""",
    """order_items: 
  order_id: VARCHAR
  item_id: VARCHAR
  product_id: VARCHAR
  quantity: INTEGER
  list_price: DOUBLE PRECISION
  discount: DOUBLE PRECISION""",
    """products: 
  product_id: VARCHAR
  product_name: VARCHAR
  brand_id: VARCHAR
  category_id: VARCHAR
  model_year: INTEGER
  list_price: DOUBLE PRECISION""",
    """staffs: 
  staff_id: VARCHAR
  first_name: VARCHAR
  last_name: VARCHAR
  email: VARCHAR
  phone: VARCHAR
  active: INTEGER
  store_id: VARCHAR
  manager_id: VARCHAR""",
    """stocks: 
  store_id: VARCHAR
  product_id: VARCHAR
  quantity: INTEGER""",
    """stores: 
  store_id: VARCHAR
  store_name: VARCHAR
    - Rowlett Bikes
    - Baldwin Bikes
    - Santa Cruz Bikes
  phone: VARCHAR
  email: VARCHAR
  street: VARCHAR
  city: VARCHAR
    - Santa Cruz
    - Baldwin
    - Rowlett
  state: VARCHAR
    - NY
    - CA
    - TX
  zip_code: INTEGER""",
]
embed = [get_embedding(t) for t in text]
test_text = "How many electra road bikes were sold in texas"
test_embed = get_embedding(test_text)


def dot_product(vec1, vec2):
    return sum(e * t for e, t in zip(vec1, vec2))


value = [dot_product(e, test_embed) for e in embed]

print(value)
