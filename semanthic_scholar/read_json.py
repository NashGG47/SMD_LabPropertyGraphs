#test
import pandas as pd
df = pd.read_json('data/semantic_scholar/examples/20240126_080619_00029_8pq62_020a9fc8-d10d-4f57-89dc-66fd71807388', lines=True)
df = df[(df.papercount >= 5) & (df.citationcount >= 10)]
print(df[['authorid', 'name', 'citationcount']])