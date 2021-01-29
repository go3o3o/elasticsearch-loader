import pandas as pd

communityPath='export/communitydb'
articlesParquet='articles.parquet'

df = pd.read_parquet(communityPath + '/' + articlesParquet)
df.to_csv('articles.csv', encoding='utf8')