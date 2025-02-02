import pandas as pd
import ast
import re
from langdetect import detect
from nltk.sentiment import SentimentIntensityAnalyzer

'''The following function counts the number of empty and null values
in a pandas series. (Supports the function below: general_information).'''

def missing_values(c):
  empty_spaces=c.apply(lambda x:x=="").sum()
  spaces=c.apply(lambda x:x==" ").sum()
  none_values=c.apply(lambda x: x==None).sum()
  nan_values=c.isna().sum()
  return empty_spaces+spaces+none_values+nan_values

'''The following function takes a dataframe as input and 
for each column it gets: the datatype, the
null values count and the percentage of null values.'''

def general_information(data):
    columns=[column for column in data.columns]
    dtypes=[type(column) for column in data.columns]
    missing_values_count=[missing_values(data[column]) for column in data.columns]
    missing_values_percentage=[round(i/len(data),2) for i in missing_values_count]
    return pd.DataFrame({"column":columns,"data_type":dtypes,"missing_values":missing_values_count,"missing_values_percentage":missing_values_percentage})

'''The following function reads through a JSON file and it turns every line into
a Pyhton dictionary and then finally creates a pandas dataframe out from that dictionary.'''

def load_json_lines(new_file_path):
    data = []
    with open(new_file_path, "r", encoding="utf-8") as file:
        for line in file:
            data.append(ast.literal_eval(line))
    return pd.DataFrame(data)

'''The following function takes a dataframe and a column as input
and returns rows of the dataframe with the same value for the 
input column.'''

def duplicated_values(df, column):
    duplicated_rows = df[df.duplicated(subset=column, keep=False)]
    if duplicated_rows.empty:
        return "No duplicated values"

    duplicated_rows_sorted = duplicated_rows.sort_values(by=column)
    return duplicated_rows_sorted

'''The following function takes as input a dataframe and a column containing
JSON objects. Then the column containing JSON objects is turned into a list
which is then converted to a pandas dataframe by using the json_normalize function.
Finally the new dataframe is concatenated to the original one and the JSON column
is removed'''

def expand_json_column(df, json_column):
    json_lists = df[json_column].tolist()
    expanded_df = pd.json_normalize(json_lists)
    df = pd.concat([df, expanded_df], axis=1)
    df.drop(json_column, axis=1, inplace=True)
    return df

'''The following function takes a date in the format YYYY-MM-DD 
and returns the year contained on such date'''

def get_year(date):
    if pd.notna(date):
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date):
            return date.split('-')[0]
    return 'Not available data'

'''The following function takes as input a dataframe and two
columns and outputs the percentage of values that are common to both
columns'''

def column_comparator(data,c1,c2):
  j=0
  for i in data.index:
    if data.loc[i,c1]==data.loc[i,c2]:
      j+=1
  percentage=round(((j*100)/len(data.index)),2)
  return f'The percentage of common values for the columns {c1} and {c2} is {percentage}%'

'''The following function detects the language in which a piece of 
text is written '''

def detect_language(t):
  try:
    return detect(t)
  except:
    return None

'''The following function applies sentiment analysis to a column
containing text'''

def analyze_sentiments(df):
    sia = SentimentIntensityAnalyzer()
    df['compound_score']=df['review'].apply(lambda review: sia.polarity_scores(review)['compound'])
    df['sentiment_analysis']=df['compound_score'].apply(lambda score: 0 if score < 0 else (1 if score == 0 else 2))
    return df

'''The following function takes as input a dataframe and a column
containing lists as its values, then returns a new dataframe in which
the input column now contains individual values rather than lists'''

def explode_column(df, column):
    if column not in df.columns:
        raise ValueError(f"The column '{column}' does not exist in the dataframe")
    df_expanded = df.explode(column).reset_index(drop=True)
    return df_expanded



