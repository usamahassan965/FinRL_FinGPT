from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoModelForSeq2SeqLM
from FinGPT.finnhub_date_range import Finnhub_Date_Range
from tqdm import tqdm
from transformers import pipeline
from statistics import mode, mean
import torch.nn.functional as F
import streamlit as st

st.title("Stock News Analysis")

model_sent = "ProsusAI/finbert"
model_sum = "Falconsai/text_summarization"
tokens = 2048


@st.cache_resource
def sentiment_model(model_name):
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    return model, tokenizer

@st.cache_resource
def summary_model(model_name):
    summarizer = pipeline("summarization", model=model_name)
    summarizer = summarizer(news_item,max_length=200,min_length=30,do_sample=False)[0]["summary_text"]
    return summarizer


@st.cache_data
def split_news(news, max_tokens):
    news_items = news.split('\n')
    split_news_list = []
    current_item = ""

    for item in tqdm(news_items):
        if len(current_item) + len(item) <= max_tokens:
            current_item += item + '\n'
        else:
            split_news_list.append(current_item.strip())  # Remove trailing newline
            current_item = item + '\n'

    # Add the last item
    split_news_list.append(current_item.strip())

    return split_news_list


# Step 1: Create text fields for start date and end date with default values
start_date = st.text_input("Start Date", "2023-09-01")
end_date = st.text_input("End Date", "2023-09-08")
News_list = []


@st.cache_data
def download_news(start_date, end_date):
    config = {
        "use_proxy": "us_free",
        "max_retry": 5,
        "proxy_pages": 5,
        "token": "ckc09r1r01qjeja48ougckc09r1r01qjeja48ov0"
    }
    stock = "AAPL"
    news_downloader = Finnhub_Date_Range(config)
    news_downloader.download_date_range_stock(str(start_date), str(end_date), stock=stock)
    news_downloader.gather_content()
    df = news_downloader.dataframe
    df["date"] = df.datetime.dt.date
    df["date"] = df["date"].astype("str")
    df = df.sort_values("datetime")
    news_list = list(df['headline'])

    st.success(f"Downloaded {len(news_list)} news articles.")
    return news_list


news = None
split_news_list = None
load_news = st.checkbox("Load News")
# Step 2: Create a button to download news
if load_news:
    News_list.extend(download_news(start_date, end_date))

if load_news and news is None:
    news = '\n'.join(News_list)
# Step 3: Create a button to perform sentiment analysis and summarization
if st.button("Stocks Analysis") and news is not None:
    sentiments = []
    probabilities = []
    summaries = []
    split_news_list = split_news(news,max_tokens=tokens)
    model, tokenizer = sentiment_model(model_sent)
    #model2, tokenizer2 = summary_model(model_sum)
    # Use a pipeline as a high-level helper

    for i, news_item in enumerate(split_news_list):
        inputs = tokenizer(news_item, return_tensors="pt")
        outputs = model(**inputs)

        probs = F.softmax(outputs.logits, dim=-1)
        sentiment = model.config.id2label[probs.argmax().item()]
        probability = probs.max().item()
        sentiments.append(sentiment)
        probabilities.append(probability)
        print(str(i)+': '+str(len(news_item)))
        summary = summary_model(model_name=model_sum)
        if summary[-1] == '.':
            pass
        else:
            summary += '.'
        summary += "\n"

        summaries.append(summary)
        print(sentiment, probability)
        print(summary)
    sentimode = mode(sentiments)
    probmean = mean(probabilities)
    result_summary = "\n".join(summaries)
    st.subheader("Headlines")
    st.success(result_summary)
    st.subheader("Sentiment Analysis")
    st.success(f'Sentiment: {sentimode.upper()}')
    st.success(f'Score: {probmean:.2f}')
    if st.button("Clear All"):
    # Clear values from *all* all in-memory and on-disk data caches:
    # i.e. clear values from both square and cube
        sentiment_model.clear()
        summary_model.clear()
        download_news.clear()
        split_news.clear()
        st.cache_data.clear()
        st.cache_resource.clear()
        
    
