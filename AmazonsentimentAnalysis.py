import asyncio
import sys
from datetime import datetime
from openai import OpenAI
# sys.path.append("Z:\\airflow\\scripts\\ReviewScraper")
# sys.path.append('/opt/airflow/scripts/ReviewScrpaer')
from AmazonDatabase import save_category_sentiment_to_db,fetch_reviews_from_db, get_categories, save_sentiment_to_db, fetch_asins_and_sentiments_for_category, fetch_avgprice_and_rating_for_category
import warnings

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

def get_openai_response(prompt):
    try:
        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "user", "content": f"""{prompt}"""},
                {"role":"system","content":"Provide the top 5 for each keyword, ensuring there are no contradictions and no null values"}
            ],
            temperature=1,
            max_tokens=256,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        response_content = completion.choices[0].message.content.strip()
        return response_content
    except Exception as e:
        print(f"Error during API call: {str(e)}")
        return None

async def analyze_sentiment_batch(reviews):
    if not reviews:
        print("No reviews to analyze.")
        return None, [], [], []

    prompt = "\n".join([f"Review: {review}" for review in reviews])
    prompt += """
    Analyze the reviews and provide a concise summary in a paragraph. Additionally, extract the most effective qualities of the product, categorized as only positive, only negative, and mixed.
    Format the response as follows:
    - Sentiment: <Sentiment summary>
    - Top Positive Keywords: keyword1, keyword2, keyword3, keyword4, keyword5
    - Top Negative Keywords: keyword1, keyword2, keyword3, keyword4, keyword5
    - Top Mixed Keywords: keyword1, keyword2, keyword3, keyword4, keyword5
    """

    try:
        response_text = get_openai_response(prompt)
        if response_text:
            sentiment = None
            top_positivekeywords = []
            top_negativekeywords = []
            top_mixedkeywords = []

            for line in response_text.strip().split('\n'):
                if "Sentiment:" in line:
                    sentiment = line.split(':')[-1].strip()
                elif "Top Positive Keywords:" in line:
                    top_positivekeywords = [kw.strip() for kw in line.split(':')[-1].strip().split(',')]
                elif "Top Negative Keywords:" in line:
                    top_negativekeywords = [kw.strip() for kw in line.split(':')[-1].strip().split(',')]
                elif "Top Mixed Keywords:" in line:
                    top_mixedkeywords = [kw.strip() for kw in line.split(':')[-1].strip().split(',')]

            if not top_positivekeywords or top_positivekeywords == ['']:
                print("No positive keywords found in the response.")
                top_positivekeywords = ["No significant positive keywords found"]

            if not top_negativekeywords or top_negativekeywords == ['']:
                print("No negative keywords found in the response.")
                top_negativekeywords = ["No significant negative keywords found"]

            if not top_mixedkeywords or top_mixedkeywords == ['']:
                print("No mixed keywords found in the response.")
                top_mixedkeywords = ["No significant mixed keywords found"]

            return sentiment, top_positivekeywords, top_negativekeywords, top_mixedkeywords
        else:
            print("Empty response from OpenAI.")
            return None, [], [], []
    except Exception as e:
        print(f"Error during sentiment analysis: {str(e)}")
        return None, [], [], []

async def analyze_category_sentiment(category):
    asin_sentiments = fetch_asins_and_sentiments_for_category(category)
    
    all_reviews_text = []
    
    for asin, sentiment_summary in asin_sentiments:
        all_reviews_text.append(f"ASIN: {asin}, Sentiment: {sentiment_summary}")
    
    prompt = "\n".join(all_reviews_text)
    prompt += """
    Analyze the following sentiment and extract an overall detailed summary of the category as a whole in a paragraph. Additionally, extract up to 5 of the most effective qualities of the product, categorized as only positive, only negative, and mixed.
    Format the response as follows:
    - Sentiment: <Sentiment summary>
    - Top Positive Keywords: keyword1, keyword2, keyword3, keyword4, keyword5
    - Top Negative Keywords: keyword1, keyword2, keyword3, keyword4, keyword5
    - Top Mixed Keywords: keyword1, keyword2, keyword3, keyword4, keyword5
    """
    
    try:
        collective_summary = get_openai_response(prompt)
        if collective_summary:
            sentiment = None
            top_positivekeywords = []
            top_negativekeywords = []
            top_mixedkeywords = []
            for line in collective_summary.strip().split('\n'):
                if "Sentiment:" in line:
                    sentiment = line.split(':')[-1].strip()
                elif "Top Positive Keywords:" in line:
                    top_positivekeywords = [kw.strip() for kw in line.split(':')[-1].strip().split(',')]
                elif "Top Negative Keywords:" in line:
                    top_negativekeywords = [kw.strip() for kw in line.split(':')[-1].strip().split(',')]
                elif "Top Mixed Keywords:" in line:
                    top_mixedkeywords = [kw.strip() for kw in line.split(':')[-1].strip().split(',')]

            if not top_positivekeywords or top_positivekeywords == ['']:
                print("No positive keywords found in the response.")
                top_positivekeywords = ["No significant positive keywords found"]

            if not top_negativekeywords or top_negativekeywords == ['']:
                print("No negative keywords found in the response.")
                top_negativekeywords = ["No significant negative keywords found"]

            if not top_mixedkeywords or top_mixedkeywords == ['']:
                print("No mixed keywords found in the response.")
                top_mixedkeywords = ["No significant mixed keywords found"]

            return sentiment, top_positivekeywords, top_negativekeywords, top_mixedkeywords
        else:
            print("Empty response from OpenAI.")
            return None, [], [], []
    except Exception as e:
        print(f"Error during sentiment analysis: {str(e)}")
        return None, [], [], []

async def perform_sentiment_analysis():
    reviews_df = fetch_reviews_from_db()

    grouped_reviews = reviews_df.groupby('asin')

    all_asin_sentiments = []

    for asin, group in grouped_reviews:
        review_texts = group['review'].tolist()
        print(f"Analyzing sentiment for ASIN: {asin}")

        sentiment_summary, positivekeywords, negativekeywords, mixedkeywords = await analyze_sentiment_batch(review_texts)
        
        if sentiment_summary:
            print(f"Overall Sentiment for {asin}: {sentiment_summary}")
            print(f"Top Positive Keywords: {positivekeywords}")
            print(f"Top Negative Keywords: {negativekeywords}")
            print(f"Top Mixed Keywords: {mixedkeywords}")

            sentiment_info = {
                'valuationdate': datetime.now().strftime('%Y-%m-%d'),
                'asin': asin,
                'sentiment_summary': sentiment_summary,
                'positivekeywords': ', '.join(positivekeywords),
                'negativekeywords': ', '.join(negativekeywords),
                'mixedkeywords': ', '.join(mixedkeywords)
            }

            all_asin_sentiments.append(sentiment_info)
        else:
            print(f"No sentiment returned for ASIN {asin}")
        save_sentiment_to_db(all_asin_sentiments)
    categoies = get_categories()
    for category in categoies:
        avgprice, avgrating = fetch_avgprice_and_rating_for_category(category)

        category_sentiment, category_positivekeywords, category_negativekeywords, category_mixedkeywords = await analyze_category_sentiment(category)
            
        if category_sentiment:
            overall_sentiment_info = {
                'valuationdate': datetime.now().strftime('%Y-%m-%d'),
                'category': category,
                'category_sentiment': category_sentiment,
                'positivekeywords': ', '.join(category_positivekeywords),
                'negativekeywords': ', '.join(category_negativekeywords),
                'mixedkeywords': ', '.join(category_mixedkeywords),
                'avgprice': avgprice,
                'avgrating': avgrating
            }
            print(f"Category Sentiment: {category_sentiment}")
            print(f"Top Positive Keywords: {category_positivekeywords}")
            print(f"Top Negative Keywords: {category_negativekeywords}")
            print(f"Top Mixed Keywords: {category_mixedkeywords}")
            print(f"avgprice: {avgprice}")
            print(f"avgrating: {avgrating}")
            save_category_sentiment_to_db(overall_sentiment_info)
        else:
            print(f"No category sentiment returned for {category}")

if __name__ == "__main__":
    asyncio.run(perform_sentiment_analysis())
