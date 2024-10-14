import psycopg2
import pandas as pd

def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="Scraper",
            user="postgres",
            password="Ritika@22",
            host="localhost",
            port=5432
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {str(e)}")
        return None
# def connect_to_db():
#     try:
#         conn = psycopg2.connect(
#             dbname="Insyt",
#             user="postgres",
#             password="admin",
#             host="172.18.0.66",
#             port=5432
#         )
#         return conn
#     except Exception as e:
#         print(f"Error connecting to the database: {str(e)}")
#         return None
# scrapper

def get_platform_code():
    conn = None
    cursor = None
    platform_codes = []

    try:
        conn = connect_to_db()
        if conn is None:
            return platform_codes

        cursor = conn.cursor()
        cursor.execute("SELECT platformcode FROM Amazon.productmaster LIMIT 20;")
        platform_codes = [row[0] for row in cursor.fetchall()]

    except Exception as e:
        print(f"Error retrieving Platform Codes: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return platform_codes

def save_to_database(df_products: pd.DataFrame, df_reviews: pd.DataFrame):
    conn = None
    cursor = None
    try:
        conn = connect_to_db()
        if conn is None:
            return

        cursor = conn.cursor()

        for _, row in df_products.iterrows():
            cursor.execute("""
                        INSERT INTO Amazon.productoverview (valuationdate, platformcode, title, price, rating, totalrating,totalreviews)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (valuationdate,platformcode)  
                        DO UPDATE SET
                            title = EXCLUDED.title,
                            price = EXCLUDED.price,
                            rating = EXCLUDED.rating,
                            totalrating = EXCLUDED.totalrating,
                            totalreviews=EXCLUDED.totalreviews;
            """, (
                row['valuationdate'],
                row['ASIN'],
                row['title'],
                row['price'],
                row['rating'],
                row['NumberOfRatings'],
                row['NumberOfReviews']
            ))

        for _, row in df_reviews.iterrows():
            cursor.execute("""
                INSERT INTO Amazon.reviewdump (valuationdate,  platformcode, reviewid,reviewername, rating, review, reviewdate)
                VALUES (%s,%s, %s, %s, %s, %s, %s)
                ON CONFLICT (reviewid) DO UPDATE SET
                    valuationdate = EXCLUDED.valuationdate,
                    platformcode = EXCLUDED.platformcode ,
                    reviewername = EXCLUDED.reviewername,
                    rating = EXCLUDED.rating,
                    review = EXCLUDED.review,
                    reviewdate = EXCLUDED.reviewdate;
            """, (
                row['valuationdate'],
                row['ASIN'],
                row['review_id'],
                row['name'],
                row['rating'],
                row['review'],
                row['date'] 
            ))

        conn.commit()
        return "Data inserted successfully."

    except Exception as e:
        raise ValueError(f"Failed to retrieve data : {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# sentiment analysis

def fetch_reviews_from_db():
    conn = connect_to_db()
    
    query = "SELECT platformcode, review FROM amazon.reviewdump"
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        reviews_df = pd.DataFrame(rows, columns=['asin', 'review'])
    except Exception as e:
        print(f"Error fetching reviews from the database: {str(e)}")
        reviews_df = pd.DataFrame()  
    
    conn.close()
    return reviews_df


def fetch_asins_and_sentiments_for_category(category):
    conn = connect_to_db()
    try:
        query = f"""
        SELECT asm.platformproductcode , asm.summary 
        from amazon.sentiment as asm  
        left join amazon.productmaster as apm ON apm.platformcode = asm.platformproductcode
        LEFT JOIN bsc.productmaster as pm ON pm.ipcode = apm.ipcode
        LEFT JOIN bsc.categories as bscc ON bscc.id = pm.categoryid
        WHERE bscc.name ='{category}';
        """
        
        sentiments_df = pd.read_sql_query(query, conn)
        return sentiments_df.values.tolist()
    
    except Exception as e:
        print(f"Error fetching ASINs and sentiments for category: {str(e)}")
        return []

def fetch_avgprice_and_rating_for_category(category):
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        query = f"""
        SELECT ROUND(AVG(apo.price),2),ROUND(AVG(apo.rating),2) as category from amazon.productoverview as apo 
        LEFT JOIN amazon.productmaster as apm ON apm.platformcode = apo.platformcode
        LEFT JOIN bsc.productmaster as pm ON pm.ipcode = apm.ipcode
        LEFT JOIN bsc.categories as bscc ON bscc.id = pm.categoryid
        WHERE bscc.name = '{category}' AND apo.price >0 and apo.rating>0 and apo.valuationdate =(
                    SELECT MAX(valuationdate)
                    FROM amazon.productoverview);
        """
        cur.execute(query, (category,))
        result = cur.fetchone()

        avgprice = result[0] if result[0] is not None else 0
        avgrating = result[1] if result[1] is not None else 0

        cur.close()
        conn.close()

        return avgprice, avgrating

    except Exception as e:
        print(f"Error fetching average price and rating: {str(e)}")
        return 0, 0


def save_sentiment_to_db(asins_sentiments):
    conn = connect_to_db()
    cursor = conn.cursor()
    if asins_sentiments:
        for sentiment in asins_sentiments:
            assert isinstance(sentiment, dict), f"Expected a dictionary but got {type(sentiment)}"
            cursor.execute("""
                INSERT INTO amazon.sentiment (
                                                valuationdate, 
                                                platformproductcode, 
                                                summary, 
                                                positivekeywords, 
                                                negativekeywords, 
                                                mixedkeywords
                                            ) 
                                            VALUES (%s, %s, %s, %s, %s, %s)
                                            ON CONFLICT (valuationdate, platformproductcode) 
                                            DO UPDATE 
                                            SET 
                                                summary = EXCLUDED.summary,
                                                positivekeywords = EXCLUDED.positivekeywords,
                                                negativekeywords = EXCLUDED.negativekeywords,
                                                mixedkeywords = EXCLUDED.mixedkeywords;

            """, (sentiment['valuationdate'], sentiment['asin'], sentiment['sentiment_summary'], sentiment['positivekeywords'], sentiment['negativekeywords'], sentiment['mixedkeywords']))

   
    conn.commit()
    cursor.close()
    conn.close()

def save_category_sentiment_to_db(category_sentiment_info):
    conn = connect_to_db()
    cursor = conn.cursor()
    if category_sentiment_info:
        assert isinstance(category_sentiment_info, dict), f"Expected a dictionary but got {type(category_sentiment_info)}"
        cursor.execute("""
           INSERT INTO amazon.categorysentiment (
                                                    valuationdate, 
                                                    category, 
                                                    categorysummary, 
                                                    positivekeywords, 
                                                    negativekeywords, 
                                                    mixedkeywords, 
                                                    avgprice, 
                                                    avgrating
                                                ) 
                                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                                ON CONFLICT (valuationdate, category)
                                                DO UPDATE 
                                                SET 
                                                    categorysummary = EXCLUDED.categorysummary,
                                                    positivekeywords = EXCLUDED.positivekeywords,
                                                    negativekeywords = EXCLUDED.negativekeywords,
                                                    mixedkeywords = EXCLUDED.mixedkeywords,
                                                    avgprice = EXCLUDED.avgprice,
                                                    avgrating = EXCLUDED.avgrating;

        """, (category_sentiment_info['valuationdate'], category_sentiment_info['category'], category_sentiment_info['category_sentiment'], category_sentiment_info['positivekeywords'], category_sentiment_info['negativekeywords'], category_sentiment_info['mixedkeywords'],category_sentiment_info['avgprice'],category_sentiment_info['avgrating']))
    conn.commit()
    cursor.close()
    conn.close()


# streamlit

def get_sentiment_summary_and_keywords(asin):
    conn = connect_to_db()
    if conn is None:
        return None
    
    query = f"""
    SELECT summary, positivekeywords, negativekeywords, mixedkeywords 
    FROM amazon.sentiment 
    WHERE platformproductcode= '{asin}';
    """
    data = pd.read_sql(query, conn)
    conn.close()
    
    if not data.empty:
        return data.iloc[0] 
    else:
        return None
    

def get_price_rating_history(asin):
    conn = connect_to_db()
    if conn is None:
        return None, None, None
    
    query = f"""
    SELECT valuationdate, price, rating 
    FROM amazon.productoverview
    WHERE platformcode = '{asin}'  AND price >0 AND rating >0
    ORDER BY valuationdate;
    """
    data = pd.read_sql(query, conn)
    
    min_price_query = f"SELECT MIN(price) FROM amazon.productoverview WHERE price > 0 AND platformcode = '{asin}';"
    max_price_query = f"SELECT MAX(price) FROM amazon.productoverview WHERE platformcode = '{asin}';"
    
    min_price = pd.read_sql(min_price_query, conn).iloc[0, 0]
    max_price = pd.read_sql(max_price_query, conn).iloc[0, 0]
    
    conn.close()
    
    return data, min_price, max_price   


def get_categories():
    conn = connect_to_db()
    if conn is None:
        return []
    
    query = """ SELECT DISTINCT bscc.name AS category
                FROM amazon.productoverview AS apo
                LEFT JOIN amazon.productmaster AS apm ON apm.platformcode = apo.platformcode
                LEFT JOIN bsc.productmaster AS pm ON pm.ipcode = apm.ipcode
                LEFT JOIN bsc.categories AS bscc ON bscc.id = pm.categoryid
                WHERE apo.price > 0 
                AND apo.rating > 0; """
    data = pd.read_sql(query, conn)
    conn.close()
    
    return data['category'].tolist()

def get_avgprice_rating_history(category):
    conn = connect_to_db()
    if conn is None:
        return None, None, None
    # ROUND(AVG(apo.price),2)
    query = f"""
    SELECT apo.valuationdate, 
    ROUND(AVG(apo.price),2) AS avgprice, 
    ROUND(AVG(apo.rating),2) AS avgrating
    FROM amazon.productoverview AS apo
    LEFT JOIN amazon.productmaster AS apm ON apm.platformcode = apo.platformcode
    LEFT JOIN bsc.productmaster AS pm ON pm.ipcode = apm.ipcode
    LEFT JOIN bsc.categories AS bscc ON bscc.id = pm.categoryid
    WHERE bscc.name = '{category}' 
    AND apo.price > 0 
    AND apo.rating > 0
    GROUP BY apo.valuationdate;
    """
    data = pd.read_sql(query, conn)
    
    # min_price_query = "SELECT MIN(price) FROM amazon.productoverview WHERE price > 0;"
    # max_price_query = "SELECT MAX(price) FROM amazon.productoverview;"
    
    min_price = data['avgprice'].min()
    max_price = data['avgprice'].max()

    # min_rating=data['avgrating'].min()
    # max_rating=data['avgrating'].max()
    
    conn.close()
    
    return data, min_price, max_price

def get_whsku_for_category(category):
    conn = connect_to_db()
    if conn is None:
        return {}
    
    query = f"""
    SELECT  pm.whsku ,asm.platformproductcode from amazon.sentiment as asm
    LEFT JOIN amazon.productmaster as apm ON apm.platformcode = asm.platformproductcode 
    LEFT JOIN bsc.productmaster as pm ON pm.ipcode = apm.ipcode
    LEFT JOIN bsc.categories as bscc ON bscc.id = pm.categoryid
    WHERE bscc.name = '{category}';
    """
    data = pd.read_sql(query, conn)
    conn.close()
    
    return dict(zip(data['whsku'], data['platformproductcode']))


def get_current_price_rating(asin):
    conn = connect_to_db()
    if conn is None:
        return None
    
    query = f"""
    SELECT price, rating , totalreviews
    FROM amazon.productoverview 
    WHERE platformcode = '{asin}' 
    AND valuationdate = (
	    SELECT MAX(valuationdate)
	    FROM amazon.productoverview
	    WHERE platformcode = '{asin}')
    """
    data = pd.read_sql(query, conn)
    conn.close()

    if not data.empty:
        return data.iloc[0] 
    else:
        return None



def get_category_summary(category):
    conn = connect_to_db()
    if conn is None:
        return None
    
    query = f"""
    SELECT categorysummary, valuationdate, positivekeywords, negativekeywords, mixedkeywords, avgprice, avgrating
    FROM amazon.categorysentiment 
    WHERE category = '{category}'AND valuationdate=(
                    SELECT MAX(valuationdate)
                    FROM amazon.categorysentiment);
    """
    data = pd.read_sql(query, conn)
    conn.close()
    
    if not data.empty:
        return data.iloc[0]
    else:
        return None


def get_sku_data_for_whsku(category):
    conn = connect_to_db()
    if conn is None:
        return None
    query = """
    SELECT DISTINCT
    pm.whsku,
    asm.platformproductcode,
    po.rating,
    AVG(po.rating) OVER (
        PARTITION BY asm.platformproductcode 
        ORDER BY po.valuationdate 
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS avg_rating_last_10_days,
    po.totalreviews,
    po.valuationdate
    FROM amazon.sentiment AS asm
    LEFT JOIN amazon.productmaster AS apm ON apm.platformcode = asm.platformproductcode 
    LEFT JOIN amazon.productoverview AS po ON po.platformcode = apm.platformcode
    LEFT JOIN bsc.productmaster AS pm ON pm.ipcode = apm.ipcode
    LEFT JOIN bsc.categories AS bscc ON bscc.id = pm.categoryid
    WHERE bscc.name = %s AND po.valuationdate = (
            SELECT MAX(valuationdate) 
            FROM amazon.productoverview 
            WHERE platformcode = apm.platformcode
        );
    """

    sku_data = pd.read_sql(query, conn, params=(category,))
    conn.close()
    return sku_data

def get_sales_and_rating_history(asin):
    conn=connect_to_db()
    if conn is None:
        return None
    
    query="""
        WITH SalesAndRatings AS (
            SELECT 
                dsr.valuationdate,
                dsr.totalquantitysold,
                apo.rating
            FROM Amazon.DSRDump dsr
            LEFT JOIN Amazon.ProductMaster pm ON pm.Id = dsr.PlatformProductId
            LEFT JOIN amazon.productoverview AS apo ON apo.platformcode = pm.platformcode 
                AND apo.valuationdate = dsr.valuationdate
            WHERE apo.platformcode = %s
        )
        SELECT
            valuationdate,
            AVG(totalquantitysold) OVER (ORDER BY valuationdate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS netsales,
            AVG(rating) OVER (ORDER BY valuationdate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rating
        FROM SalesAndRatings
        ORDER BY valuationdate;
    """
    data=pd.read_sql(query,conn,params=(asin,))
    conn.close()
    return data


def get_sales_and_reviews_history(asin):
    conn=connect_to_db()
    if conn is None:
        return None
    
    query="""
        WITH SalesAndRatings AS (
            SELECT 
                dsr.valuationdate,
                dsr.totalquantitysold,
                apo.totalreviews
            FROM Amazon.DSRDump dsr
            LEFT JOIN Amazon.ProductMaster pm ON pm.Id = dsr.PlatformProductId
            LEFT JOIN amazon.productoverview AS apo ON apo.platformcode = pm.platformcode 
                AND apo.valuationdate = dsr.valuationdate
            WHERE apo.platformcode = %s
        )
        SELECT
            valuationdate,
            AVG(totalquantitysold) OVER (ORDER BY valuationdate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS netsales,
            AVG(totalreviews) OVER (ORDER BY valuationdate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS totalreviews
        FROM SalesAndRatings
        ORDER BY valuationdate;
    """
    data=pd.read_sql(query,conn,params=(asin,))
    conn.close()
    return data


#  competitors



def save_comp_data(df_products: pd.DataFrame):
    conn = None
    cursor = None
    try:
        conn = connect_to_db()
        if conn is None:
            return

        cursor = conn.cursor()
        print("going in database")
        for _, row in df_products.iterrows():
            cursor.execute("""
                        INSERT INTO Amazon.competitors (valuationdate, asin ,compasin,compsku,category,comprating,compbrand,volume,sku)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (valuationdate,asin,compasin)  
                        DO UPDATE SET
                            compsku = EXCLUDED.compsku,
                            category = EXCLUDED.category,
                            comprating = EXCLUDED.comprating,
                            compbrand=EXCLUDED.compbrand,
                            volume=EXCLUDED.volume,
                           sku=EXCLUDED.sku;
            """, (
                row['valuationdate'],
                row['ASIN'],
                row['Competitor ASIN'],
                row['Competitor SKU'],
                row['Category'],
                row['rating'],
                row['brand'],
                row['count'],
                row['BSC Trimmer']
            ))
        conn.commit()
        print("Data inserted successfully.")
    except Exception as e:
        raise ValueError(f"Failed to retrieve data : {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def product_comp_rating(asin):
    query = f"""
    SELECT 
        po.valuationdate, 
        po.rating AS product_rating,
        c.sku,
        c.compasin, 
        c.comprating,
		c.compsku
    FROM amazon.productoverview as po
    JOIN amazon.competitors as c ON po.platformcode = c.asin and po.valuationdate = c.valuationdate
    WHERE c.asin = '{asin}'
    """
    conn = connect_to_db()
    df = pd.read_sql(query, conn)
    conn.close()
    return df