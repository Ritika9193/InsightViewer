import streamlit as st
import psycopg2
import sys
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import warnings
# sys.path.append("Z:\\airflow\\scripts\\ReviewScraper")
# sys.path.append('/opt/airflow/scripts/ReviewScrpaer')
from AmazonDatabase import (
    get_sales_and_reviews_history,
    get_sales_and_rating_history,
    get_current_price_rating,
    get_sentiment_summary_and_keywords,
    get_price_rating_history,
    get_categories,
    get_avgprice_rating_history,
    get_whsku_for_category,
    get_category_summary,
    get_sku_data_for_whsku,
    product_comp_rating
)

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

st.set_page_config(layout="wide")
if 'avgprice_history' not in st.session_state:
    st.session_state['avgprice_history'] = pd.DataFrame(columns=['valuationdate', 'avgprice'])

if 'avgrating_history' not in st.session_state:
    st.session_state['avgrating_history'] = pd.DataFrame(columns=['valuationdate', 'avgrating'])

st.title('Amazon Sentiment Analysis')
categories = get_categories()

selected_category = st.selectbox("Select Category", options=[""] + categories)

if selected_category:
    category_summary = get_category_summary(selected_category)
    if category_summary is not None:

        st.subheader(f"Category: {selected_category}")

        avgrating=category_summary['avgrating'] if category_summary['avgrating'] is not None else -1
        st.write(f"**Average Rating:** {avgrating}/5")

        avgprice = category_summary['avgprice'] if category_summary['avgprice'] is not None else -1
        st.write(f"**Average Price:** ₹{avgprice:.2f}")

        st.write('\n\n\n')
        st.subheader(f"Sentiment Summary for {selected_category}:")
        st.write(category_summary['categorysummary'])

        st.markdown("<br><br>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Positive Keywords"):
                st.session_state.show_positive = not st.session_state.get('show_positive', False)
            if st.session_state.get('show_positive', False):
                st.subheader("Positive Keywords")
                st.write(category_summary['positivekeywords'])

        with col2:
            if st.button("Negative Keywords"):
                st.session_state.show_negative = not st.session_state.get('show_negative', False)
            if st.session_state.get('show_negative', False):
                st.subheader("Negative Keywords")
                st.write(category_summary['negativekeywords'])

        with col3:
            if st.button("Mixed Keywords"):
                st.session_state.show_mixed = not st.session_state.get('show_mixed', False)
            if st.session_state.get('show_mixed', False):
                st.subheader("Mixed Keywords")
                st.write(category_summary['mixedkeywords'])

        st.markdown("<br><br>", unsafe_allow_html=True)

        avg_price_rating_data, min_price, max_price = get_avgprice_rating_history(selected_category)
        if not avg_price_rating_data.empty:
                price_history = avg_price_rating_data[['valuationdate', 'avgprice']].drop_duplicates(subset='valuationdate', keep='last').sort_values(by='valuationdate')
                rating_history = avg_price_rating_data[['valuationdate', 'avgrating']].drop_duplicates(subset='valuationdate', keep='last').sort_values(by='valuationdate')
                gap = (max_price - min_price) / 10
                yaxis_price_range = [min_price - gap, max_price + gap] if gap > 0 else [min_price, max_price]
                col1, col2 = st.columns(2)
                with col1:
                    st.write("### Average Price over Time")
                    fig_price = go.Figure()
                    fig_price.add_trace(go.Scatter(
                        x=price_history['valuationdate'],
                        y=price_history['avgprice'],
                        mode='lines+markers',
                        name='Avg Price'
                    ))
                    fig_price.update_layout(
                        xaxis_title='Date',
                        yaxis_title='Avg Price',
                        xaxis_rangeslider_visible=False,
                        xaxis_range=[price_history['valuationdate'].iloc[-10], price_history['valuationdate'].iloc[-1]],
                        yaxis_range=yaxis_price_range,
                        yaxis=dict(
                            tick0=min_price,
                            dtick=gap
                        ),
                        height=500,
                        hovermode='x unified'
                    )
                    st.plotly_chart(fig_price, use_container_width=True)
                with col2:
                    st.write("### Average Rating over Time")
                    fig_rating = go.Figure()
                    fig_rating.add_trace(go.Scatter(
                        x=rating_history['valuationdate'],
                        y=rating_history['avgrating'],
                        mode='lines+markers',
                        name='Avg Rating'
                    ))
                    fig_rating.update_layout(
                        xaxis_title='Date',
                        yaxis_title='Avg Rating',
                        xaxis_rangeslider_visible=False,
                        xaxis_range=[rating_history['valuationdate'].iloc[-10], rating_history['valuationdate'].iloc[-1]],
                        yaxis_range=[0, 5],
                        yaxis=dict(
                            tick0=0,
                            dtick=0.5
                        ),
                        height=500,
                        hovermode='x unified',
                    )
                    st.plotly_chart(fig_rating, use_container_width=True)
        else:
            st.error("No price or rating data found for the selected category.")
else:
    st.info("Please select a category.")


if 'price_history' not in st.session_state:
    st.session_state['price_history'] = pd.DataFrame(columns=['valuationdate', 'price'])

if 'rating_history' not in st.session_state:
    st.session_state['rating_history'] = pd.DataFrame(columns=['valuationdate', 'rating'])

st.markdown("<br><br>", unsafe_allow_html=True)

def color_ratings(rating):
    """Return background color based on rating value."""
    if rating < 4.1:
        return 'background-color: rgba(255, 0, 0, 0.3);'  
    elif rating > 4.3:
        return 'background-color: rgba(0, 255, 0, 0.3);'  
    else:
        return 'background-color: rgba(255, 255, 0, 0.3);'  

if selected_category:
    sku_data = get_sku_data_for_whsku(selected_category) 
    if sku_data is not None and not sku_data.empty:

        styled_sku_data = sku_data.style.format({
            'rating': '{:.2f}',  
            'avg_rating_last_10_days': '{:.2f}', 
            'totalreviews': '{:,.0f}'
        }).apply(
            lambda row: [color_ratings(row['rating'])] * len(row), axis=1
        )
        st.dataframe(styled_sku_data, use_container_width=True)
    else:
        st.warning("No data found for the selected category.")
    

    whsku_asin_map = get_whsku_for_category(selected_category)

    if whsku_asin_map:

        selected_whsku = st.selectbox("Select WHSKU", options=[""] + list(whsku_asin_map.keys()))

        if selected_whsku:
            selected_asin = whsku_asin_map[selected_whsku] 

            current_price_rating = get_current_price_rating(selected_asin)
    
            if current_price_rating is not None:
                st.write(f"**Price :** ₹{current_price_rating['price']:.2f}")
                st.write(f"**Rating :** {current_price_rating['rating']}/5")
                total_reviews = current_price_rating['totalreviews'] if current_price_rating['totalreviews'] is not None else -1
                st.write(f"**Total Reviews :** {total_reviews:.0f}")
            else:
                st.error("No price or rating data available for today.")


            result = get_sentiment_summary_and_keywords(selected_asin)

            
            if result is not None:
                st.write('\n\n\n')
                st.subheader("Sentiment Summary:")
                st.write(result['summary'])
                st.markdown("<br><br>", unsafe_allow_html=True)
                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("Positive-Keywords"):
                        st.session_state.show_positive = not st.session_state.get('show_positive', False)
                    if st.session_state.get('show_positive', False):
                        st.subheader("Positive Keywords")
                        st.write(result['positivekeywords'])

                with col2:
                    if st.button("Negative-Keywords"):
                        st.session_state.show_negative = not st.session_state.get('show_negative', False)
                    if st.session_state.get('show_negative', False):
                        st.subheader("Negative Keywords")
                        st.write(result['negativekeywords'])

                with col3:
                    if st.button("Mixed-Keywords"):
                        st.session_state.show_mixed = not st.session_state.get('show_mixed', False)
                    if st.session_state.get('show_mixed', False):
                        st.subheader("Mixed Keywords")
                        st.write(result['mixedkeywords'])
            else:
                st.error("No data found for the selected product.")

            st.markdown("<br><br>", unsafe_allow_html=True)

            price_rating_data, min_price_sku, max_price_sku = get_price_rating_history(selected_asin)
            if not price_rating_data.empty:
                price_history = price_rating_data[['valuationdate', 'price']].drop_duplicates(subset='valuationdate', keep='last')
                rating_history = price_rating_data[['valuationdate', 'rating']].drop_duplicates(subset='valuationdate', keep='last')
                gap = (max_price_sku - min_price_sku) / 10
                yaxis_price_range = [min_price_sku - gap, max_price_sku + gap] if gap > 0 else [min_price_sku, max_price_sku]
                col1, col2 = st.columns(2)
                with col1:
                    st.write("### Price over Time")
                    fig_price = go.Figure()
                    fig_price.add_trace(go.Scatter(
                        x=price_history['valuationdate'],
                        y=price_history['price'],
                        mode='lines+markers',
                        name='Price'
                    ))
                    fig_price.update_layout(
                        xaxis_title='Date',
                        yaxis_title='Price',
                        xaxis_rangeslider_visible=False,
                        yaxis_range=yaxis_price_range,
                        yaxis=dict(
                            tick0=min_price_sku,
                            dtick=gap
                        ),
                        hovermode='x unified',
                        # xaxis_range=[price_history['valuationdate'].iloc[-10], price_history['valuationdate'].iloc[-1]],
                        height=500
                    )
                    st.plotly_chart(fig_price, use_container_width=True)
                with col2:
                    st.write("### Rating over Time")
                    fig_rating = go.Figure()
                    fig_rating.add_trace(go.Scatter(
                        x=rating_history['valuationdate'],
                        y=rating_history['rating'],
                        mode='lines+markers',
                        name='Rating'
                    ))
                    fig_rating.update_layout(
                        xaxis_title='Date',
                        yaxis_title='Rating',
                        xaxis_rangeslider_visible=False,
                        yaxis_range=[0, 5],
                        yaxis=dict(
                            tick0=0,
                            dtick=0.5
                        ),
                        hovermode='x unified',
                        # xaxis_range=[rating_history['valuationdate'].iloc[-10], rating_history['valuationdate'].iloc[-1]],
                        height=500
                    )
                    st.plotly_chart(fig_rating, use_container_width=True)

            else:
                st.error("No price or rating data found for the selected product.")

            st.markdown("<br><br>", unsafe_allow_html=True)

            combined_data = get_sales_and_rating_history(selected_asin)

            if not combined_data.empty:
                rating_history = combined_data[['valuationdate', 'rating']].drop_duplicates(subset='valuationdate', keep='last')
                sales_history = combined_data[['valuationdate', 'netsales']].drop_duplicates(subset='valuationdate', keep='last')
                rating_history['rating'] = rating_history['rating'].round(2)
                sales_history['netsales']=sales_history['netsales'].round(2)
                st.write("### Sales and Rating over Time(3 Days Average)")
                fig_comparison = go.Figure()

                fig_comparison.add_trace(go.Scatter(
                    x=sales_history['valuationdate'],
                    y=sales_history['netsales'],
                    mode='lines+markers',
                    name='Sales ',
                    line=dict(color='blue'),  
                    yaxis='y'
                ))
                fig_comparison.add_trace(go.Scatter(
                    x=rating_history['valuationdate'],
                    y=rating_history['rating'],
                    mode='lines+markers',
                    name='Rating',
                    line=dict(color='green'), 
                    yaxis='y2'
                ))

                fig_comparison.update_layout(
                    xaxis_title='Date',
                    yaxis=dict(
                        title='Net Sales',
                        titlefont=dict(color='blue'),
                        tickfont=dict(color='blue'),
                        range=[0, sales_history['netsales'].max() ]
                    ),
                    yaxis2=dict(
                        title='Ratings',
                        titlefont=dict(color='green'),
                        tickfont=dict(color='green'),
                        overlaying='y', 
                        side='right',
                        range=[0, rating_history['rating'].max() + 0.5]
                    ),
                    # xaxis_range=[sales_history['valuationdate'].iloc[-10], sales_history['valuationdate'].iloc[-1]],
                    xaxis_rangeslider_visible=False,
                    height=500,
                    hovermode='x unified' 
                )
                st.plotly_chart(fig_comparison, use_container_width=True)

            
            combined_sales_and_reviews_data = get_sales_and_reviews_history(selected_asin)

            if not combined_sales_and_reviews_data.empty:
                reviews_history = combined_sales_and_reviews_data[['valuationdate', 'totalreviews']].drop_duplicates(subset='valuationdate', keep='last')
                sales_history = combined_sales_and_reviews_data[['valuationdate', 'netsales']].drop_duplicates(subset='valuationdate', keep='last')

                st.write("### Sales and No. Of Reviews over Time (last 7 Days Average)")
                fig_comparison = go.Figure()
                fig_comparison.add_trace(go.Bar(
                    x=sales_history['valuationdate'],
                    y=sales_history['netsales'],
                    name='Sales',
                    marker_color='blue',
                    yaxis='y',
                    offsetgroup=1  
                ))
                fig_comparison.add_trace(go.Bar(
                    x=reviews_history['valuationdate'],
                    y=reviews_history['totalreviews'],
                    name='Reviews',
                    marker_color='green',
                    yaxis='y2',
                    offsetgroup=2 
                ))
                fig_comparison.update_layout(
                    xaxis_title='Date',
                    yaxis=dict(
                        title='Net Sales',
                        titlefont=dict(color='blue'),
                        tickfont=dict(color='blue')
                    ),
                    yaxis2=dict(
                        title='Total Reviews',
                        titlefont=dict(color='green'),
                        tickfont=dict(color='green'),
                        overlaying='y', 
                        side='right'  
                    ),
                    barmode='group', 
                    bargap=0.5,
                    # xaxis_range=[sales_history['valuationdate'].iloc[-10], sales_history['valuationdate'].iloc[-1]], 
                    xaxis_rangeslider_visible=False,
                    height=500,
                )

                st.plotly_chart(fig_comparison, use_container_width=True)

            else:
                st.error("No sales or review data found for the selected product.")

            st.write("### ASIN vs Competitors' Rating Comparison")

            if selected_asin:
                asin = selected_asin
                df = product_comp_rating(asin)

            if not df.empty:
                product_sku = df['sku'].iloc[0]
                df.rename(columns={'product_rating': product_sku}, inplace=True)

                df = df.groupby(['valuationdate', 'compsku'], as_index=False).agg({
                    product_sku: 'first',      
                    'comprating': 'mean'       
                })

                df_melted = pd.melt(df, id_vars=['valuationdate'], 
                                    value_vars=[product_sku], 
                                    var_name='SKU', value_name='rating')
                fig = px.line(df_melted, x='valuationdate', y='rating', color='SKU',
                            title=f'Rating Comparison for - {product_sku} and Competitors',
                            labels={
                                'valuationdate': 'Valuation Date', 
                                'rating': 'Rating',
                            },
                            markers=True)
                
                fig.update_traces(
                            selector=dict(name=product_sku), 
                            line=dict(color='green',width=4),         
                            marker=dict(size=10, symbol='circle')  
                        )
                
                for compsku in df['compsku'].unique():
                    comp_df = df[df['compsku'] == compsku]
                    fig.add_scatter(
                        x=comp_df['valuationdate'], y=comp_df['comprating'],
                        mode='lines+markers', name=f'{compsku}',
                        hovertemplate='Valuation Date: %{x}<br>' +
                                    'Competitor Rating: %{y}<br>' +
                                    'Competitor SKU: ' + compsku +
                                    '<extra></extra>',
                        connectgaps=True 
                    )

                fig.update_yaxes(range=[0, 5], tick0=0, dtick=0.5)
                fig.update_layout(height=600)
                st.plotly_chart(fig)
            else:
                st.write("No data found for the selected ASIN.")
