import asyncio
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError
from datetime import datetime
import sys
# sys.path.append("Z:\\airflow\\scripts\\ReviewScraper")
# sys.path.append('/opt/airflow/scripts/ReviewScrpaer')
from AmazonDatabase import get_platform_code , save_to_database

async def take_screenshot(page, filename):
    await page.screenshot(path=filename)

async def scrape_amazon_product(asin):
    url = f"https://www.amazon.in/dp/{asin}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            await page.goto(url, timeout=30000)
        except TimeoutError:
            print(f"TimeoutError: Failed to load page for ASIN: {asin}")
            await take_screenshot(page, f"screenshots/{asin}_error_loading_page.png")
            await browser.close()
            return pd.DataFrame(), pd.DataFrame()

        product_info = {'ASIN': asin}

        try:
            product_info['title'] = (await page.title()).strip()[:255]
            print(f"Title: {product_info['title']}")
        except Exception as e:
            product_info['title'] = "0000"
            await take_screenshot(page, f"screenshots/{asin}_error_title.png")

        try:
            price_whole_element = await page.query_selector('span.a-price-whole')
            price_fraction_element = await page.query_selector('span.a-price-fraction')
            if price_whole_element:
                price_whole = (await price_whole_element.inner_text()).replace('\n', '').strip()
                price_fraction = (await price_fraction_element.inner_text()).replace('\n', '').strip() if price_fraction_element else "00"
                product_info['price'] = f"{price_whole}.{price_fraction}".replace('..', '.')
                product_info['price'] = product_info['price'].replace(',', '')  
                print(f"Price: {product_info['price']}")
            else:
                product_info['price'] = -1
        except Exception as e:
            product_info['price'] = -1
            await take_screenshot(page, f"screenshots/{asin}_error_price.png")

        try:
            rating_text = (await (await page.query_selector('span.a-icon-alt')).inner_text()).strip()
            product_info['rating'] = rating_text.split(' ')[0] if rating_text else -1
            print(f"Rating: {product_info['rating']}")
        except Exception as e:
            product_info['rating'] = -1
            await take_screenshot(page, f"screenshots/{asin}_error_rating.png")

        try:
            rating_count_text = (await (await page.query_selector('span#acrCustomerReviewText')).inner_text()).replace('\n', '').strip()
            product_info['NumberOfRatings'] = ''.join(filter(str.isdigit, rating_count_text)) if rating_count_text else "Ratings count not found"
            print(f"Number of Ratings: {product_info['NumberOfRatings']}")
        except Exception as e:
            product_info['NumberOfRatings'] = "-1"
            await take_screenshot(page, f"screenshots/{asin}_error_reviews_count.png")

        try:
            see_all_reviews_link = await page.query_selector('a[data-hook="see-all-reviews-link-foot"]')
            if see_all_reviews_link:
                await see_all_reviews_link.click()
                await page.select_option('select#sort-order-dropdown', 'recent')
                await page.wait_for_timeout(2000)
            else:
                raise Exception("See all reviews link not found")
        except Exception as e:
            product_info['reviews_navigation_error'] = f"Error: {str(e)}"
            await take_screenshot(page, f"screenshots/{asin}_error_navigation.png")

        try:
            review_count_text = await page.query_selector('div[data-hook="cr-filter-info-review-rating-count"]')
            if review_count_text:
                reviewscount = await review_count_text.inner_text()

                # Extract the number of reviews using simple text manipulation
                if 'with reviews' in reviewscount:
                    reviews_part = reviewscount.split('with reviews')[0].strip()
                    number_of_reviews = ''.join(filter(str.isdigit, reviews_part.split()[-1]))
                    product_info['NumberOfReviews'] = number_of_reviews
                    print(f"Number of Reviews: {number_of_reviews}")
                else:
                    product_info['NumberOfReviews'] = "-1"
                    print("Could not find 'with reviews' in the text.")
            else:
                product_info['NumberOfReviews'] = "-1"
                print("Could not find the filter info section.")
        except Exception as e:
            product_info['NumberOfReviews'] = f"Error: {str(e)}"
            await take_screenshot(page, f"screenshots/{asin}_error_navigation.png")

        product_info['valuationdate'] = datetime.now().strftime('%Y-%m-%d')

        reviews = []
        while True:
            try:
                review_elements = await page.query_selector_all('div[data-hook="review"]')
                for review_element in review_elements:
                    review = {}
                    try:
                        review['review_id'] = await review_element.get_attribute('id')
                    except Exception as e:
                        review['review_id'] = f"Error: {str(e)}"
                        await take_screenshot(page, f"screenshots/{asin}_error_review_id.png")

                    try:
                        review['name'] = (await (await review_element.query_selector('span.a-profile-name')).inner_text()).strip()[:255]
                    except Exception as e:
                        review['name'] = f"Error: {str(e)}"
                        await take_screenshot(page, f"screenshots/{asin}_error_reviewer_name.png")

                    try:
                        review_text = (await (await review_element.query_selector('i[data-hook="review-star-rating"] span.a-icon-alt')).inner_text()).strip()
                        review['rating'] = review_text.split(' ')[0]
                    except Exception as e:
                        review['rating'] = "-1"
                        await take_screenshot(page, f"screenshots/{asin}_error_review_rating.png")

                    try:
                        review['review'] = (await (await review_element.query_selector('span[data-hook="review-body"] span')).inner_text()).strip()
                        # print(review['review'])
                    except Exception as e:
                        review['review'] = f"Error: {str(e)}"
                        await take_screenshot(page, f"screenshots/{asin}_error_review_text.png")

                    try:
                        try:
                            review_date_text = (await (await review_element.query_selector('span[data-hook="review-date"]')).inner_text()).replace("Reviewed in India on ", "").strip()
                        except:
                            try:
                                review_date_text = (await (await review_element.query_selector('span[data-hook="review-date"]')).inner_text()).replace("Reviewed in India on ", "").strip()
                            except:
                                pass
            
                        review['date'] = datetime.strptime(review_date_text, '%d %B %Y').strftime('%Y-%m-%d')
                    except Exception as e:
                        review['date'] = datetime.now().strftime('%Y-%m-%d')
                        await take_screenshot(page, f"screenshots/{asin}_error_review_date.png")

                    review['ASIN'] = asin
                    review['valuationdate'] = datetime.now().strftime('%Y-%m-%d')
                    reviews.append(review)

                next_page_link = await page.query_selector('li.a-last a')
                if not next_page_link:
                    break
                await next_page_link.click()
                await page.wait_for_timeout(2000)

            except Exception as e:
                print(f"Error while scraping reviews for ASIN: {asin}. Error: {str(e)}")
                await take_screenshot(page, f"screenshots/{asin}_error_scraping_reviews.png")
                break

        await browser.close()

    product_info_df = pd.DataFrame([product_info])
    reviews_df = pd.DataFrame(reviews)
    return product_info_df, reviews_df

async def amazon_review_scraper():
    asins = ['B0D2HFN2N1','B098P8NRRW','B0D91S9NF1','B0D96CPM1B','B0CKLP826Z','B0CHSCR7B7','B0CGNHGXXH']


    all_products_df = pd.DataFrame()
    all_reviews_df = pd.DataFrame()

    for asin in asins:
        product_df, reviews_df = await scrape_amazon_product(asin)
        # all_products_df = pd.concat([all_products_df, product_df], ignore_index=True)
        # all_reviews_df = pd.concat([all_reviews_df, reviews_df], ignore_index=True)
        save_to_database(product_df, reviews_df)

asyncio.run(amazon_review_scraper())
 





















