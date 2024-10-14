# import asyncio
# import pandas as pd
# from playwright.async_api import async_playwright,TimeoutError
# from datetime import datetime
# from AmazonDatabase import get_platform_code

# async def take_screenshot(page, filename):
#     await page.screenshot(path=filename)

# async def scrape_details(asin):
#     url=f"https://www.amazon.in/dp/{asin}"
#     async with async_playwright() as p:
#         browser=await p.chromium.launch(headless=True)
#         page=await browser.new_page()

#         try:
#             await page.goto(url, timeout=30000)
#         except TimeoutError:
#             print(f"TimeoutError: Failed to load page for ASIN: {asin}")
#             await take_screenshot(page, f"screenshots/{asin}_error_loading_page.png")
#             await browser.close()
#             return pd.DataFrame()

#         competitors_info={'ASIN':asin}

#         competitors_info['valuationdate'] = datetime.now().strftime('%Y-%m-%d')

#         try:
#             rating_text = (await (await page.query_selector('span.a-icon-alt')).inner_text()).strip()
#             competitors_info['rating'] = rating_text.split(' ')[0] if rating_text else -1
#             print(f"Rating: {competitors_info['rating']}")
#         except Exception as e:
#             competitors_info['rating'] = -1
#             await take_screenshot(page, f"screenshots/{asin}_error_rating.png")

#         try:
#             brand_text = (await (await page.query_selector('tr.po-brand .po-break-word')).inner_text()).strip()
#             competitors_info['brand'] = brand_text if brand_text else 'Unknown'
#             print(f"Brand: {competitors_info['brand']}")
#         except Exception as e:
#             competitors_info['brand'] = 'Unknown'
#             await take_screenshot(page, f"screenshots/{asin}_error_brand.png")
        
#         try:
#             count_text =await page.inner_text('xpath=//*[@id="social-proofing-faceout-title-tk_bought"]/span[1]')
#             competitors_info['count'] = count_text.split()[0] if count_text else 'Unknown'
#             print(f"Count: {competitors_info['count']}")
#         except Exception as e:
#             competitors_info['count'] = 'Unknown'
#             await take_screenshot(page, f"screenshots/{asin}_error_count.png")


#         await browser.close()
#         return pd.DataFrame([competitors_info])




# async def competitors_scrapper():
#     asins=get_platform_code()
#     for asin in asins:
#         competitor_df = await scrape_details(asin)
#         print (competitor_df)
    
# asyncio.run(competitors_scrapper())



# import asyncio
# import pandas as pd
# from playwright.async_api import async_playwright, TimeoutError
# from datetime import datetime
# from AmazonDatabase import save_comp_data
# def load_google_sheet(sheet_id):
#     df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv")
#     print("Google Sheet data loaded:")
#     print(df) 
#     return df

# async def take_screenshot(page, filename):
#     await page.screenshot(path=filename)

# async def scrape_details(BSCasin,asin, competitor_name, category):
#     url = f"https://www.amazon.in/dp/{asin}"
#     print(f"Scraping details for ASIN: {asin}") 
#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=True)
#         page = await browser.new_page()

#         try:
#             await page.goto(url, timeout=30000)
#         except TimeoutError:
#             print(f"TimeoutError: Failed to load page for ASIN: {asin}")
#             await take_screenshot(page, f"screenshots/{asin}_error_loading_page.png")
#             await browser.close()
#             return pd.DataFrame()

#         competitors_info = {'ASIN': BSCasin,'Competitor ASIN': asin, 'Competitor SKU': competitor_name, 'Category': category}
#         print(f"ASIN: {competitors_info['ASIN']}")
#         print(f"Competitor ASIN: {competitors_info['Competitor ASIN']}")
#         print(f"Competitor SKU: {competitors_info['Competitor SKU']}")
#         print(f"Category: {competitors_info['Category']}")

#         competitors_info['valuationdate'] = datetime.now().strftime('%Y-%m-%d')
#         print(f"Date: {competitors_info['valuationdate']}")

#         try:
#             rating_text = (await (await page.query_selector('span.a-icon-alt')).inner_text()).strip()
#             competitors_info['rating'] = rating_text.split(' ')[0] if rating_text else -1
#             print(f"Rating: {competitors_info['rating']}")  
#         except Exception as e:
#             competitors_info['rating'] = -1
#             print(f"Error retrieving rating for ASIN {asin}: {e}")  
#             await take_screenshot(page, f"screenshots/{asin}_error_rating.png")

#         try:
#             brand_text = (await (await page.query_selector('tr.po-brand .po-break-word')).inner_text()).strip()
#             competitors_info['brand'] = brand_text if brand_text else 'Unknown'
#             print(f"Brand: {competitors_info['brand']}")  
#         except Exception as e:
#             competitors_info['brand'] = 'Unknown'
#             print(f"Error retrieving brand for ASIN {asin}: {e}")  
#             await take_screenshot(page, f"screenshots/{asin}_error_brand.png")

#         try:
#             count_text = await page.inner_text('xpath=//*[@id="social-proofing-faceout-title-tk_bought"]/span[1]')
#             competitors_info['count'] = count_text.split('+')[0] if count_text else 'Unknown'
#             print(f"Count: {competitors_info['count']}")  
#         except Exception as e:
#             competitors_info['count'] = 'Unknown'
#             print(f"Error retrieving count for ASIN {asin}: {e}")  
#             await take_screenshot(page, f"screenshots/{asin}_error_count.png")

#         await browser.close()
#         return pd.DataFrame([competitors_info])


# async def competitors_scrapper():
#     sheet_id = '1mzO0A9_WjbXoUu7PfmB_BC1bKQv6dHEO302IUb1smlc'
#     sheet_data = load_google_sheet(sheet_id)
#     # asins = ['B0D2HFN2N1']
#     asins = ['B0D2HFN2N1','B098P8NRRW','B0D91S9NF1','B0D96CPM1B','B0CKLP826Z','B0CHSCR7B7','B0CGNHGXXH']
#     print(f"ASINs from platform: {asins}")

#     for asin in asins:
#         matched_row = sheet_data[sheet_data['BSC asin'] == asin]
#         if not matched_row.empty:
#             print(f"Matched row found for ASIN: {asin}")
#         else:
#             print(f"No match found for ASIN: {asin}")
#             continue

#         competitor_asins = [matched_row[f'Comp {i}'].values[0] for i in range(1, 6) if f'Comp {i}' in matched_row.columns]
#         competitor_names = [matched_row[f'Comp {i} name'].values[0] for i in range(1, 6) if f'Comp {i} name' in matched_row.columns]
#         category = matched_row['Category'].values[0] if 'Category' in matched_row.columns else 'Unknown'
#         print(f"Competitor ASINs for {asin}: {competitor_asins}, Category: {category}")
#         for i, comp_asin in enumerate(competitor_asins):
#             if pd.isna(comp_asin) or comp_asin == '':
#                 print(f"Skipping empty competitor ASIN for {asin}")
#                 continue
#             competitor_name = competitor_names[i] if i < len(competitor_names) else 'Unknown'
#             competitor_df = await scrape_details(asin, comp_asin, competitor_name, category)
#             print(competitor_df) 
#             save_comp_data(competitor_df)
# asyncio.run(competitors_scrapper())




import asyncio
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError
from datetime import datetime
from AmazonDatabase import save_comp_data

def load_google_sheet(sheet_id):
    df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv")
    print("Google Sheet data loaded:")
    print(df) 
    return df

async def take_screenshot(page, filename):
    await page.screenshot(path=filename)

async def scrape_details(BSCasin, bsc_trimmer, asin, competitor_name, category):
    url = f"https://www.amazon.in/dp/{asin}"
    print(f"Scraping details for ASIN: {asin}") 
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            await page.goto(url, timeout=30000)
        except TimeoutError:
            print(f"TimeoutError: Failed to load page for ASIN: {asin}")
            await take_screenshot(page, f"screenshots/{asin}_error_loading_page.png")
            await browser.close()
            return pd.DataFrame()

        competitors_info = {
            'ASIN': BSCasin,
            'BSC Trimmer': bsc_trimmer,  
            'Competitor ASIN': asin,
            'Competitor SKU': competitor_name,
            'Category': category
        }
        print(f"ASIN: {competitors_info['ASIN']}")
        print(f"BSC Trimmer: {competitors_info['BSC Trimmer']}")  
        print(f"Competitor ASIN: {competitors_info['Competitor ASIN']}")
        print(f"Competitor SKU: {competitors_info['Competitor SKU']}")
        print(f"Category: {competitors_info['Category']}")

        competitors_info['valuationdate'] = datetime.now().strftime('%Y-%m-%d')
        print(f"Date: {competitors_info['valuationdate']}")

        try:
            rating_text = (await (await page.query_selector('span.a-icon-alt')).inner_text()).strip()
            competitors_info['rating'] = rating_text.split(' ')[0] if rating_text else -1
            print(f"Rating: {competitors_info['rating']}")  
        except Exception as e:
            competitors_info['rating'] = -1
            print(f"Error retrieving rating for ASIN {asin}: {e}")  
            await take_screenshot(page, f"screenshots/{asin}_error_rating.png")

        try:
            brand_text = (await (await page.query_selector('tr.po-brand .po-break-word')).inner_text()).strip()
            competitors_info['brand'] = brand_text if brand_text else 'Unknown'
            print(f"Brand: {competitors_info['brand']}")  
        except Exception as e:
            competitors_info['brand'] = 'Unknown'
            print(f"Error retrieving brand for ASIN {asin}: {e}")  
            await take_screenshot(page, f"screenshots/{asin}_error_brand.png")

        try:
            count_text = await page.inner_text('xpath=//*[@id="social-proofing-faceout-title-tk_bought"]/span[1]')
            competitors_info['count'] = count_text.split('+')[0] if count_text else 'Unknown'
            print(f"Count: {competitors_info['count']}")  
        except Exception as e:
            competitors_info['count'] = 'Unknown'
            print(f"Error retrieving count for ASIN {asin}: {e}")  
            await take_screenshot(page, f"screenshots/{asin}_error_count.png")

        await browser.close()
        return pd.DataFrame([competitors_info])

async def competitors_scrapper():
    sheet_id = '1mzO0A9_WjbXoUu7PfmB_BC1bKQv6dHEO302IUb1smlc'
    sheet_data = load_google_sheet(sheet_id)
    
    # Get all unique BSC ASINs from the DataFrame
    bsc_asins = sheet_data['BSC asin'].unique()
    print(f"BSC ASINs from platform: {bsc_asins}")

    for bsc_asin in bsc_asins:
        matched_row = sheet_data[sheet_data['BSC asin'] == bsc_asin]
        if not matched_row.empty:
            print(f"Matched row found for BSC ASIN: {bsc_asin}")
        else:
            print(f"No match found for BSC ASIN: {bsc_asin}")
            continue

        # Get BSC Trimmer for the current row
        bsc_trimmer = matched_row['BSC Trimmer'].values[0] if 'BSC Trimmer' in matched_row.columns else 'Unknown'
        print(f"BSC Trimmer: {bsc_trimmer}")

        # Get all competitors dynamically
        competitor_asins = []
        competitor_names = []
        
        for col in matched_row.columns:
            if 'Comp' in col:
                competitor_asins.append(matched_row[col].values[0])
                name_col = col + ' name'
                if name_col in matched_row.columns:
                    competitor_names.append(matched_row[name_col].values[0])
        
        category = matched_row['Category'].values[0] if 'Category' in matched_row.columns else 'Unknown'
        print(f"Competitor ASINs for {bsc_asin}: {competitor_asins}, Category: {category}")

        for i, comp_asin in enumerate(competitor_asins):
            if pd.isna(comp_asin) or comp_asin == '':
                print(f"Skipping empty competitor ASIN for {bsc_asin}")
                continue
            competitor_name = competitor_names[i] if i < len(competitor_names) else 'Unknown'
            competitor_df = await scrape_details(bsc_asin, bsc_trimmer, comp_asin, competitor_name, category)
            print(competitor_df) 
            save_comp_data(competitor_df)

asyncio.run(competitors_scrapper())
