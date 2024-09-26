import requests
import json
from bs4 import BeautifulSoup
from input_files.utils import load_items
from urllib.parse import urlparse
import pandas as pd
import asyncio
import aiohttp
import logging
import random
from openpyxl import Workbook
from datetime import datetime, timezone

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

class YandexMarketReviews:
    def __init__(self):
        logging.info("Starting YandexMarketReviews")
        self.items = load_items()
        self.url = "https://market.yandex.ru/api/render-lazy?w=%40card%2FReviewsLayout"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'cookie': 'i=QKtkBs6AmVyGRKgg+2tpK/T+P1YYUX4yvs/O0wULiCItRZJzZQp2Sz/IAe88ZHzJGqIPDmTjaF6axLOk8MM2jG+hgNE=; yandexuid=1704044661726667882; yashr=1570000711726667882; cmp-merge=true; reviews-merge=true; skid=833951061726805537; nec=0; muid=1152921512212072350%3A4NeHeaNYTl%2Fq6C5PngCjp52NWBwHDRJs; yuidss=1704044661726667882; ymex=2042165542.yrts.1726805542; receive-cookie-deprecation=1; _ym_uid=1726805541493289859; _ym_d=1726805542; yandexmarket=48%2CRUR%2C1%2C%2C%2C%2C2%2C0%2C0%2C213%2C0%2C0%2C12%2C0%2C0; is_gdpr=0; is_gdpr_b=CLmcHRCKlAI=; global_delivery_point_skeleton={%22regionName%22:%22%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%22%2C%22addressLineWidth%22:49.400001525878906}; oq_last_shown_date=1727159318330; oq_shown_onboardings=%5B%5D; spvuid_market:product127996735_expired:1727332490907=1727246090667%2F5f3bd841888f84e94984acd1eb220600%2F1%2F1; spvuid_market:product1774932097_expired:1727334394315=1727247994056%2Ffb70948466245c724ced1f43ec220600%2F1%2F1; spvuid_market:product102542522_expired:1727347949100=1727261549012%2F49bcab397e047ec15226106bef220600%2F1%2F1; spvuid_market:product1450038724_expired:1727348468771=1727262068713%2F2ead1f7c0dbad88575270a8aef220600%2F1%2F1; spvuid_market:product1808043416_expired:1727348576018=1727262175941%2F36ce4e8028374ff2f3526e90ef220600%2F1%2F1; spvuid_market:product1753736848_expired:1727353604954=1727267204895%2Fb7685fbf54325ed6ca142ebcf0220600%2F1%2F1; spvuid_market:product1808094064_expired:1727376870969=1727290470919%2F069331b4c5e522caaf6bf126f6220600%2F1%2F1; visits=1726805537-1727235385-1727330614; parent_reqid_seq=1727330614438%2Fafb2a43caf46fe8762ebae7fff220600%2F1%2F1; rcrr=true; gdpr=0; _ym_isad=2; bh=EkAiR29vZ2xlIENocm9tZSI7dj0iMTI5IiwgIk5vdD1BP0JyYW5kIjt2PSI4IiwgIkNocm9taXVtIjt2PSIxMjkiKgI/MDoJIldpbmRvd3MiYLvy07cGah7cyuH/CJLYobEDn8/h6gP7+vDnDev//fYPtZbNhwg=; _yasc=/EF9lEYzveXRkqUlHqk0n3iISi+6hUJhjB14PzMEiDQXQ0s5Imw41aJRhQH2rFrslyMUscuj6+9JYhER; _yasc=Y17BnvsErZszXqQqMGoni9K+fg2Wvun9ULqRVuxCMZfCCTs0HR9x+rq5IXCGryRI9tgC; i=BfCZ8+Ou85WWOiSmbtRKSssyF0qidewNsK/ivwIjsecr7HGTlPAmOn0gbRb6nVGXgiSooBYIKi32Gk3HB1L9MtXY9e8=; spravka=dD0xNjk1NTcyNjM5O2k9NTQuODYuNTAuMTM5O0Q9RjQ4MjRGRDI5NzM4QTUzREZFOEMxOUQ1NTY3N0Q3MUU5QUQzRThCQURCNjI3RUE1Mjc0MjY5MTc0NkIzQjM1NzFENTc4NzFFNTgzOUU2OTY7dT0xNjk1NTcyNjM5MjkyODE4MjQ5O2g9YjY3YTg4MGM3MDVmM2MwNDI0Zjk5NDNlNjY4YjU0YTg=; yandexuid=9997713411727158771; yashr=478096001727158771',
            'origin': 'https://market.yandex.ru',
            'priority': 'u=1, i',
            'referer': 'https://market.yandex.ru/product--televizor-philips-50pus8507-60/1808094064/reviews?sku=101903223819&uniqueId=34099948&do-waremd5=Ba2XyJdVHwBsRT99DU3FBQ',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sk': 's9f8352c7afd03fd4bdd377f1fe8d5349',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'x-market-app-version': '2024.09.22.2-desktop.t2583578361',
            'x-market-core-service': 'default',
            'x-market-first-req-id': '1727330614438/afb2a43caf46fe8762ebae7fff220600/1/1',
            'x-market-front-glue': '1727330614459983',
            'x-market-page-id': 'market:product-reviews'
            }

        # Initialize the workbook globally
        self.workbook = Workbook()
        self.sheet = self.workbook.active
        self.sheet.append([
            "product_sku",
            "adminproductname",
            "scope",
            "source_code",
            "product_link",
            "data_author",
            "date_published",
            "data_content",
            "data_score",
            "url_status"
        ])
        self.data = []

    def set_payload(self, path, page=1):
        return {
            "widgets": [
                {
                    "lazyId": "cardReviewsLayout42",
                    "widgetName": "@card/ReviewsLayout",
                    "options": {
                        "widgetId": "reviewsPageEntitiesList",
                        "entityWrapperProps": {
                            "paddings": {
                                "top": "5",
                                "bottom": "5"
                            }
                        },
                        "nextPageConfig": "all_product_reviews_web_next_page",
                        "isChefRemixExp": False,
                        "initial": False,
                        "params": {
                            "customConfigName": "all_product_reviews_web_next_page",
                            "reviewPage": str(page)
                        },
                        "widgetSource": "default"
                    },
                    "slotOptions": {
                        "dynamic": True,
                        "measured": True
                    }
                }
            ],
            "path": f"{path}/reviews",
            "widgetsSource": "default",
            "experimental": {}
        }

    def handle_missing(self, item, status):
        item['url_status'] = status
        # Append the missing data to the Excel sheet
        self.sheet.append([
            item.get("product_sku"),
            item.get("adminproductname"),
            item.get("scope"),
            item.get("source_code"),
            item.get("product_link"),
            None,  # Author is None
            None,  # Date published is None
            None,  # Content is None
            None,  # Score is None
            status  # Status indicates why no reviews were found
        ])
        self.save_to_excel()  # Save immediately after appending missing data
        self.data.append(item)

    async def fetch_reviews(self, product_link, item, session):
        logging.info(f"Scraping reviews for {product_link}...")
        page = 1
        while True:
            parsed_url = urlparse(product_link)
            path = parsed_url.path
            payload = self.set_payload(path, page)
            payload = json.dumps(payload)
            async with session.post(self.url, headers=self.headers, data=payload) as response:
                logging.info(f"Response status: {response.status} for {product_link}")
                if response.status == 200:
                    text = await response.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    reviews = soup.find_all('div', attrs={'data-apiary-widget-name': '@card/PaginationEntity'})                    

                    if not reviews:
                        logging.info(f"No reviews found for {product_link}")
                        self.handle_missing(item, "No Reviews")
                        break
                    else:
                        for review in reviews:
                            self.parse_review(review, item)
                else:
                    logging.info(f"Failed to scrape page {page} for {product_link}. Status code: {response.status}")
                    self.handle_missing(item, "Not Found")
                    break
            
            page += 1

    def parse_review(self, review, item):
        author = review.find("span", attrs={"data-auto": "user_name"})
        rating = review.find("span", attrs={"data-auto": "rating-stars"})
        
        widgets = json.loads(review.find("noframes", class_="apiary-patch").text)
                    
        item_dict = widgets["widgets"]["@card/ProductEntitiesPaginationReviewHeader"]
        
        for key, value in item_dict.items():
            timestamp = item_dict[key]["reviewItem"]["reviewDate"]
            if timestamp:
                date = datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc)
                date_published = date.strftime("%Y-%m-%d %H:%M:%S")
                
        if rating:
            rating = rating["data-rate"]
        description = review.select_one('meta[itemprop="description"]')['content'].strip()
        rating_value = review.select_one('meta[itemprop="ratingValue"]')['content'].strip()
        
        # Append the data to the Excel sheet
        self.sheet.append([
            item.get("product_sku"),
            item.get("adminproductname"),
            item.get("scope"),
            item.get("source_code"),
            item.get("product_link"),
            author.text if author else None,
            date_published,
            description,
            rating_value,
            "Data Extracted" if description else "No Reviews"
        ])
        self.save_to_excel()  # Save immediately after appending review data
        self.data.append(item)

    async def run(self):
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_reviews(item['product_link'], item, session) for item in self.items]
            logging.info("Item Count: " + str(len(self.items)))
            for task in tasks:
                await task
                await asyncio.sleep(random.uniform(1, 2))

    def save_to_excel(self):
        excel_filename = 'cleaned2-6. Russian Stable Competitors - YANDEX - Fede (1) 1.xlsx'
        self.workbook.save(excel_filename)
        logging.info(f"Data saved to {excel_filename}")

if __name__ == "__main__":
    yandex_reviews = YandexMarketReviews()
    asyncio.run(yandex_reviews.run())
    yandex_reviews.save_to_excel()
