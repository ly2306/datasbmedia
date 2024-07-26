from flask import Flask, render_template, request, flash, redirect, url_for
from celery import Celery
import gspread
from google.oauth2.service_account import Credentials
import requests
from urllib.parse import urljoin
import bs4
import unicodedata
import re
import time
import random
import threading
from datetime import datetime
import webbrowser
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(filename='stderr.log', level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Celery configuration
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'  # Update with your broker URL
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

# Google Sheets credentials and scope setup
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
credentials_file = 'cre.json'  # Replace with your credentials file
creds = Credentials.from_service_account_file(credentials_file, scopes=scope)

# Base URL for districts
base_url = 'https://infocom.vn/'

# Random User-Agent headers
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
]
headers = {
    'User-Agent': random.choice(user_agents),
    'Referer': 'https://infocom.vn/'
}

def authorize_google_sheets(creds):
    try:
        # Authenticate and return Google Sheets client
        return gspread.authorize(creds)
    except Exception as e:
        logging.error(f"Error authorizing Google Sheets: {e}")
        return None

def find_existing_spreadsheet(client, spreadsheet_name):
    try:
        # Get list of spreadsheets from Google Drive
        drive_files = client.list_spreadsheet_files()

        # Check if spreadsheet exists
        for file in drive_files:
            if file['name'] == spreadsheet_name:
                return file['id']  # Return ID of existing spreadsheet
        
        return None  # Return None if spreadsheet not found

    except Exception as e:
        logging.error(f"Error finding spreadsheet: {e}")
        return None

def create_or_get_spreadsheet(client, spreadsheet_name):
    try:
        # Check if spreadsheet already exists
        spreadsheet_id = find_existing_spreadsheet(client, spreadsheet_name)

        if spreadsheet_id:
            logging.info(f"Spreadsheet '{spreadsheet_name}' already exists.")
            return spreadsheet_id
        
        else:
            # If spreadsheet doesn't exist, create new one
            spreadsheet_name_encoded = spreadsheet_name.encode('utf-8')  # Ensure UTF-8 encoding
            spreadsheet = client.create(spreadsheet_name_encoded.decode('utf-8'))  # Decode back to string

            spreadsheet_key = spreadsheet.id

            logging.info(f"Created new spreadsheet '{spreadsheet_name}' with key '{spreadsheet_key}'.")

            # Share spreadsheet with write access
            spreadsheet.share('phanvothanhly462@gmail.com', perm_type='user', role='writer', email_message='Welcome to the spreadsheet!')

            # Share the spreadsheet with anyone with the link with write access
            spreadsheet.share('', perm_type='anyone', role='writer')

            return spreadsheet_key

    except Exception as e:
        logging.error(f"Error creating or accessing spreadsheet: {e}")
        return None

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

def get_url_segment_from_name(spreadsheet_name):
    formatted_name = remove_accents(spreadsheet_name.strip().lower())
    formatted_name = re.sub(r'[^\w\s-]', '', formatted_name)
    formatted_name = re.sub(r'\s+', '-', formatted_name)
    return formatted_name

def get_district_urls(base_url, formatted_name=''):
    district_urls = {}
    url = urljoin(base_url, formatted_name) if formatted_name else base_url
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        ul_tag = soup.find('ul', class_='list-districts-wards-paging')

        if ul_tag:
            li_tags = ul_tag.find_all('li')
            for li in li_tags:
                a_tag = li.find('a')
                if a_tag:
                    district_name = a_tag.text.strip()
                    district_url = a_tag['href']
                    district_urls[district_name] = district_url
        else:
            logging.warning("No <ul> tag found with class 'list-districts-wards-paging'.")
    else:
        logging.error(f"Failed to retrieve page. Status code: {response.status_code}")

    return district_urls

def process_district(client, district_name, district_url, headers, spreadsheet_key):
    # Open Google Sheets and check if spreadsheet exists, otherwise create a new one
    try:
        spreadsheet = client.open_by_key(spreadsheet_key)
    except gspread.exceptions.APIError as api_error:
        logging.error(f"Error opening spreadsheet by key: {api_error}")
        return

    sheet_name = district_name.capitalize()
    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
        headers_row = ['STT', 'NAME', 'PHONE', 'GENDER', 'CODE', 'PRODUCT', 'TIME', 'LOCATION']
        sheet.insert_row(headers_row, 1)
        logging.info(f"Created new sheet '{sheet_name}' and inserted headers.")
    
    row_num = len(sheet.get_all_values()) + 1

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    
    page_num = 1
    while True:
        url = urljoin(base_url, f"{district_url}?page={page_num}") if page_num > 1 else urljoin(base_url, district_url)
        driver.get(url)

        try:
            close_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, 'dismiss-button'))
            )
            close_button.click()
        except Exception as e:
            logging.warning(f"Close button not found on page {page_num} for '{sheet_name}': {e}")

        response = driver.page_source
        bsObject = bs4.BeautifulSoup(response, 'html.parser')

        danhsach = []
        sections = bsObject.find_all('div', class_='main-content-paging')
        if sections:
            for section in sections:
                h2_tags = section.find_all('h2')
                a_tags = section.select('h2 > a')
                for h2, a_tag in zip(h2_tags, a_tags):
                    h2_text = h2.get_text(strip=True)
                    company_name = h2_text.split(maxsplit=1)[-1]
                    a_text = a_tag.get_text(strip=True)
                    href = a_tag.get('href', 'No href')
                    full_href_url = urljoin(url, href)

                    danhsach.append({'h2_text': 'CÔNG ' + company_name, 'a_text': a_text, 'href': href})
        else:
            logging.warning(f"No sections found on page {page_num} for '{sheet_name}'")

        existing_companies = set(sheet.col_values(2))

        for item in danhsach:
                    company_name = item['h2_text']
                    if company_name in existing_companies:
                        print(f"Company '{company_name}' already exists in '{sheet_name}'. Skipping...")
                        continue
                    
                    try:
                        # Fetch additional details from the item
                        href_url = item['href']
                        if href_url.startswith('http'):
                            response_href = requests.get(href_url, headers=headers)
                            response_href.encoding = 'utf-8'
                            bsObject_href = bs4.BeautifulSoup(response_href.text, 'html.parser')
                            
                            # Extract address
                            address = None
                            mst = None
                            address_element = bsObject_href.find('ul', class_='content-review-paging')
                            if address_element:
                                all_list = address_element.find_all('li')
                                if all_list:
                                    first_address_ele = all_list[0]
                                    mst_value = all_list[1]
                                    
                                    address = first_address_ele.get_text(strip=True)
                                    mst_ex = mst_value.get_text(strip=True)
                                    mst = mst_ex.replace("MST:", "").strip()


                            # Extract phone number text
                            phone = None
                            phone_element = bsObject_href.find('li', class_='phone-review-paging')
                            if phone_element:
                                phone_link = phone_element.find('a', href=True)
                                if phone_link:
                                    phone = phone_link.get_text(strip=True)
                            
                            # Initialize variables
                            representative = None
                            establishment_date = None

                            table_element = bsObject_href.find('table', class_='table-info')
                            if table_element:
                                rows = table_element.find_all('tr')
                                for row in rows:
                                    cells = row.find_all('td')
                                    if len(cells) == 2:
                                        cell_title = cells[0].get_text(strip=True)
                                        cell_content = cells[1].get_text(strip=True)
                                        
                                        if cell_title == 'Đại diện pháp luật:':
                                            strong_tag = cells[1].find('strong')
                                            if strong_tag:
                                                representative = strong_tag.get_text(strip=True)

                                        elif cell_title == 'Ngày thành lập:':
                                            try:
                                                date_obj = datetime.strptime(cell_content, '%Y-%m-%d %H:%M:%S')
                                                establishment_date = date_obj.strftime('%d/%m/%Y')
                                            except ValueError:
                                                establishment_date = cell_content

                            industry_name = None
                            # Locate the div with the class 'box-business-view'
                            business_view_div = bsObject_href.find('div', class_='box-business-view')

                            if business_view_div:
                                # Find the h3 tag with the title 'Ngành nghề chính'
                                title_tag = business_view_div.find('h3', class_='title-business-view')
                                if title_tag:
                                    # Extract the subsequent p tag which contains the details
                                    industry_name = title_tag.find_next('p').get_text(strip=True)
                                else:
                                    print("Title 'Ngành nghề chính' not found.")
                            else:
                                print("Div with class 'box-business-view' not found.")
                            # Insert data into Google Sheets (example, adjust as per your specific data structure)
                            row_data = [
                                row_num-1,  # STT
                                item['h2_text'],  # NAME
                                phone,  # PHONE
                                representative,  # GENDER
                                mst,  # CODE (adjust as needed)
                                industry_name,  # PRODUCT
                                establishment_date,  # TIME
                                address,  # LOCATION
                            ]
                            sheet.insert_row(row_data, row_num)
                            logging.info(f"Inserted row {row_num} for '{sheet_name}'.")
                            row_num += 1
                            time.sleep(2)

                    except Exception as e:
                        logging.warning(f"Failed to fetch details for '{item['h2_text']}' at '{href_url}': {e}")

        logging.info(f"Finished processing page {page_num} for '{sheet_name}'.")

        next_button = bsObject.find('a', class_='page-link', text='»')

        if next_button and 'href' in next_button.attrs:
            next_url = urljoin(base_url, next_button['href'])
            page_num += 1
            time.sleep(5)
        else:
            break
    
    driver.quit()


def open_google_sheet(sheet_key):
    sheet_url = f'https://docs.google.com/spreadsheets/d/{sheet_key}'
    webbrowser.open(sheet_url)

    
@celery.task(bind=True)
def scrape_provinces_and_districts(self, spreadsheet_name):
    client = authorize_google_sheets(creds)
    if not client:
        return

    spreadsheet_key = create_or_get_spreadsheet(client, spreadsheet_name)
    if not spreadsheet_key:
        return

    formatted_name = get_url_segment_from_name(spreadsheet_name)
    district_urls = get_district_urls(base_url, formatted_name)
    
    if district_urls:
        for district_name, district_url in district_urls.items():
            logging.info(f"Processing district: {district_name}, URL: {district_url}")
            process_district(client, district_name, district_url, headers, spreadsheet_key)
    else:
        logging.warning(f"No districts found for '{spreadsheet_name}'.")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start_scraping', methods=['POST'])
def start_scraping():
    # Extract data and start scraping process
    client = authorize_google_sheets(creds)
    if client is None:
        return "Failed to authorize Google Sheets", 500

    spreadsheet_name = "Thông tin doanh nghiệp " + request.form.get('spreadsheet_name')
    if not spreadsheet_name:
        return "Error: Spreadsheet Name is required."

    spreadsheet_key = create_or_get_spreadsheet(client, spreadsheet_name)
    if not spreadsheet_key:
        return "Error: Failed to create or access the spreadsheet."

    open_google_sheet(spreadsheet_key)

    formatted_name = get_url_segment_from_name(request.form.get('spreadsheet_name'))
    if not formatted_name:
        return "Error: Could not generate URL segment from spreadsheet name."

    # Get district URLs
    district_urls = get_district_urls(base_url, formatted_name)

    if not district_urls:
        return "Error: Failed to retrieve district URLs."

    # Start scraping process in separate threads
    threads = []

    for district_name, district_url in district_urls.items():
        thread = threading.Thread(target=process_district, args=(client, district_name, district_url, headers, spreadsheet_key))
        thread.start()
        threads.append(thread)
        time.sleep(5)

    for thread in threads:
        thread.join()

    # Pause before scraping again (adjust time as needed)
    time.sleep(3600)  # Scrapes every hour

    return f"Scraping process started for '{spreadsheet_name}'. Check Google Sheets for updates."

@app.route('/stop_scraping', methods=['POST'])
def stop_scraping_route():
    stop_threads()
    return 'Stopping scraping...'
if __name__ == '__main__':
    app.run(debug=True)