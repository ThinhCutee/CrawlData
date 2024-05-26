import requests
from bs4 import BeautifulSoup
import json
import csv
import os
from scrapy.crawler import CrawlerProcess

api_link = 'https://fptjobs.com/Jobs/GetJobComponent'
form_data = {"PageSize": "20"}
response = requests.post(api_link, data=form_data)
soup = BeautifulSoup(response.text, 'html.parser')

# find all job href in a tag with class = "job__href"
job_hrefs = soup.find_all('a', class_='job__href')
root_link = "https://fptjobs.com"

list_href = [root_link + job_href['href'] for job_href in job_hrefs]

directory = '/opt/airflow/data/crawl_data'
if not os.path.exists(directory):
    os.makedirs(directory)

for link in list_href:
    response = requests.get(link)
    soup = BeautifulSoup(response.text, 'html.parser')
    job_name = soup.find('h3').text
    address = soup.select('div.main--content--head > span')[0].text.strip()
    salary = soup.select('div.main--content--subhead > ul:nth-child(2) > li:nth-child(1) > strong')[0].text.strip()
    job_detail_list = soup.select('div.main--content--body > ul:nth-child(2)')
    
    for job_details in job_detail_list:
        job_details = job_details.text.strip()

    print(job_name)
    print(address)
    print(link)
    print(salary)
    print(job_details)
    print('---------------------------------')

    # Tạo cột
    item = {
        'job_name': job_name,
        'address': address,
        'job_link': link,
        'salary': salary,
        'job_details': job_details
    }

    # Ghi dữ liệu vào tệp JSON
    json_file_path = os.path.join(directory, 'nguyenducthinh_fpt.json')
    with open(json_file_path, 'a', encoding='utf-8') as file:
        line = json.dumps(item, ensure_ascii=False) + '\n'
        file.write(line)

    # Ghi dữ liệu vào tệp CSV
    csv_file_path = os.path.join(directory, 'nguyenducthinh_fpt.csv')
    with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['job_name', 'address', 'job_link', 'salary', 'job_details']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if os.stat(csv_file_path).st_size == 0:
            writer.writeheader()
        writer.writerow(item)
        
process = CrawlerProcess(settings={
    "CONCURRENT_REQUESTS": 3,
    "DOWNLOAD_TIMEOUT": 60,
    "RETRY_TIMES": 5,
    "ROBOTSTXT_OBEY": False,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
})
process.start()