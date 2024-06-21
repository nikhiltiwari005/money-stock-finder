import requests
from bs4 import BeautifulSoup
import re
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_links(url, selector):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.select(selector)
        link_list = [link['href'] for link in links]
        modified_links = [re.sub(r'/[^/]+$', '/portfolio-holdings/\g<0>', link) for link in link_list]
        return modified_links
    else:
        print("Failed to retrieve page:", response.status_code)
        return None

def fetch_table_data(url, table_selector, column_headers):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.select_one(table_selector)
        if table:
            rows = table.find_all('tr')
            headers = rows[0].find_all('th')
            header_map = {header.get_text(strip=True): idx for idx, header in enumerate(headers)}
            
            required_headers = ["Stock Invested in", "Sector", "Value(Mn)", "% of Total Holdings", 
                                "1M Change", "1Y Highest Holding", "1Y Lowest Holding", "Quantity", 
                                "1M Change in Qty"]
            
            for required_header in required_headers:
                if required_header not in header_map:
                    header_map[required_header] = None

            rows_data = []
            for row in rows[1:]:
                cells = row.find_all('td')
                if cells:
                    row_data = {header: "" for header in column_headers}
                    row_data["Link"] = url
                    for header in required_headers:
                        idx = header_map[header]
                        if idx is not None and idx < len(cells):
                            row_data[header] = cells[idx].get_text(strip=True)
                    rows_data.append([row_data.get(column, "") for column in column_headers])
            return rows_data
        else:
            print("Table not found on page:", url)
            return None
    else:
        print("Failed to retrieve page:", response.status_code)
        return None

def process_link(link, table_selector, column_headers, csv_writer):
    if link == "":
        return

    link = link.replace("/nav", "")
    print("Fetching data from:", link)
    table_data = fetch_table_data(link, table_selector, column_headers)
    if table_data:
        for row in table_data:
            csv_writer.writerow(row)
    else:
        print("No table data found for:", link)

if __name__ == "__main__":
    from urllib.parse import urljoin

    parent_url = "https://www.moneycontrol.com/mutual-funds/performance-tracker/returns/small-cap-fund.html"
    parent_selector = "#dataTableId > tbody > tr > td > a"

    links = get_links(parent_url, parent_selector)

    if links:
        print("Fetching data from tables:")
        column_headers = ["Link", "Stock Invested in", "Sector", "Value(Mn)", "% of Total Holdings", 
                          "1M Change", "1Y Highest Holding", "1Y Lowest Holding", "Quantity", 
                          "1M Change in Qty"]

        with open("data.csv", mode='w', newline='', encoding='utf-8') as file:
            csv_writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(column_headers)

            # Using ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for link in links:
                    futures.append(executor.submit(process_link, link, "#equityCompleteHoldingTable", column_headers, csv_writer))
                
                # Wait for all tasks to complete
                for future in as_completed(futures):
                    try:
                        future.result()  # Retrieve the result of each task
                    except Exception as e:
                        print(f"Error processing link: {e}")
                    

    else:
        print("No links found.")
