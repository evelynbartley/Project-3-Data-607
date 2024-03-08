import json
import math
import os
import re
import urllib
import asyncio
from typing import Dict, List


from loguru import logger as log
from scrapfly import ScrapeApiResponse, ScrapeConfig, ScrapflyClient, ScrapflyScrapeError

SCRAPFLY = ScrapflyClient(key="scp-live-5bf66b5c008043dfb8af9fb52ef58ecf")

BASE_CONFIG = {
    "asp": True,
    "country": "US",
}

# Define the phrases to search for in job titles
search_phrases = [
    "Data Scientist",
    "Data Analyst",
    "Data Engineer",
]

# Compile regular expressions for faster matching
search_regex = re.compile("|".join(search_phrases), re.IGNORECASE)

# Define the output directory
output_dir = "/Users/AJStraumanScott/Documents/MSDS/Spring2024/DATA607/607_assignments/Projects/Project 3/indeed-scraper/output"


def parse_search_page(result):
    data = re.findall(r'window.mosaic.providerData\["mosaic-provider-jobcards"\]=(\{.+?\});', result.content)
    data = json.loads(data[0])
    return {
        "results": data["metaData"]["mosaicProviderJobCardsModel"]["results"],
        "meta": data["metaData"]["mosaicProviderJobCardsModel"]["tierSummaries"],
    }


async def scrape_search(url: str, max_results: int = 10000) -> List[Dict]:
    log.info(f"scraping search: {url}")
    result_first_page = await SCRAPFLY.async_scrape(ScrapeConfig(url, **BASE_CONFIG))
    data_first_page = parse_search_page(result_first_page)

    results = data_first_page["results"]
    total_results = sum(category["jobCount"] for category in data_first_page["meta"])
    if total_results > max_results:
        total_results = max_results

    print(f"scraping remaining {(total_results - 10) / 10} pages")
    other_pages = [
        ScrapeConfig(_add_url_parameter(url, start=offset), **BASE_CONFIG)
        for offset in range(10, total_results + 10, 10)
    ]
    log.info("found total pages {} search pages", math.ceil(total_results / 10))
    async for result in SCRAPFLY.concurrent_scrape(other_pages):
        if not isinstance(result, ScrapflyScrapeError):
            data = parse_search_page(result)
            results.extend(data["results"])
        else:
            log.error(f"failed to scrape {result.api_response.config['url']}, got: {result.message}")
    # Filter job listings based on search phrases
    filtered_results = [result for result in results if search_regex.search(result["title"])]
    # Save filtered results to a file
    output_file = os.path.join(output_dir, "search_results.json")
    with open(output_file, "w") as f:
        json.dump(filtered_results, f)
    return filtered_results


def parse_job_page(result: ScrapeApiResponse):
    data = re.findall(r"_initialData=(\{.+?\});", result.content)
    data = json.loads(data[0])
    data = data["jobInfoWrapperModel"]["jobInfoModel"]
    return {
        "description": data['sanitizedJobDescription'],
        **data["jobMetadataHeaderModel"],
        **(data["jobTagModel"] or {}),
        **data["jobInfoHeaderModel"],
    }


async def scrape_jobs(job_keys: List[str]):
    log.info(f"scraping {len(job_keys)} job listings")
    results = []
    urls = [
        f"https://www.indeed.com/viewjob?jk={job_key}" 
        for job_key in job_keys
    ]
    to_scrape = [ScrapeConfig(url, **BASE_CONFIG) for url in urls]
    async for result in SCRAPFLY.concurrent_scrape(to_scrape):
        results.append(parse_job_page(result))
    # Save job listings to a file
    output_file = os.path.join(output_dir, "job_listings.json")
    with open(output_file, "w") as f:
        json.dump(results, f)
    return results

# Define the URL of the Indeed job search page to scrape
search_url = "https://www.indeed.com/jobs?q=data+scientist&l=&from=searchOnHP&vjk=95dd5053f7ec8d7a"

# Define the output directory where the JSON file will be saved
output_dir = "/Users/AJStraumanScott/Documents/MSDS/Spring2024/DATA607/607_assignments/Projects/Project 3/indeed-scraper/output"

# Run the scrape_search function asynchronously

async def main():
    max_results = 5000  # Set the maximum number of results to scrape
    search_results = await scrape_search(search_url, max_results=max_results)
    print("Search results scraped successfully!")
    print("Number of records scraped:", len(search_results))
    print("Saving search results to JSON file...")
    # Save search results to a JSON file
    output_file = os.path.join(output_dir, "search_results.json")
    with open(output_file, "w") as f:
        json.dump(search_results, f)
    print("Search results saved to:", output_file)


# Create a task to run the main function within the event loop
async def run_main():
    await main()

# Run the main function within the event loop
asyncio.create_task(run_main())