import scrapy


class GenspiderSpider(scrapy.Spider):
    name = "genspider"
    allowed_domains = ["realestate.co.nz"]
    start_urls = ["https://realestate.co.nz"]

    def parse(self, response):
        pass
