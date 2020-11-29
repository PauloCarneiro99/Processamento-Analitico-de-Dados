import json

from scrapy import Request, Spider
from subregion_wikipedia.items import SubregionWikipediaItem as Item

# scrapy crawl wikipedia -L INFO -o output.csv
class WikipediaBaseSpider(Spider):

    name = "wikipedia"
    base_url = "https://en.wikipedia.org"

    custom_settings = {"ITEM_PIPELINES": {"subregion_wikipedia.pipelines.PersistencePipeline": 400}}

    def start_requests(self):
        yield Request("https://en.wikipedia.org/wiki/United_Nations_geoscheme")

    def parse(self, response):
        for continent, content_xpath in zip(
            response.xpath("//h2/span[@class='mw-headline']/a/text()").getall(),
            response.xpath("//div[contains(@class,'hatnote')]/following-sibling::ul"),
        ):
            links = list(
                map(lambda x: f"{self.base_url}{x}", content_xpath.xpath("./li/a/@href").getall())
            )
            for url in links:
                yield Request(
                    url, callback=self.parse_sub_region, cb_kwargs=dict(continent=continent)
                )

    def parse_sub_region(self, response, continent):
        for text, title in zip(
            response.xpath("//table[@class!='infobox']//a[@title]/text()").getall(),
            response.xpath("//table[@class!='infobox']//a[@title]/@title").getall(),
        ):
            if text == title:
                item = Item()
                item["nome_pais"] = text
                item["nome_continente"] = continent
                item["nome_regiao"] = (
                    response.xpath("//h1[@id='firstHeading']/text()").get().strip()
                )
                yield item

