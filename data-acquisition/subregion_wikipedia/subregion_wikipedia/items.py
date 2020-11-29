# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SubregionWikipediaItem(scrapy.Item):
    nome_pais = scrapy.Field()
    nome_continente = scrapy.Field()
    nome_regiao = scrapy.Field()
