# -*- coding: utf-8 -*-
import scrapy
from scrapy import Selector
from scrapy.crawler import CrawlerProcess



class Gram1bSpider(scrapy.Spider):
    name = 'gram1b'
    allowed_domains = ['meebhoomi.ap.gov.in']
    start_urls = ['https://meebhoomi.ap.gov.in/ROR.aspx']

    custom_settings = {
        'DOWNLOAD_DELAY' : 5,
        'COOKIES_ENABLED' : True,
        'CONCURRENT_REQUESTS' : 1 ,
        'AUTOTHROTTLE_ENABLED' : False,
        'ROBOTSTXT_OBEY' : False,
    }
    
    def parse(self, response):
        data = {
            'ContentPlaceHolder1_ToolkitScriptManager1_HiddenField': ';;AjaxControlToolkit, Version=3.0.20820.16598, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:te-IN:707835dd-fa4b-41d1-89e7-6df5d518ffb5:865923e8:91bd373d:4255254c:411fea1c:e7c87f07:bbfda34c:30a78ec5:5430d994',
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': response.css('input#__VIEWSTATE::attr(value)').extract_first(),
            '__VIEWSTATEGENERATOR': response.css('input#__VIEWSTATEGENERATOR::attr(value)').extract_first(),
            'ctl00$ddlLanguages': 'te-IN',
            'ctl00$ContentPlaceHolder1$ddlDist': '3',
            'ctl00$ContentPlaceHolder1$cddl_dist_ClientState': '3:::\u0C35\u0C3F\u0C36\u0C3E\u0C16\u0C2A\u0C1F\u0C4D\u0C28\u0C02',
            'ctl00$ContentPlaceHolder1$extender_ddldist_ClientState': '',
            'ctl00$ContentPlaceHolder1$ddlMandals': '06',
            'ctl00$ContentPlaceHolder1$CascadingDropDown1_ClientState': '06:::\u0C05\u0C28\u0C02\u0C24\u0C17\u0C3F\u0C30\u0C3F',
            'ctl00$ContentPlaceHolder1$ValidatorCalloutExtender1_ClientState': '',
            'ctl00$ContentPlaceHolder1$ddlVillageName': '306101',
            'ctl00$ContentPlaceHolder1$CascadingDropDown2_ClientState': '306101:::R.T.\u0C2A\u0C41\u0C30\u0C02',
            'ctl00$ContentPlaceHolder1$ValidatorCalloutExtender2_ClientState': '',
            # 'ctl00$ContentPlaceHolder1$txtCaptcha': '32194',
            'ctl00$ContentPlaceHolder1$RequiredFieldValidator1_ClientState': '',
            'ctl00$ContentPlaceHolder1$hdn_option': 'S',
            'ctl00$ContentPlaceHolder1$btn_go': '\u0C15\u0C4D\u0C32\u0C3F\u0C15\u0C4D \u0C1A\u0C47\u0C2F\u0C02\u0C21\u0C3F',
            'hiddenInputToUpdateATBuffer_CommonToolkitScripts': '0'
        }

        yield scrapy.FormRequest('https://meebhoomi.ap.gov.in/ROR.aspx', formdata=data, callback=self.parse_form1b)

    def parse_form1b(self, response):
        filename = '/tmp/x.html'
        with open(filename, 'w') as html_file:
            print('Writing [%s]' % filename)
            html_file.write(response.text)  # response.xpath('/html').extract_first())

        yield {'html' : response.text}


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(Gram1bSpider)
    process.start()

