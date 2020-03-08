# -*- coding: utf-8 -*-
import scrapy
from scrapy.selector import Selector
from scrapy import Spider,Request
import configparser,json
from lianjia.items import LianjiaItem

class HouseSpider(scrapy.Spider):
    name = 'House'
    allowed_domains = ['lianjia.com']
    start_urls = 'https://%s.lianjia.com/ershoufang/pg%s/'
    def start_requests(self):
        # 读取配置文件，获取电影ID并生成列表
        conf = configparser.ConfigParser()
        domainList = []
        conf.read(self.settings.get('CONF'))
        temp = conf['LJ']
        if 'domain' in temp.keys():
            domainList = conf['LJ']['domain'].split(',')
        for d in domainList:
            self.domain = d
            headers = self.settings.get('DEFAULT_REQUEST_HEADERS')
            headers['Host'] = self.domain + '.lianjia.com'
            headers['Upgrade-Insecure-Requests'] = '1'
            for p in range(100):
                url = self.start_urls %(self.domain, str(p+1))
                yield Request(url=url, headers=headers, meta={'headers': headers}, callback=self.pageInfo)

    def pageInfo(self, response):
        sel = Selector(response)
        headers = response.meta['headers']
        houseURL = sel.xpath('//ul[@class="sellListContent"]/li')
        for u in houseURL:
            url = ''.join(u.xpath('.//div[@class="title"]//a//@href').extract()).strip()
            yield Request(url=url, headers=headers, callback=self.housePage)


    def housePage(self, response):
        houseInfo = {}
        villageInfo = {}
        sel = Selector(response)
        houseInfo['url'] = response.url
        houseInfo['house_hid'] = response.url.split('/')[-1].split('.')[0]
        houseInfo['price'] = ''.join(sel.xpath('//span[@class="total"]//text()').extract()) + ''.join(sel.xpath('//span[@class="unit"]//span//text()').extract())
        houseInfo['unitPrice'] = ''.join(sel.xpath('//span[@class="unitPriceValue"]//text()').extract())
        baseInfo = sel.xpath('//div[@class="base"]//li')
        for b in baseInfo:
            i = ''.join(b.xpath('.//text()').extract())
            if '房屋户型' in str(i):
                houseInfo['layout'] = str(i).replace('房屋户型','')
            if '所在楼层' in str(i):
                houseInfo['floor'] = str(i).replace('所在楼层','')
            if '建筑面积' in str(i):
                houseInfo['acreage'] = str(i).replace('建筑面积','')
            if '户型结构' in str(i):
                houseInfo['frame'] = str(i).replace('户型结构','')
            if '套内面积' in str(i):
                houseInfo['innerAcreage'] = str(i).replace('套内面积','')
            if '建筑类型' in str(i):
                houseInfo['style'] = str(i).replace('建筑类型','')
            if '房屋朝向' in str(i):
                houseInfo['face'] = str(i).replace('房屋朝向','')
            if '建筑结构' in str(i):
                houseInfo['structure'] = str(i).replace('建筑结构','')
            if '装修情况' in str(i):
                houseInfo['renovation'] = str(i).replace('装修情况','')
            if '梯户比例' in str(i):
                houseInfo['elevatorProportion'] = str(i).replace('梯户比例','')
            if '配备电梯' in str(i):
                houseInfo['elevator'] = str(i).replace('配备电梯','')
            if '产权年限' in str(i):
                houseInfo['propertyRight'] = str(i).replace('产权年限','')
        transaction = sel.xpath('//div[@class="transaction"]//li')
        for t in transaction:
            i = ''.join(t.xpath('.//text()').extract())
            if '挂牌时间' in str(i):
                houseInfo['listingTime'] = str(i).replace('挂牌时间','')
            if '交易权属' in str(i):
                houseInfo['transaction'] = str(i).replace('交易权属','')
            if '上次交易' in str(i):
                houseInfo['lastTrading'] = str(i).replace('上次交易','')
            if '房屋用途' in str(i):
                houseInfo['use'] = str(i).replace('房屋用途','')
            if '房屋年限' in str(i):
                houseInfo['ageLimit'] = str(i).replace('房屋年限','')
            if '产权所属' in str(i):
                houseInfo['ownership'] = str(i).replace('产权所属','')

        villageInfo['region_rid'] = ''.join(sel.xpath('//a[@class="info "]//@href').extract()).split('xiaoqu/')[1].replace('/','')
        houseInfo['region_rid'] = villageInfo['region_rid']
        villageURL = 'http://%s.lianjia.com/xiaoqu/%s/'%(self.domain, villageInfo['region_rid'])

        #构建请求头
        headers = self.settings.get('DEFAULT_REQUEST_HEADERS')
        headers['Host'] = self.domain + '.lianjia.com'
        headers['X-Requested-With'] = 'XMLHttpReqeust'
        headers['Referer'] = houseInfo['url']
        yield Request(url=villageURL,headers=headers,meta={'houseInfo':houseInfo,'villageInfo':villageInfo},callback=self.villagePage,dont_filter=True)

    def villagePage(self, response):
        houseInfo = response.meta['houseInfo']
        villageInfo = response.meta['villageInfo']
        sel = Selector(response)
        villageInfo['name'] = ''.join(sel.xpath('//h1[@class="detailTitle"]//text()').extract())
        info = sel.xpath('//div[@class="xiaoquInfo"]//span[@class="xiaoquInfoContent"]')
        villageInfo['buildYear'] = ''.join(info[0].xpath('.//text()').extract())
        villageInfo['type'] = ''.join(info[1].xpath('.//text()').extract())
        villageInfo['buildCost'] = ''.join(info[2].xpath('.//text()').extract())
        villageInfo['buildCompany'] = ''.join(info[3].xpath('.//text()').extract())
        villageInfo['developer'] = ''.join(info[4].xpath('.//text()').extract())
        villageInfo['buildCount'] = ''.join(info[5].xpath('.//text()').extract())
        villageInfo['houseCount'] = ''.join(info[6].xpath('.//text()').extract())
        villageInfo['nearby'] = ''.join(info[7].xpath('.//text()').extract())

        item = LianjiaItem()
        item['houseInfo'] = houseInfo
        item['villageInfo'] = villageInfo
        yield item
