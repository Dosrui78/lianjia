# -*- coding: utf-8 -*-
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from scrapy.utils.project import get_project_settings
settings = get_project_settings()

#定义映射表类
Base = declarative_base()

#小区信息表
class villageInfo(Base):
    __tablename__ = 'villageInfo'
    id = Column(Integer(), primary_key=True)
    region_rid = Column(String(100), comment='小区编号')
    buildYear = Column(String(100), comment='建筑年代')
    type = Column(String(100), comment='建筑类型')
    buildCost = Column(String(100), comment='物业费用')
    buildCompany = Column(String(100), comment='物业公司')
    developer = Column(String(100), comment='开发商')
    buildCount = Column(String(100), comment='楼栋总数')
    houseCount = Column(String(100), comment='房屋总数')
    nearby = Column(String(100), comment='附近门店')
    log_date = Column(DateTime(), default=datetime.now, onupdate=datetime.now, comment='记录日期')

class houseInfo(Base):
    __tablename__ = 'houseInfo'
    id = Column(Integer(), primary_key=True)
    house_hid = Column(String(100), comment='房屋编号')
    layout = Column(String(100), comment='户型')
    floor = Column(String(100), comment='楼层')
    acreage = Column(String(100), comment='建筑面积')
    frame = Column(String(100), comment='户型结构')
    innerAcreage = Column(String(100), comment='套内面积')
    style = Column(String(100), comment='建筑类型')
    face = Column(String(100), comment='房屋朝向')
    structure = Column(String(100), comment='建筑结构')
    renovation = Column(String(100), comment='装修情况')
    elevatorProportion = Column(String(100), comment='梯户比例')
    elevator = Column(String(100), comment='配备电梯')
    propertyRight = Column(String(100), comment='产权年限')
    price = Column(String(100), comment='售价')
    unitPrice = Column(String(100), comment='每平方售价')
    listingTime = Column(String(100), comment='挂牌时间')
    transaction = Column(String(100), comment='交易权属')
    lastTrading = Column(String(100), comment='上次交易')
    use = Column(String(100), comment='房屋用途')
    ageLimit = Column(String(100), comment='房屋年限')
    houseProperty = Column(String(100), comment='房屋年限')
    ownership = Column(String(100), comment='产权所属')
    region_rid = Column(String(100), comment='房屋编号')
    log_date = Column(DateTime(), default=datetime.now, onupdate=datetime.now, comment='记录日期')

class LianjiaPipeline(object):
    def __init__(self):
        #初始化，连接数据库
        connection = settings['MYSQL_CONNECTION'] 
        engine = create_engine(connection,echo=False,pool_size=2000)
        DBSession = sessionmaker(bind=engine)
        self.SQLsession = DBSession()
        #创建数据表
        Base.metadata.create_all(engine)

    def house_db(self, info):
        house_hid = info['house_hid']            
        #判断是否已存在记录
        temp = self.SQLsession.query(houseInfo).filter_by(house_hid=house_hid).first()    
        if temp:
            temp.layout = info.get('layout','')
            temp.floor = info.get('floor','')
            temp.acreage = info.get('acreage','')
            temp.frame = info.get('frame','')
            temp.innerAcreage = info.get('innerAcreage','')
            temp.style = info.get('style','')
            temp.face = info.get('face','')
            temp.structure = info.get('structure','')
            temp.renovation = info.get('renovation','')
            temp.elevatorProportion = info.get('elevatorProportion','')
            temp.elevator = info.get('elevator','')
            temp.propertyRight = info.get('propertyRight','')
            temp.price = info.get('price','')
            temp.unitPrice = info.get('unitPrice','')
            temp.listingTime = info.get('listingTime','')
            temp.transaction = info.get('transaction','')
            temp.lastTradin = info.get('lastTradin','')
            temp.use = info.get('use','')
            temp.ageLimit = info.get('ageLimit','')
            temp.houseProperty = info.get('houseProperty','')
            temp.ownership = info.get('ownership')
            temp.region_rid = info.get('region_rid')
        else:
            inset_data = houseInfo(
                house_hid=info.get('house_hid',''),
                layout = info.get('layout',''),
                floor = info.get('floor',''),
                acreage = info.get('acreage',''),
                frame = info.get('frame',''),
                innerAcreage = info.get('innerAcreage',''),
                style = info.get('style',''),
                face = info.get('face',''),
                structure = info.get('structure',''),
                renovation = info.get('renovation',''),
                elevatorProportion = info.get('elevatorProportion',''),
                elevator = info.get('elevator',''),
                propertyRight = info.get('propertyRight',''),
                listingTime = info.get('listingTime',''),
                transaction = info.get('transaction',''),
                lastTrading = info.get('lastTrading',''),
                use = info.get('use',''),
                ageLimit = info.get('ageLimit',''),
                houseProperty = info.get('houseProperty',''),
                ownership = info.get('ownership'),
                region_rid = info.get('region_rid','')
                    )
            self.SQLsession.add(inset_data)
        self.SQLsession.commit()

    #写入小区信息
    def village_db(self, info):
        region_rid = info['region_rid']
        temp = self.SQLsession.query(villageInfo).filter_by(region_rid=region_rid).first()
        if temp:
            temp.name = info.get('name')
            temp.region_rid = info.get('region_rid', '')
            temp.buildYear = info.get('buildYear', '')
            temp.type = info.get('type', '')
            temp.buildCost = info.get('buildCost', '')
            temp.buildCompany = info.get('buildCompany', '')
            temp.developer = info.get('developer', '')
            temp.buildCount = info.get('buildCount', '')
            temp.houseCount = info.get('houseCount', '')
            temp.nearby = info.get('nearby','')
        else:
            inset_data = villageInfo(
                region_rid = info.get('region_rid', ''),
                buildYear = info.get('buildYear', ''),
                type = info.get('type', ''),
                buildCost = info.get('buildCost', ''),
                buildCompany = info.get('buildCompany', ''),
                developer = info.get('developer', ''),
                buildCount = info.get('buildCount', ''),
                houseCount = info.get('houseCount', ''),
                nearby = info.get('nearby','')
                )
            self.SQLsession.add(inset_data)
        self.SQLsession.commit()

    def process_item(self, item, spider):
        self.house_db(item['houseInfo'])
        self.village_db(item['villageInfo'])
        return item
