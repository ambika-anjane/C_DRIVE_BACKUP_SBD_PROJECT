import xml.etree.ElementTree as ET
from xml.dom import minidom

mytree = ET.parse('xml_sample.xml')
myroot = mytree.getroot()
'''
for x in myroot[0]:
    print(x.tag, x[2].attrib)

for x in myroot.findall('DESCRIPTION'):
    item =x.find('DESCRIPTION').text
    print(item)

dat=minidom.parse('xml_sample.xml')
print(dat)
for x in dat:
    print(x.firstChild.data)

import xml.etree.ElementTree as ET

tree = ET.parse('xml_sample.xml')
root = tree.getroot()

des = tree.findall('NAME')
print('Employee count:', len(des))
for ep in des:
    print('Name: ', ep.find('NAME').text)

import re

content = open("xml_sample.xml").read();

#get all departments
trans = re.findall('<SOURCE></SOURCE>', content)
for t in trans : 
   print(t)

from xml.dom.minidom import parse, Node
xmlTree = parse("xml_sample.xml")
#get all departments
for node1 in xmlTree.getElementsByTagName("FOLDER") :
    for node2 in node1.childNodes:
        if(node2.nodeType == Node.TEXT_NODE) :
            print(node2.data)
'''
import pandas as pd 

import xml.etree.ElementTree as etree

tree = etree.parse("xml_sample.xml")

root = tree.getroot()

columns = ["ITEM_ID", "PRICE"]

datatframe = pd.DataFrame(columns = columns)

for node in root: 

    name = node.attrib.get("ITEM_ID")
    
    age = node.find("PRICE").text 

    datatframe = datatframe.append(pd.Series([name, age], index = columns), ignore_index = True)

    print(dataframe)