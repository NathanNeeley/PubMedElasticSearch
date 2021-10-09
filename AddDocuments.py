# September 2021
# Python program that creates an ElasticSearch index mappings, parses text from an xml.gz file into a dictionary, and then adds dictionary to ElasticSearch index. A search query is then run on index and printed out. The statistics of the journal titles is visualized in a Treemap in Kibana to show frequencies.
from lxml import etree
from elasticsearch import Elasticsearch, helpers
from collections import deque
import collections

def genmapping():
    mapping = {"mappings":{
               "properties":{
               "Abstract":{
                    "type":"keyword"},
               "Authors":{
                    "type":"keyword"},
               "Journal":{
                    "type":"keyword"},
               "Keywords":{
                    "type":"keyword"},
               "MeshIDs":{
                    "type":"keyword"},
               "PMID":{
                    "type":"text"},
               "PublishDate":{
                    "type":"date"},
               "Title":{
                    "type":"text"},
               "Uploader":{
                    "type":"keyword"}
               }}}
                   
    return mapping
    
def gendata(es):
    file_contents = etree.parse('pubmed21n0019.xml.gz')
    entry = collections.defaultdict(list)
    FoundTag = {'MeshIDs': True, 'Title': True, 'PublishDate': True, 'Authors': True, 'Keywords': True, 'Abstract': True, 'Journal': True}
    for element in file_contents.iter():
        if element.tag == 'PMID': # PMID
            for tag in FoundTag.keys(): # Fill dict value with empty string if it is not found
                if FoundTag[tag] == False and tag == 'PublishDate':
                    entry[tag].append('0')
                elif FoundTag[tag] == False:
                    entry[tag].append('')
                FoundTag[tag] = False
            if len(entry) != 0:
                yield entry
            entry.clear()
            entry['PMID'].append(element.text)
            entry['Uploader'].append('Nathan Neeley')
        elif element.tag == 'DateCompleted': # Article Date Completed
            DateCompleted = ''
            children = element.getchildren()
            for child in children:
                if child.tag == 'Year':
                    DateCompleted = child.text
                elif child.tag == 'Month':
                    DateCompleted = DateCompleted + '-' + child.text
                elif child.tag == 'Day':
                    DateCompleted = DateCompleted + '-' + child.text
            entry['PublishDate'].append(DateCompleted)
            FoundTag['PublishDate'] = True
        elif element.tag == 'MeshHeadingList': # Mesh IDs
            MeshIDsList = list()
            children = element.getchildren()
            for child in children:
                for grand in child:
                    if grand.tag == 'DescriptorName':
                        MeshIDsList.append(grand.attrib['UI'])
            entry['MeshIDs'].append(MeshIDsList)
            FoundTag['MeshIDs'] = True
        elif element.tag == 'ArticleTitle': # Title of Article
            entry['Title'].append(element.text)
            FoundTag['Title'] = True
        elif element.tag == 'AuthorList': # Author List
            AuthorList = list()
            children = element.getchildren()
            for child in children:
                Author = ''
                for grand in child:
                    if grand.tag == 'LastName':
                        Author = grand.text
                    elif grand.tag == 'ForeName':
                        Author = grand.text + ' ' + Author 
                AuthorList.append(Author)
            entry['Authors'].append(AuthorList)
            FoundTag['Authors'] = True
        elif element.tag == 'Abstract': # Abstract Text
            AbstractTextList = list()
            children = element.getchildren()
            for child in children:
                if child.tag == 'AbstractText':
                    AbstractTextList.append(child.text)
            entry['Abstract'].append(AbstractTextList)
            FoundTag['Abstract'] = True
        elif element.tag == 'KeywordList': # Keyword List
            KeywordList = list()
            children = element.getchildren()
            for child in children:
                if child.tag == 'Keyword':
                    KeywordList.append(child.text)
            entry['Keywords'].append(KeywordList)
            FoundTag['Keywords'] = True
        elif element.tag == 'Title': # Journal Title
            entry['Journal'].append(element.text)
            FoundTag['Journal'] = True
    
    for tag in FoundTag.keys(): # Done on last PMID iteration
        if FoundTag[tag] == False and tag == 'PublishDate':
            entry[tag].append('0')
        elif FoundTag[tag] == False:
            entry[tag].append('')
        FoundTag[tag] = False
    if len(entry) != 0:
        yield entry
    entry.clear()
    

if __name__ == '__main__':
    user = 'elastic'
    password = 'iYYX96TPlAJ000UJ0vqa'
    index_name = 'pubmed2021nneeley'
    mapping = genmapping()
    es = Elasticsearch(hosts=['10.80.34.86:9200'], timeout=100, http_auth=(user, password))
    es.indices.delete(index=index_name, ignore=[400, 404])
    es.indices.create(index=index_name, body=mapping)
    deque(helpers.parallel_bulk(es, gendata(es), index=index_name), maxlen=0)
    es.indices.refresh()