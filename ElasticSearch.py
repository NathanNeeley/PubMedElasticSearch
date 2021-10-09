from elasticsearch import Elasticsearch

def SearchQuery():
    user = 'elastic'
    password = 'iYYX96TPlAJ000UJ0vqa'
    index_name = 'pubmed2021nneeley'
    es = Elasticsearch(hosts=['10.80.34.86:9200'], http_auth=(user, password))
    query = es.search(
    index=index_name,
    body={
        "_source": [ "PMID", "Title", "Uploader", "_score" ],
        "size": 3,
        "query": {
            "bool": {
                "must": [
                    {"match_phrase": { "Title": "patients" }},
                    {"match": { "Uploader": "Nathan Neeley" }}
                ]
            }
        }
    })

    return query
 
def PrintResults():
    results = SearchQuery()
    print("{")
    print("   \"took\" : " + str(results['took']))
    print("   \"timed_out\" : " + str(results['timed_out']))
    print("   \"_shards\" : {")
    print("     \"total\" : " + str(results['_shards']['total']) + ",")
    print("     \"successful\" : " + str(results['_shards']['successful']) + ",")
    print("     \"skipped\" : " + str(results['_shards']['skipped']) + ",")
    print("     \"failed\" : " + str(results['_shards']['failed']))
    print("   },")
    print("   \"hits\" : {")
    print("     \"total\" : {")
    print("       \"value\" : " + str(results['hits']['total']['value']) + ",")
    print("       \"relation\" : \"" + str(results['hits']['total']['relation']) + "\"")
    print("     },")
    print("     \"max_score\" : " + str(results['hits']['max_score']) + ",")
    print("     \"hits\" : [")
    
    count = 0
    while count < 3:
        print("        {")
        for key in results['hits']['hits'][count].keys():
            if key == "_index":
                print("          \"_index\" : \"" + str(results['hits']['hits'][count][key]) + "\",")
            elif key == "_type":
                print("          \"_type\" : \"" + str(results['hits']['hits'][count][key]) + "\",")
            elif key == "_id":
                print("          \"_id\" : \"" + str(results['hits']['hits'][count]['_source']['PMID']).strip('[').strip(']').strip('\"').strip('\'') + "\",")
            elif key == "_score":
                print("          \"_score\" : " + str(results['hits']['hits'][count][key]) + ",")
            elif key == "_source":
                print("          \"_source\" : {")
                for grandKey in results['hits']['hits'][count][key]:
                    if grandKey == "Title":
                        print("            \"Title\" : \"" + str(results['hits']['hits'][count][key][grandKey]).replace('[', '').replace('.', '').replace(']', '').strip('\'').strip('\"') + "\",")
                    elif grandKey == "PMID":
                        print("            \"PMID\" : " + str(results['hits']['hits'][count][key][grandKey]).replace('[', '').replace('.', '').replace(']', '').replace('\'', '\"') + ",")
                    elif grandKey == "Uploader":
                        print("            \"Uploader\" : " + str(results['hits']['hits'][count][key][grandKey]).replace('[', '').replace('.', '').replace(']', '').replace('\'', '\"'))
                print("          }")
                if count < 2:
                    print("        },")
                else:
                    print("        }")
        
        count += 1
    print("      ]")
    print("    }")
    print("  }")

if __name__ == '__main__':
    PrintResults()
        
        