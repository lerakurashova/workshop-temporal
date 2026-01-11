# Query / Search
# Return the top `size` (5) most relevant documents, ranked by Elasticsearch relevance (_score).
#
# How Elasticsearch chooses the top 5:
# - Analyze the query text ("how to understand dogs"):
#     • remove stop words (e.g. "how", "to")
#     • apply stemming (e.g. "understanding" → "understand", "dogs" → "dog")
# - Search across fields:
#     • title (boosted x3, so matches here are more important)
#     • subtitles
# - Compute a relevance score (_score) for each matching document
# - Sort documents by _score (highest first)
# - Return only the top 20 documents

def search_videos(query: str, size: int = 5):
    body = {
        "size": size,
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["title^3", "subtitles"], #if query matches the title it's 3 times more important 
                "type": "best_fields",
                "analyzer": "english_with_stop_and_stem" 
            }
        },
        "highlight": {
            "pre_tags": ["*"],
            "post_tags": ["*"],
            "fields": {
                "title": {
                    "fragment_size": 150,
                    "number_of_fragments": 1
                },
                "subtitles": {
                    "fragment_size": 150,
                    "number_of_fragments": 1 
                }
            }
        } #Extracts and returns small text snippets where the query matched
    }

    response = es.search(index="podcasts", body=body)
    hits = response.body['hits']['hits']

    results = []
    for hit in hits:
        highlight = hit['highlight']
        highlight['video_id'] = hit['_id']
        results.append(highlight)

    return results