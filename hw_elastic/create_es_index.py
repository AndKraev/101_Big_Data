from elasticsearch import Elasticsearch


def create_es_index(
    es_client: Elasticsearch, index_name: str, settings: dict, delete: bool = False
) -> None:
    """Creates elasticsearch index with passed settings. Can delete old index if needed.

    :param es_client: elasticsearch client
    :param index_name: elasticsearch index name to create
    :param settings: elasticsearch settings for creation
    :param delete: If True, deletes existing index with given name. Default: False
    :return: no return
    """
    if delete and es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)
    es_client.indices.create(index=index_name, body=settings)


if __name__ == "__main__":
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "index.mapping.ignore_malformed": True,
        },
        "mappings": {
            "properties": {
                "date_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"}
            }
        },
    }
    create_es_index(Elasticsearch(["localhost"]), "hotels-3m", settings, delete=True)
