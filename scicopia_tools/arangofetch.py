import argparse
import logging
from typing import Tuple
from pyArango.collection import Collection
from pyArango.connection import Connection
from progress.bar import Bar

from config import read_config
from analyzers.AutoTagger import AutoTagger
from analyzers.TextSplitter import TextSplitter


def setup() -> Tuple[Collection, Connection, str]:
    config = read_config()
    if "arango_url" in config:
        arangoconn = Connection(
            arangoURL=config["arango_url"],
            username=config["username"],
            password=config["password"],
        )
    else:
        arangoconn = Connection(
            username=config["username"], password=config["password"]
        )

    if arangoconn.hasDatabase(config["database"]):
        db = arangoconn[config["database"]]
    else:
        logging.error("Database %s not found.", config["database"])

    if db.hasCollection(config["collection"]):
        collection = db[config["collection"]]
    else:
        logging.error("Collection %s not found.", config["collection"])

    return collection, db, config["collection"]


def main(feature: str) -> None:
    collection, db, collectionName = setup()
    features = {"auto_tag": AutoTagger, 'split': TextSplitter}
    section = {"auto_tag": "abstract", 'split': "abstract"}
    aql = f"FOR x IN {collectionName} RETURN x._key"
    query = db.AQLQuery(aql, rawResults=True, batchSize=100, ttl=3600)
    # cursor error with higher batchSize, reason not found
    progress = Bar("entries", max=collection.count())
    analyzer = features[feature]()
    for key in query:
        # query contains the ENTIRE database split in parts by batchSize
        doc = collection[key]
        # for each databaseobject add each entry of feature
        data = doc[section[feature]]
        if not data is None:
            try:
                data = analyzer.process(data)
                for field in data:
                    doc[field] = data[field]
                doc.save()
            except Exception as e:
                logging.error("Exception occurred while processing document %s: %s", key, str(e))
        progress.next()
    progress.finish()


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Use feature to update Arango database")
    PARSER.add_argument("feature", choices=["auto_tag", 'split'], help="Feature to use.")
    ARGS = PARSER.parse_args()
    main(ARGS.feature)
