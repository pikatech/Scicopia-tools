
import argparse
import logging
from pyArango.connection import Connection
from pyArango.theExceptions import DocumentNotFoundError, CreationError
from config import read_config
from progress.bar import Bar

from scicopia_tools.analyzers.auto_tag import auto_tag


def setup():
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
        logging.error(f'Database {config["database"]} not found.')
        
    if db.hasCollection(config["collection"]):
        collection = db[config["collection"]]
    else:
        logging.error(f'Collection {config["collection"]} not found.')

    return collection, db, config["collection"]



def main(feature):
    collection , db, collectionName = setup()
    featuredict = {'auto_tag':auto_tag}
    datadict = {'auto_tag':"abstract"}
    aql = f"FOR x IN {collectionName}  RETURN x._key"
    query = db.AQLQuery(aql, rawResults=True, batchSize=10)
    # cursor error with higher batchSize, reason not found
    bar = Bar("entries", max=collection.count())
    for key in query:
        # query contains the ENTIRE database split in parts by batchSize
        doc = collection[key]
        # for each databaseobject add each entry of feature
        data = doc[datadict[feature]]
        if data is not None:
            data = featuredict[feature](data)
            for field in data:
                doc[field] = data[field]
            doc.save()
        bar.next()
    bar.finish()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Use feature to update Arangodatabase')
    parser.add_argument('feature', choices=['auto_tag'], help='Feature to use.')
    args = parser.parse_args()
    main(args.feature)