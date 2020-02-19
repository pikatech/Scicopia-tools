
from pyArango.connection import Connection
from pyArango.theExceptions import DocumentNotFoundError, CreationError
from config import read_config

from scicopia_tools.analyzers.auto_tag import auto_tag


def setup():
    config = read_config()
    arangoconn = Connection(username = config.username, password = config.password)
    if arangoconn.hasDatabase(config.database):
        db = arangoconn[config.database]
    else:
        db = arangoconn.createDatabase(name = config.database) 
    if db.hasCollection(config.collection):
        collection = db[config.collection]
    else:
        collection = db.createCollection(name = config.collection)
    return collection, db, config.collection



def main(feature):
    collection , db, coll = setup()
    featuredict = {'auto_tag':auto_tag}
    datadict = {'auto_tag':"abstract"}
    aql = "FOR x IN %s  RETURN x._key" % (coll)
    query = db.AQLQuery(aql, rawResults=True, batchSize=2)
    for key in query:
        print(query)
        # query contains the ENTIRE database split in parts by batchSize
        doc = collection[key]
        # for each databaseobject add each entry of feature
        data = doc[datadict[feature]]
        if data is not None:
            data = featuredict[feature](data)
            for field in data:
                doc[field] = data[field]
            doc.save()

if __name__ == '__main__':
    main('auto_tag')