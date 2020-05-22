import argparse
import logging
from typing import Dict, Tuple
from pyArango.collection import Collection
from pyArango.connection import Connection
from pyArango.theExceptions import UpdateError
from progress.bar import Bar

from config import read_config
from analyzers.AutoTagger import AutoTagger
from analyzers.TextSplitter import TextSplitter

from collections import deque
from itertools import zip_longest
import multiprocessing
from dask.distributed import Client, LocalCluster, get_worker
from streamz import Stream

features = {"auto_tag": AutoTagger, "split": TextSplitter}
BATCHSIZE = 100


def grouper(iterable, n: int):
    # https://docs.python.org/3/library/itertools.html#recipes
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=None)


def log(level: str, message: str = ""):
    if level is None:
        return
    if level == "critical":
        logging.critical(message)
    elif level == "error":
        logging.error(message)
    elif level == "warning":
        logging.warning(message)
    elif level == "info":
        logging.info(message)
    elif level == "debug":
        logging.debug(message)


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


def worker_setup(feature, dask_worker):
    dask_worker.collection, dask_worker.db, dask_worker.collectionName = setup()
    dask_worker.feature = feature
    dask_worker.analyzer = features[feature]()


def process_parallel(docs: Tuple[Dict[str, str]]):
    worker = get_worker()
    updates = deque(maxlen=len(docs))
    for doc in docs:
        if doc is None:
            continue
        data = doc["doc_section"]
        if not data is None:
            try:
                data = worker.analyzer.process(data)
                data["_key"] = doc["_key"]
                updates.append(data)
            except Exception as e:
                error = f"Exception occurred while processing document {doc['_key']}: {str(e)}"
                logging.error(error)
                return ("error", error)
        else:
            print(f"Document {doc['_key']} has None for {worker.feature}")
    try:
        worker.collection.bulkSave(updates, details=True, onDuplicate="update")
    except UpdateError as e:
        return ("error", e.message)
    finally:
        updates.clear()


def generate_query(collection: str, db, Analyzer):
    AQL = f"FOR x IN {collection} FILTER x.{Analyzer.field} == null RETURN x._key"
    return db.AQLQuery(AQL, rawResults=True, batchSize=BATCHSIZE, ttl=3600)


class DocTransformer:
    def __init__(self, feature: str):
        self.collection, self.db, self.collectionName = setup()
        self.feature = feature

    def parallel_main(self, parallel: int):
        if parallel <= 0:
            print("The number of processes has to be greater than zero!")
            return
        if parallel > multiprocessing.cpu_count():
            logging.warning(
                "Number of requested CPUs surpasses CPUs on machine: %d > %d.\nWill use all available CPUs.",
                parallel,
                multiprocessing.cpu_count(),
            )
            parallel = multiprocessing.cpu_count()
        cluster = LocalCluster(n_workers=parallel)
        client = Client(cluster)
        client.run(worker_setup, self.feature)

        source = Stream()
        source.scatter().map(process_parallel).buffer(parallel * 2).gather().sink(log)
        Analyzer = features[self.feature]
        AQL = f"FOR x IN {self.collectionName} FILTER x.{Analyzer.field} == null RETURN {{ '_key': x._key, 'doc_section': x.{Analyzer.doc_section} }}"
        query = self.db.AQLQuery(AQL, rawResults=True, batchSize=BATCHSIZE, ttl=3600)
        unfinished = (
            query.response["extra"]["stats"]["scannedFull"]
            - query.response["extra"]["stats"]["filtered"]
        )
        if unfinished == 0:
            logging.info("Nothing to be done. Task %s completed.", self.feature)
            return
        progress = Bar("entries", max=unfinished)
        for docs in grouper(query, BATCHSIZE):
            source.emit(docs)
            progress.next(len(docs))
        progress.finish()

    def main(self) -> None:
        Analyzer = features[self.feature]
        query = generate_query(self.collectionName, self.db, Analyzer)
        self.analyzer = Analyzer()
        progress = Bar("entries", max=self.collection.count())
        for key in query:
            # query contains the ENTIRE database split in parts by batchSize
            self.process_doc(key)
            progress.next()
        progress.finish()

    def process_doc(self, key: str):
        doc = self.collection[key]
        # for each database object add each entry of feature
        data = doc[self.analyzer.doc_section]
        if not data is None:
            try:
                data = self.analyzer.process(data)
                for field in data:
                    doc[field] = data[field]
                doc.patch()
            except Exception as e:
                logging.error(
                    "Exception occurred while processing document %s: %s", key, str(e)
                )


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Use feature to update Arango database"
    )
    PARSER.add_argument(
        "feature", choices=["auto_tag", "split"], help="Feature to use."
    )
    PARSER.add_argument(
        "-p",
        "--parallel",
        metavar="N",
        type=int,
        help="Distribute the computation on multiple cores",
    )
    ARGS = PARSER.parse_args()
    transformer = DocTransformer(ARGS.feature)
    if ARGS.parallel is None:
        transformer.main()
    else:
        transformer.parallel_main(ARGS.parallel)
