import argparse
import logging
from typing import List, Tuple
from pyArango.collection import Collection
from pyArango.connection import Connection
from progress.bar import Bar

from config import read_config
from analyzers.AutoTagger import AutoTagger
from analyzers.TextSplitter import TextSplitter

from itertools import zip_longest
import multiprocessing
from dask.distributed import Client, LocalCluster
from streamz import Stream

features = {"auto_tag": AutoTagger, "split": TextSplitter}
section = {"auto_tag": "abstract", "split": "abstract"}
BATCHSIZE = 100


def grouper(iterable, n: int):
    # https://docs.python.org/3/library/itertools.html#recipes
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=None)


class DocTransformer:
    def __init__(self, feature: str):
        self.collection, self.db, self.collectionName = self.setup()
        self.AQL = f"FOR x IN {self.collectionName} RETURN x._key"
        self.feature = feature
        self.analyzer = features[feature]()

    def setup(self) -> Tuple[Collection, Connection, str]:
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
        source = Stream()
        source.scatter().map(self.process_parallel).gather()
        query = self.db.AQLQuery(
            self.AQL, rawResults=True, batchSize=BATCHSIZE, ttl=3600
        )
        progress = Bar("entries", max=self.collection.count())
        for keys in grouper(query, BATCHSIZE):
            # query contains the ENTIRE database split in parts by batchSize
            source.emit(keys)
            progress.next()
        progress.finish()

    def process_parallel(self, keys: List[str]):
        self.collection, self.db, self.collectionName = self.setup()
        for key in keys:
            doc = self.collection[key]
            # for each database object add each entry of feature
            data = doc[section[self.feature]]
            if not data is None:
                try:
                    data = self.analyzer.process(data)
                    for field in data:
                        doc[field] = data[field]
                    doc.patch()
                except Exception as e:
                    logging.error(
                        "Exception occurred while processing document %s: %s",
                        key,
                        str(e),
                    )
            else:
                print(f"Document {key} has None for {self.feature}")

    def main(self) -> None:
        query = self.db.AQLQuery(
            self.AQL, rawResults=True, batchSize=BATCHSIZE, ttl=3600
        )
        progress = Bar("entries", max=self.collection.count())
        for key in query:
            # query contains the ENTIRE database split in parts by batchSize
            self.process_doc(key)
            progress.next()
        progress.finish()

    def process_doc(self, key: str):
        doc = self.collection[key]
        # for each database object add each entry of feature
        data = doc[section[self.feature]]
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
