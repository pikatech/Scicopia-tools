import argparse
import logging
import multiprocessing
from datetime import datetime
from typing import Dict, Iterable, List, Tuple

from dask.distributed import Client, LocalCluster, WorkerPlugin, get_worker
from pyArango.database import Database
from pyArango.theExceptions import UpdateError
from streamz import Stream
from tqdm import tqdm

from scicopia_tools.analyzers.AutoTagger import AutoTagger
from scicopia_tools.analyzers.TextSplitter import TextSplitter
from scicopia_tools.db.arango import setup

features = {"auto_tag": AutoTagger, "split": TextSplitter}


logger = logging.getLogger("scicopia_tools.arangofetch")

def split_batch(query: Iterable, n: int) -> List:
    """
    Split an iterable into batches of size n.
    An alternative implementation to Stream.partition().

    Parameters
    ----------
    query : Iterable
        Any collection or other data type supporting the
        iterator protocol
    n : int
        Batch size

    Yields
    -------
    List
        A part of the Iterable of length at most n
    """
    data = []
    for doc in query:
        data.append(doc)
        if len(data) == n:
            yield data.copy()
            data.clear()
    if data:
        yield data


def log(level: str, message: str = ""):
    if level is None:
        return
    if level == "critical":
        logger.critical(message)
    elif level == "error":
        logger.error(message)
    elif level == "warning":
        logger.warning(message)
    elif level == "info":
        logger.info(message)
    elif level == "debug":
        logger.debug(message)


def worker_setup(feature, dask_worker):
    dask_worker.collection, dask_worker.connection, dask_worker.db = setup()
    dask_worker.feature = feature
    dask_worker.analyzer = features[feature]()


class TeardownPlugin(WorkerPlugin):
    def teardown(self, worker):
        worker.analyzer.release_resources()
        worker.db.disconnectSession()


def process_parallel(docs: Tuple[Dict[str, str]]):
    worker = get_worker()
    updates = []
    for doc in docs:
        if doc is None:
            continue
        data = doc["doc_section"]
        if data:
            try:
                data = worker.analyzer.process(data)
                data["modified_at"] = round(datetime.now().timestamp())
                data["_key"] = doc["_key"]
                updates.append(data)
            except Exception as e:
                error = f"Exception occurred while processing document {doc['_key']}: {str(e)}"
                logger.error(error)
                return ("error", error)
        else:
            logger.debug(f"Document {doc['_key']} has None for {worker.feature}")
    try:
        worker.collection.bulkSave(updates, details=True, onDuplicate="update")
    except UpdateError as e:
        return ("error", e.message)
    finally:
        updates.clear()


def generate_query(collection: str, db: Database, Analyzer, batch_size: int):
    # TODO: change to work with multiple doc_sections
    if isinstance(Analyzer.doc_section, list):
        AQL = f"FOR x IN {collection} FILTER x.{Analyzer.field} == null AND {Analyzer.doc_section} ANY IN ATTRIBUTES(x) RETURN {{ '_key': x._key, 'doc_section': x }}"
        return db.AQLQuery(AQL, rawResults=True, batchSize=batch_size, ttl=3600)
    else:
        AQL = f"FOR x IN {collection} FILTER x.{Analyzer.field} == null and x.{Analyzer.doc_section} != null RETURN {{ '_key': x._key, 'doc_section': x.{Analyzer.doc_section} }}"
        return db.AQLQuery(AQL, rawResults=True, batchSize=batch_size, ttl=3600)


class DocTransformer:
    def __init__(self, feature: str):
        self.collection, self.connection, self.db = setup()
        self.feature = feature

    def teardown(self):
        self.connection.disconnectSession()

    def parallel_main(self, parallel: int, batch_size: int):
        if parallel <= 0:
            print("The number of processes has to be greater than zero!")
            return
        if parallel > multiprocessing.cpu_count():
            logger.warning(
                "Number of requested CPUs surpasses CPUs on machine: %d > %d.\nWill use all available CPUs.",
                parallel,
                multiprocessing.cpu_count(),
            )
            parallel = multiprocessing.cpu_count()

        # Leave early, if there is nothing to be done
        Analyzer = features[self.feature]
        query = generate_query(self.collection.name, self.db, Analyzer, batch_size)
        unfinished = (
            query.response["extra"]["stats"]["scannedFull"]
            - query.response["extra"]["stats"]["filtered"]
        )
        if unfinished == 0:
            logger.info("Nothing to be done. Task %s completed.", self.feature)
            return

        cluster = LocalCluster(n_workers=parallel)
        teardown = TeardownPlugin()
        client = Client(cluster)
        client.register_worker_plugin(teardown)
        client.run(worker_setup, self.feature)

        source = Stream()
        source.scatter().map(process_parallel).gather().sink(log)
        with tqdm(total=unfinished) as progress:
            for docs in split_batch(query, batch_size):
                source.emit(docs)
                progress.update(len(docs))
                # if not query.response["hasMore"]:
                #     break

    def main(self, batch_size: int) -> None:
        Analyzer = features[self.feature]
        query = generate_query(self.collection.name, self.db, Analyzer)
        unfinished = (
            query.response["extra"]["stats"]["scannedFull"]
            - query.response["extra"]["stats"]["filtered"]
        )
        if unfinished == 0:
            logger.info("Nothing to be done. Task %s completed.", self.feature)
            return
        self.analyzer = Analyzer()
        with tqdm(total=unfinished) as progress:
            for docs in split_batch(query, batch_size):
                self.process_doc(docs)
                progress.update(len(docs))

    def process_doc(self, docs: Tuple[Dict[str, str]]):
        updates = []
        for doc in docs:
            if doc is None:
                continue
            data = doc["doc_section"]
            if data:
                try:
                    data = self.analyzer.process(data)
                    data["modified_at"] = round(datetime.now().timestamp())
                    data["_key"] = doc["_key"]
                    updates.append(data)
                except Exception as e:
                    error = f"Exception occurred while processing document {doc['_key']}: {str(e)}"
                    logger.error(error)
                    return ("error", error)
            else:
                logger.debug(f"Document {doc['_key']} has None for {self.feature}")
        try:
            self.collection.bulkSave(updates, details=True, onDuplicate="update")
        except UpdateError as e:
            return ("error", e.message)
        finally:
            updates.clear()


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
    PARSER.add_argument(
        "--batch", type=int, help="Batch size of bulk import", default=100
    )
    ARGS = PARSER.parse_args()
    transformer = DocTransformer(ARGS.feature)
    if ARGS.parallel is None:
        transformer.main(ARGS.batch)
    else:
        transformer.parallel_main(ARGS.parallel, ARGS.batch)
    # transformer.teardown()
