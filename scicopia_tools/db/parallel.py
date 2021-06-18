from datetime import datetime
import logging
import multiprocessing
from typing import Dict, Iterable, List, Tuple

from dask.distributed import Client, LocalCluster, WorkerPlugin, get_worker
from pyArango.database import Database
from pyArango.theExceptions import UpdateError
from streamz import Stream
from tqdm import tqdm

from scicopia_tools.db.arango import setup

logger = logging.getLogger("scicopia_tools.db.parallel")

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


def worker_setup(feature, Analyzer, params, dask_worker):
    dask_worker.collection, dask_worker.connection, dask_worker.db = setup()
    dask_worker.feature = feature
    dask_worker.analyzer = Analyzer() if params is None else Analyzer(**params)


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
    def __init__(self, feature: str, analyzer, params=None):
        self.collection, self.connection, self.db = setup()
        self.feature = feature
        self.analyzer = analyzer
        self.params = params

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
        query = generate_query(self.collection.name, self.db, self.analyzer, batch_size)
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
        client.run(worker_setup, self.feature, self.analyzer, self.params)

        source = Stream()
        # Sink should be a no-op, since process_parallel saves into
        # a database
        source.scatter().map(process_parallel).gather().sink(lambda x: None)
        with tqdm(total=unfinished) as progress:
            for docs in split_batch(query, batch_size):
                source.emit(docs)
                progress.update(len(docs))
                # if not query.response["hasMore"]:
                #     break

    def main(self, batch_size: int) -> None:
        query = generate_query(self.collection.name, self.db, self.analyzer, batch_size)
        unfinished = (
            query.response["extra"]["stats"]["scannedFull"]
            - query.response["extra"]["stats"]["filtered"]
        )
        if unfinished == 0:
            logger.info("Nothing to be done. Task %s completed.", self.feature)
            return
        self.analyzer = self.analyzer() if self.params is None else self.analyzer(self.params)
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
