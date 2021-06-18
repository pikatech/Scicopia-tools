import argparse
import logging

from scicopia_tools.analyzers.AutoTagger import AutoTagger
from scicopia_tools.analyzers.TextSplitter import TextSplitter
from scicopia_tools.db.arango import setup
from scicopia_tools.db.parallel import DocTransformer

features = {"auto_tags": AutoTagger, "split": TextSplitter}
logger = logging.getLogger("scicopia_tools.arangofetch")

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="Use feature to update Arango database"
    )
    PARSER.add_argument(
        "feature", choices=list(features.keys()), help="Feature to use."
    )
    PARSER.add_argument(
        "-p",
        "--parallel",
        metavar="N",
        type=int,
        help="Distribute the computation on multiple cores",
    )
    PARSER.add_argument(
        "--batch", type=int, help="Batch size of bulk import", default=1000
    )
    ARGS = PARSER.parse_args()
    transformer = DocTransformer(ARGS.feature, features[ARGS.feature])
    if ARGS.parallel is None:
        transformer.main(ARGS.batch)
    else:
        transformer.parallel_main(ARGS.parallel, ARGS.batch)
    # transformer.teardown()
