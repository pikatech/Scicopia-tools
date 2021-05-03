#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 15:03:36 2021

@author: tech
"""
import argparse
import pickle
from collections import Counter
from itertools import tee
from typing import Any, Iterable, Iterator

import spacy
import zstandard as zstd
from spacy.matcher import Matcher
from tqdm import tqdm

from scicopia_tools.db.arango import DbAccess, setup
from scicopia_tools.exceptions import ScicopiaException


# "ADJ": "adjective",
# "ADV": "adverb",
# "NOUN": "noun",
# "PROPN": "proper noun"
# "VERB": "verb",
# "X": "other"
ngram_masks =  {1: [
        [  # e.g. "meta-algorithm"
            {"POS": {"IN": ["NOUN", "PROPN"]}},
            {"ORTH": "-"},
            {"POS": {"IN": ["NOUN", "PROPN"]}},
        ],
        [  # e.g. "state-of-the-art"
            {"POS": {"IN": ["NOUN", "PROPN"]}},
            {"ORTH": "-"},
            {"POS": "ADP"},
            {"ORTH": "-"},
            {"POS": "DET"},
            {"ORTH": "-"},
            {"POS": {"IN": ["NOUN", "PROPN"]}},
        ]
    ],
    2: [
    [  # e.g. "random variable"
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "space station", "Nash equilibrium"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "long-term memory"
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "tabular grid-world",
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "human-like AI", "fault-tolerant setting"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "zero-shot transfer"
        {"POS": "NUM"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "Alzheimer's disease"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": "PART"},
        {"POS": "NOUN"},
    ],
    [  # e.g. "semi-supervised learning"
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "grid-world environments"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "continuous-state puddle-world"
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "Simulation-to-real transfer"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": "ADP"},
        {"ORTH": "-"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "stop-and-go traffic"
        {"POS": "VERB"},
        {"ORTH": "-"},
        {"POS": "CCONJ"},
        {"ORTH": "-"},
        {"POS": "VERB"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "state-of-the-art performance"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": "ADP"},
        {"ORTH": "-"},
        {"POS": "DET"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "optimism-in-face-of-uncertainty principle"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": "ADP"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": "ADP"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "challenging environments"
        {"DEP": "amod"},
        {"DEP": "pobj"},
    ],
    [  # e.g. "dynamic programming"
        {"DEP": "amod"},
        {"DEP": "conj"},
    ],
    [  # e.g. "machine learning"
        {"DEP": "compound"},
        {"DEP": "pobj"},
    ],
    [  # e.g. "Q-learning"
        {"DEP": "compound"},
        {"ORTH": "-"},
        {"DEP": "nsubj"},
    ],
],
3: [
    [  # e.g. "atomic electronic structure",
        #      "temporally abstract actions"
        {"POS": {"IN": ["ADJ", "ADV"]}},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "non-linear dynamical models",
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "model-free control options",
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "complex multi-vehicle interactions",
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "preliminary Gazebo experiments", "human world record"
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "air traffic control"
        #      "Monte Carlo regression"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "out-of-distribution traffic densities"
        {"POS": "SCONJ"},
        {"ORTH": "-"},
        {"POS": "ADP"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "X-ray free-electron laser"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "Sim-to-real transfer systems"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": "ADP"},
        {"ORTH": "-"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "state-of-the-art reinforcement learning"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": "ADP"},
        {"ORTH": "-"},
        {"POS": "DET"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "reinforcement learning technique"
        {"DEP": "compound"},
        {"DEP": "compound"},
        {"DEP": "dobj"},
    ],
    [  # e.g. "reinforcement learning strategies"
        {"DEP": "compound"},
        {"DEP": "compound"},
        {"DEP": "nsubjpass"},
    ],
    [  # e.g. "deep reinforcement learning"
        {"DEP": "amod"},
        {"DEP": "compound"},
        {"DEP": "nsubj"},
    ],
    [  # e.g. "continually shrinking segment"
        {"DEP": "advmod"},
        {"DEP": "amod"},
        {"DEP": "pobj"},
    ],
],
4: [
    [  # e.g. "deep Reinforcement Learning methods",
        #      "experimental charge state distributions"
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "dense commercial air traffic",
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "modular deep neural network",
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "previously unknown thermodynamic cycle",
        {"POS": "ADV"},
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "previously unsolvable hard-exploration problems",
        {"POS": "ADV"},
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # "Simultaneous Localization and Mapping"
        {"POS": "PROPN"},
        {"POS": "PROPN"},
        {"POS": "CCONJ"},
        {"POS": "PROPN"},
    ],
    [  # e.g. "non-differential convex optimization problem",
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "multi-goal reinforcement learning algorithm",
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "various benchmark grid-world games",
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "asynchronous advantage actor-critic algorithm",
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "Stochastic Lower Bounds Optimization",
        #      "task relation learning approach"
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "RL state-feedback boundary controllers",
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "large-scale fault-tolerant quantum computation",
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"ORTH": "-"},
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
],
5: [
    [  
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "maximum entropy inverse reinforcement learning",
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "poly-time linear programming solution",
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "traditional Simultaneous Localization and Mapping"
        {"POS": "ADJ"},
        {"POS": "PROPN"},
        {"POS": "PROPN"},
        {"POS": "CCONJ"},
        {"POS": "PROPN"},
    ],
    [  # e.g. "small neural network control law"
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "classical approximate dynamic programming approaches"
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [
        {"POS": "ADV"},
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # "completely differentiable deep neural network"
        {"POS": "ADV"},
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
    [  # e.g. "two-player general-sum stochastic game framework",
        {"POS": "NUM"},
        {"ORTH": "-"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": "ADJ"},
        {"ORTH": "-"},
        {"POS": "ADJ"},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
        {"POS": {"IN": ["NOUN", "PROPN"]}},
    ],
]
}


def clean_ngrams(ngrams: Counter) -> Counter:
    """
    Remove all n-grams that contain punctuation that is not a
    period mark - (',', ':', ';', '?', '!').
    This also applies to newline characters (\\n).

    Examples:
        "code, as well"
        "Comments: Has been accepted"

    Parameters
    ----------
    ngrams : Counter
        A collection of text fragments

    Returns
    -------
    Counter
        A new collection of filtered text fragments
    """
    punct = [",", ":", ";", "?", "!", "-"]
    cleaned_ngrams = Counter(
        {
            k: v
            for (k, v) in ngrams.items()
            if not ", " in k and not ": " in k and not k[-1] in punct and not "\n" in k
        }
    )
    return cleaned_ngrams


def lower_ngrams(ngrams: Counter) -> Counter:
    """
    Lowercase all strings in the Counter.

    Parameters
    ----------
    ngrams : Counter
        A Counter of strings, in this case n-grams

    Returns
    -------
    Counter
        A Counter of lowercased strings
    """
    lowercased_ngrams = Counter()
    # Sadly, this doesn't work with a dictionary comprehension
    for k, v in ngrams.items():
        lowercased_ngrams.update({k.lower(): v})
    return lowercased_ngrams


def trim_ngrams(ngrams: Counter, threshold: int=2) -> Counter:
    """
    Remove all strings in the Counter with a frequency lower
    than a threshold.

    Parameters
    ----------
    ngrams : Counter
        A Counter of strings, in this case n-grams

    Returns
    -------
    Counter
        A Counter of trimmed frequencies
    """
    return Counter({k: v for k,v in ngrams.items() if v >= threshold})


def ngrams(iterable: Iterable, n=3):
    """
    Generate an iterator returning a sequence of adjacent items
    in the iterable.
    s -> (s0, s1, s2), (s1, s2, s3), (s2, s3, s4), ...

    A generalization of the pairwise function from
    https://docs.python.org/3/library/itertools.html#itertools-recipes

    Parameters
    ----------
    iterable : Iterable
        A generic Iterable containing any values.
        In the context of this module, these values will be
        of type 'str'.
    n : int, optional
        The order of the n-grams, by default 3

    Returns
    -------
    Iterator
        A sequence of adjacent items in this iterable
    """
    iterables = tee(iterable, n)
    for i, part in enumerate(iterables):
        # Shift iterators accordingly
        for _ in range(i):
            next(part, None)
    return zip(*iterables)


def export_ngrams(
    docs: Iterator[str], nlp: spacy.language.Language, n: int, patterns=False
) -> Counter:
    """
    Extracts n-gram frequencies of a series of documents

    Parameters
    ----------
    docs : Iterator[str]
        An iterator of documents, e.g. abstracts
    nlp : spacy.language.Language
        A spaCy language model, e.g. en_core_web_sm
    patterns : bool, optional
        Further analysis of neighboring tokens, by default False.
        If True, a spaCy matcher will be used to filter most of the stopword
        combinations that might not be of interest.
        The matcher will also extract bigrams made up of three tokens, like
        "Alzheimer's disease" and "human-like AI", while filtering most of the
        other punctuation.

    Returns
    -------
    Counter
        n-gram frequencies

    Raises
    ------
    ValueError
        In case that the 'patterns' options is used for anything but bigrams
    """
    n_grams = Counter()

    if patterns:
        if not 1 <= n <= 5:
            raise ValueError("Patterns can only be used for n-grams with n <= 5.")
        matcher = Matcher(nlp.vocab)
        matcher.add("N-grams", ngram_masks[n])
        for doc in tqdm(nlp.pipe(docs)):
            matches = matcher(doc)
            candidates = (doc[start:end].text for _, start, end in matches)
            # some n-grams are part of bigger m-grams and might
            # start or end with a '-' because of that
            n_grams.update(c for c in candidates if not c.startswith('-') and not c.endswith('-'))
    else:
        for doc in tqdm(nlp.pipe(docs)):
            for sent in doc.sents:
                n_words = ngrams(sent.text.split(), n=n)
                n_grams.update(list(" ".join(words) for words in n_words))
    return n_grams


def zstd_pickle(filename: str, obj: Any, protocol: int = 4):
    """
    Pickles a Zstandard-compressed object.

    Parameters
    ----------
    filename : str
        The path to the storage destination of the file
    obj : Any
        Any picklable object
    protocol : int, optional
        Protocol to be used for pickling, by default 4.
        Protocol version 4 was added in Python 3.4.
    """
    cctx = zstd.ZstdCompressor(level=10)
    with open(filename, "wb") as fh:
        with cctx.stream_writer(fh) as compressor:
            pickle.dump(obj, compressor, protocol=protocol)


def fetch_abstracts(db_access: DbAccess) -> Iterator[str]:
    """
    Fetches the abstracts of all documents in a collection that have them.

    Parameters
    ----------
    db_access : DbAccess
        Access to the collection of the ArangoDB database one wants to access

    Returns
    -------
    Iterator[str]
        An iterator of all available abstracts
    """
    aql = f"FOR x IN {db_access.collection.name} FILTER x.abstract != null RETURN x.abstract"
    docs = db_access.database.AQLQuery(aql, rawResults=True, batchSize=100, ttl=60)
    return docs


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Export bigram frequencies to file")
    PARSER.add_argument(
        "output",
        type=str,
        help="Where to store the pickled n-grams",
    )
    PARSER.add_argument(
        "--patterns",
        action="store_true",
        help="Use a spaCy matcher to extract n-grams. n has to be between 1 and 5.",
    )
    PARSER.add_argument(
        "-n",
        type=int,
        default=2,
        help="The order of the n-grams, by default 2",
    )
    PARSER.add_argument(
        "--threshold", "-t",
        type=int,
        default=0,
        help="A threshold for n-gram frequencies to be kept, by default 0",
    )
    ARGS = PARSER.parse_args()
    try:
        arango_access = setup()
        db_docs = fetch_abstracts(arango_access)
    except ScicopiaException as e:
        print(e)
    else:
        spacy_model = spacy.load("en_core_web_lg", exclude=["ner", "textcat"])
        PATTERNS = ARGS.patterns
        frequencies = export_ngrams(db_docs, spacy_model, ARGS.n, PATTERNS)
        frequencies = clean_ngrams(frequencies)
        frequencies = lower_ngrams(frequencies)
        THRESHOLD = ARGS.threshold
        if THRESHOLD <= 0:
            pass
        else:
            frequencies = trim_ngrams(frequencies)
        zstd_pickle(ARGS.output, frequencies)
