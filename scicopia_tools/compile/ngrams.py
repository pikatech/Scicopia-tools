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
    punct = [",", ":", ";", "?", "!"]
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


def pairwise(iterable: Iterable) -> Iterator:
    """
    Generate an iterator returning a sequence of adjacent items
    in the iterable.
    s -> (s0,s1), (s1,s2), (s2, s3), ...

    Taken straight from
    https://docs.python.org/3/library/itertools.html#itertools-recipes

    Parameters
    ----------
    iterable : Iterable
        A generic Iterable containing any values.
        In the context of this module, these values will be
        of type 'str'.

    Returns
    -------
    Iterator
        A sequence of adjacent items in this iterable
    """
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def export_bigrams(
    db_access: DbAccess, nlp: spacy.language.Language, patterns=False
) -> Counter:
    """
    Extracts bigram frequencies of all abstracts stored in an ArangoDB collection.

    Parameters
    ----------
    db_access : DbAccess
        Access to the collection of the ArangoDB database one wants to access
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
        Bigram frequencies
    """
    bigrams = Counter()

    aql = f"FOR x IN {db_access.collection.name} FILTER x.abstract != null RETURN x.abstract"
    arango_docs = db_access.database.AQLQuery(
        aql, rawResults=True, batchSize=100, ttl=60
    )
    if patterns:
        matcher = Matcher(nlp.vocab)
        # "ADJ": "adjective",
        # "ADV": "adverb",
        # "NOUN": "noun",
        # "PROPN": "proper noun"
        # "VERB": "verb",
        # "X": "other"
        pattern = [
            [   # e.g. "space station"
                {"POS": {"IN": ["ADJ", "ADV", "NOUN", "PROPN", "VERB", "X"]}},
                {"POS": {"IN": ["ADJ", "ADV", "NOUN", "PROPN", "VERB", "X"]}},
            ],
            [   # e.g. "human-like AI"
                {"POS": {"IN": ["ADJ", "ADV", "NOUN", "PROPN", "VERB", "X"]}},
                {"ORTH": "-"},
                {"POS": {"IN": ["ADJ", "ADV", "NOUN", "PROPN", "VERB", "X"]}},
                {"POS": {"IN": ["ADJ", "ADV", "NOUN", "PROPN", "VERB", "X"]}},
            ],
            [   # e.g. "Alzheimer's disease"
                {"POS": {"IN": ["NOUN", "PROPN"]}},
                {"POS": "PART"},
                {"POS": {"IN": ["ADJ", "ADV", "NOUN", "PROPN", "VERB", "X"]}},
            ],
        ]
        matcher.add("Bigrams", pattern)
    for doc in tqdm(nlp.pipe(arango_docs)):
        if not patterns:
            for sent in doc.sents:
                word_pairs = pairwise(sent.text.split())
                bigrams.update(list(" ".join(word_pair) for word_pair in word_pairs))
        else:
            matches = matcher(doc)
            bigrams.update(doc[start:end].text for _, start, end in matches)
    return bigrams


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


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Export bigram frequencies to file")
    PARSER.add_argument(
        "output",
        type=str,
        help="Where to store the pickled bigrams",
    )
    PARSER.add_argument("--patterns", action="store_true",
                    help="Will use a spaCy matcher to extract bigrams")
    ARGS = PARSER.parse_args()
    try:
        arango_access = setup()
    except ScicopiaException as e:
        print(e)
    else:
        spacy_model = spacy.load("en_core_web_sm", disable=["ner", "textcat"])
        PATTERNS = ARGS.patterns
        bigrams = export_bigrams(arango_access, spacy_model, PATTERNS)
        if not PATTERNS:
            bigrams = clean_ngrams(bigrams)
        bigrams = lower_ngrams(bigrams)
        zstd_pickle(ARGS.output, bigrams)