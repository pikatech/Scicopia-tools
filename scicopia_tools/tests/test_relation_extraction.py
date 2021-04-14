#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 15:08:07 2020

@author: tech
"""

import pytest


@pytest.fixture
def pipeline():
    import spacy

    nlp = spacy.load("en_core_web_sm", exclude=["ner", "lemmatizer", "textcat"])
    taxa_path = "scicopia_tools/tests/resources/taxa.tsv"
    nlp.add_pipe("taxontagger", config={"wordlist": taxa_path}, after="tagger")
    chemicals_path = "scicopia_tools/tests/resources/chemicals.txt"
    nlp.add_pipe("chemtagger", config={"wordlist": chemicals_path}, after="tagger")
    return nlp


def test_recognition(pipeline):
    # Taken from: https://en.wikipedia.org/wiki/Nitrospira_moscoviensis
    doc = pipeline(
        "In aerobic environments, N. moscoviensis obtains energy by oxidizing nitrite to nitrate."
    )
    assert len(doc.ents) == 3
