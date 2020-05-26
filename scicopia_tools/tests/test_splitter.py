#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 19 12:42:07 2020

@author: tech
"""

import json
from analyzers.TextSplitter import TextSplitter


def test_such_as():
    result = [
        (0, 56),
        (57, 75),
        (76, 118),
        (119, 139),
        (140, 200),
        (201, 263),
        (264, 306),
        (307, 366),
        (367, 414),
        (415, 468),
    ]
    with open("tests/data/arxiv.json") as input:
        doc = json.load(input)

    split = TextSplitter("en_core_web_sm")
    splits = split.process(doc["abstract"])
    assert result == splits["abstract_offsets"]
