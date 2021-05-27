#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 26 17:26:07 2020

@author: tech
"""

from typing import Dict

import pycld2 as cld2


class LangDetect:
    """
    Makes the Compact Langauge Detect 2 available as a pipeline module
    """

    field = "language"
    doc_section = "abstract"

    def __init__(self, model=None):
        """
        No initialization needed.

        Parameters
        ----------
        model : Optional[str]
            Only needed for compatibility with the other analyzers.

        Returns
        -------
        None.

        """

    def process(self, doc: str) -> Dict[str, str]:
        """
        Detects the language of a text using Compact Langauge Detect 2.

        Parameters
        ----------
        doc : str
            DESCRIPTION.

        Returns
        -------
        Dict[str, str]
            A field 'language' with the ISO 639-1 code associated with the text,
            if the language could be reliably detected and 'unk' otherwise.

        """
        isReliable, _, details = cld2.detect(doc)
        return {
            LangDetect.field: details[0][1] if isReliable else "unk"
        }  # ISO 639-1 Code

    def release_resources(self):
        pass
