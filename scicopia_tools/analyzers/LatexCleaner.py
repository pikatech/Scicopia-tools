#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 13:44:30 2020

@author: tech
"""
import logging


class LatexCleaner:
    field = "Clean"
    doc_section = ["abstract", "title", "author", "fulltext"]
    
    def __init__(self):
        """
        Loads dict for cleanning.

        Parameters
        ----------
        None.

        Returns
        -------
        None.

        """
        self.dict = {
        r"\textgreater":">",
        r"\textless":"<",
        r"{\textasciitilde a}":"&atilde;",
        r"{\\textasciitilde a}":"&atilde;",
        r"\textasciitilde":"~",
        r"\'e":"é",
        r"\'a":"á",
        r"\'A":"Á",
        r"\'o":"ó",
        r"\'\i":"í",
        r"\'i":"í",
        r"\'c":"ć",
        r"\"a":"ä",
        r"\"o":"ö",
        r"\"O":"Ö",
        r"\"u":"ü",
        r"\c{c}":"ç",
        r"\c{C}":"Ç",
        r"\u{g}":"ğ",
        r"\u{G}":"Ğ",
        r"\v{s}":"š",
        r"\v{S}":"Š",
        r"{``}":'“',
        r"{''}":'”',
        r"{`}":"‘",
        r"{'}":"’",
        r"<\\": "</",

        r"{---}":"—",
        r"{--}":"–",
        }

    def clean(self, doc: str):
        """
        Clean Sting.

        Parameters
        ----------
        doc: str
            String to clean

        Returns
        -------
        str
        cleanned string

        """
        for latex, normal in self.dict.items():
            doc = doc.replace(latex, normal)

        #TODO: solve Problem: lost of / in </x> while import by arangodoc
        start1 = -1
        while True:
            start1 = doc.find("<b>", start1+1) # search "first" <b>
            if start1 != -1: # <b> found
                start2 = doc.find("</b>", start1+1) # search </b> after <b>
                if start2 == -1: # no </b> found
                    start2 = doc.find("<b>", start1+1) # search "second" <b>
                    if start2 == -1: # no "second" <b> found
                        logging.error(f"odd number of <b> in {self.id}")
                        break
                    else:
                        doc = f"{doc[:start2+1]}/{doc[start2+1:]}"
                else:
                    start3 = doc.find("<b>", start1+1) # search "second" <b>
                    if start3 != -1 and start2 > start3: # <b> ... <b> ... </b>
                        logging.error(f"bb in {self.id}")
                        break
            else:
                break
        start1 = -1
        while True:
            start1 = doc.find("<i>", start1+1) # search "first" <i>
            if start1 != -1: # <i> found
                start2 = doc.find("</i>", start1+1) # search </i> after <i>
                if start2 == -1: # no </i> found
                    start2 = doc.find("<i>", start1+1) # search "second" <i>
                    if start2 == -1: # no "second" <i> found
                        logging.error(f"odd number of <i> in {self.id}")
                        break
                    else:
                        doc = f"{doc[:start2+1]}/{doc[start2+1:]}"
                else:
                    start3 = doc.find("<i>", start1+1) # search "second" <i>
                    if start3 != -1 and start2 > start3: # <i> ... <i> ... </i>
                        logging.error(f"ii in {self.id}")
                        break
            else:
                break
        start1 = -1
        while True:
            start1 = doc.find("<sup>", start1+1) # search "first" <sup>
            if start1 != -1: # <sup> found
                start2 = doc.find("</sup>", start1+1) # search </sup> after <sup>
                if start2 == -1: # no </sup> found
                    start2 = doc.find("<sup>", start1+1) # search "second" <sup>
                    if start2 == -1: # no "second" <sup> found
                        logging.error(f"odd number of <sup> in {self.id}")
                        break
                    else:
                        doc = f"{doc[:start2+1]}/{doc[start2+1:]}"
                else:
                    start3 = doc.find("<sup>", start1+1) # search "second" <sup>
                    if start3 != -1 and start2 > start3: # <sup> ... <sup> ... </sup>
                        logging.error(f"supsup in {self.id}")
                        break
            else:
                break
        start1 = -1
        while True:
            start1 = doc.find("<sub>", start1+1) # search "first" <sub>
            if start1 != -1: # <sub> found
                start2 = doc.find("</sub>", start1+1) # search </sub> after <sub>
                if start2 == -1: # no </sub> found
                    start2 = doc.find("<sub>", start1+1) # search "second" <sub>
                    if start2 == -1: # no "second" <sub> found
                        logging.error(f"odd number of <sub> in {self.id}")
                        break
                    else:
                        doc = f"{doc[:start2+1]}/{doc[start2+1:]}"
                else:
                    start3 = doc.find("<sub>", start1+1) # search "second" <sub>
                    if start3 != -1 and start2 > start3: # <sub> ... <sub> ... </sub>
                        logging.error(f"subsub in {self.id}")
                        break
            else:
                break

        # Italic \textit
        while True:
            start = doc.find(r"\textit{")
            if start != -1:
                end = doc.find("}",start)
                doc = f"{doc[:start]}<i>{doc[start+8:end]}</i>{doc[end+1:]}"
            else:
                break
        # Bold \textbf
        while True:
            start = doc.find(r"\textbf{")
            if start != -1:
                end = doc.find("}",start)
                doc = f"{doc[:start]}<b>{doc[start+8:end]}</b>{doc[end+1:]}"
            else:
                break

        # Italic {\\it n}
        while True:
            start = doc.find("{\\it ")
            if start != -1:
                end = doc.find("}",start)
                doc = f"{doc[:start]}<i>{doc[start+5:end]}</i>{doc[end+1:]}"
            else:
                break
        # Bold {\\bf n}
        while True:
            start = doc.find("{\\bf ")
            if start != -1:
                end = doc.find("}",start)
                doc = f"{doc[:start]}<b>{doc[start+5:end]}</b>{doc[end+1:]}"
            else:
                break

        # remove all {, } and \
        doc = doc.replace("\\", "")
        doc = doc.replace("{", "")
        doc = doc.replace("}", "")
        return doc

    def process(self, data):
        """
        Removes LatexArtefacts from dictfields
    
        Parameters
        ----------
        data :
            dict to clean

        Returns
        -------
        dict
            "Clean": bool if cleaned.
            doc_section: fields withe cleaned Text

        """
        res = {LatexCleaner.field: True}
        self.id = data['_key']
        for sec in LatexCleaner.doc_section:
            try:
                doc = data[sec]
                if type(doc) == list:
                    result = []
                    for entry in doc:
                        result.append(self.clean(entry))
                else:
                    result = self.clean(doc)
                res[sec] = result
            except:
                pass
        return res

    def release_resources(self):
        del self.dict
