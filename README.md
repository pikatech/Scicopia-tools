# Scicopia-tools

## Installation of dependencies

Pip -r requirements will resolve all of the dependencies that are necessary to run jobs, including a small English spaCy model.
It is recommended to install a large model for English afterwards by issuing:

````python -m spacy download en_core_web_lg````

The small models is only meant to be used by the unit tests.

This work has been funded by the German Research Foundation (DFG) as  part of the project D01 in the Collaborative Research Center (CRC) 1076  “AquaDiva”.