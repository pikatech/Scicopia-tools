## An overview of the dependencies

### 1. Parallelization / Cluster computing

- dask==2021.3.0
- dask-jobqueue==0.7.2
- distributed==2021.3.0
- streamz==0.6.2

https://distributed.dask.org/en/latest/queues.html

### 2. NLP toolkit

- spacy==3.0.5
- https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.0.0/en_core_web_lg-3.0.0.tar.gz#egg=en_core_web_lg
- https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.0.0/en_core_web_sm-3.0.0.tar.gz#egg=en_core_web_sm

### 3. Pipeline components

1. Dictionary taggers (taxa, chemicals)
   - pyahocorasick==1.4.2
   - intervaltree==3.1.0

2. Keyphrase extraction (pke)
   - git+https://github.com/boudinfl/pke.git@aa7df17214252b6bab2f1988eba89fdce8050818
   - language_data==1.0
   - nltk==3.6.1
3. Language detection
   - pycld2==0.41

### 4. Database connector

pyArango==1.3.5

### 5. Other

tqdm==4.59.0

A high-performance low overhead progress bar.



