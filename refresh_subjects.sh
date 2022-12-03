#! /usr/bin/env bash

if [[ ! -f "subjects.madsrdf.jsonld" ]]; then
    wget https://id.loc.gov/download/authorities/subjects.madsrdf.jsonld.gz
    gunzip subjects.madsrdf.jsonld.gz
    jq '.["@graph"][]|.["madsrdf:authoritativeLabel"]|select(. != null)|select(.["@language"]=="en")|.["@value"]' subjects.madsrdf.jsonld | pv --line-mode --size 1700000 | sort | uniq | sed 's/^/    /' | sed 's/$/,/' > subjects_list.txt
fi

cat subjects_header.txt subjects_list.txt subjects_footer.txt > subjects.py
