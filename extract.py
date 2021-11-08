#! /usr/bin/env python

import sys
import click
import pymysql
import subjects as s


def get_etds(dbc):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`nid` as 'nid', "
            "`node`.`uuid` as 'source_identifier', "
            "'Etd' as 'model', "
            "`node`.`title` as 'title' "
            "FROM `node` "
            "WHERE `node`.`type` = 'etd' AND `node`.`status`"
        )
        cursor.execute(sql)
        return cursor.fetchall()


def add_creator(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "dcterms_creator_value as 'creator' "
            "FROM `field_data_dcterms_creator` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) != 1:
        sys.exit(f"ERROR - {etd} does not have exactly one creator.")
    etd["creator"] = rows[0]["creator"].strip()


def add_identifier(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "dcterms_identifier_url as 'identifier' "
            "FROM `field_data_dcterms_identifier` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if not rows:
        sys.exit(f"ERROR - {etd} does not have any identifiers.")
    for row in rows:
        indentifier = row["identifier"]
        if indentifier.startswith("https://doi.org/10.22215"):
            etd["identifier"] = f"DOI: {indentifier}"
            break
    if "identifier" not in etd:
        sys.exit(f"ERROR - {etd} does not have a DOI.")


def add_subjects(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "dcterms_subject_value as 'subject' "
            "FROM `field_data_dcterms_subject` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    subjects = []
    for row in rows:
        subject = row["subject"].strip()
        if subject in s.proquest_to_lc:
            subjects.extend(s.proquest_to_lc[subject])
        else:
            subject = process_subject(subject)
            if subject in s.lc:
                subjects.append(subject)
            else:
                with open(
                    "missing_processed.txt", "a", encoding="utf-8"
                ) as missing_file:
                    missing_file.write(f"{subject}\n")
                with open(
                    "missing_raw.txt", "a", encoding="utf-8"
                ) as missing_file:
                    missing_file.write(f"{row['subject'].strip()}\n")
    etd["subjects"] = "|".join(subjects)


def process_subject(subject):
    # Use LC standard, no spaces around double dash.
    subject = subject.replace(" -- ", "--")
    # Drop trailing periods.
    subject = subject.rstrip(".")
    # Capitalize only the leading character in each 'chunk'.
    subject = "--".join([x.capitalize() for x in subject.split("--")])
    # Capitalize the first character after an "(".
    paran_indexes = []
    for index, c in enumerate(subject):
        if c == "(":
            paran_indexes.append(index)
    for index in paran_indexes:
        if index + 1 < len(subject):
            subject = (
                subject[: index + 1]
                + subject[index + 1].upper()
                + subject[index + 2 :]  # noqa: E203
            )
    # Capitalize the first character after an ", ".
    comma_indexes = []
    for index, c in enumerate(subject):
        if c == ",":
            comma_indexes.append(index)
    for index in comma_indexes:
        if index + 2 < len(subject) and subject[index + 1] == " ":
            subject = (
                subject[: index + 2]
                + subject[index + 2].upper()
                + subject[index + 3 :]  # noqa: E203
            )
    # Individual fixes
    subject = subject.replace("  ", " ")
    subject = subject.replace("Aids", "AIDS")
    subject = subject.replace("Air canada", "Air Canada")
    subject = subject.replace("Analog to digital", "Analog-to-digital")
    subject = subject.replace("Analog-to digital", "Analog-to-digital")
    subject = subject.replace("charles", "Charles")
    subject = subject.replace("Gas turbines", "Gas-turbines")
    subject = subject.replace("Georgraphy", "Geography")
    subject = subject.replace("indians", "Indians")
    # Raw Fixes
    if subject == "Airflow--Computersimulation":
        subject = "Air flow--Computer simulation"
    if subject == "Apl (Computer program language)":
        subject = "APL (Computer program language)"
    if subject == "Aquatic organisms --Effect of water pollution on":
        subject = "Aquatic organisms--Effect of water pollution on"
    if subject == "Architecture-- ontario--Toronto (Ont.)":
        subject = "Architecture--Ontario--Toronto"
    if subject == "Headache - treatment":
        subject = "Headache--Treatment"
    return subject


@click.command()
@click.option("--host", default="localhost")
@click.option("--user")
@click.option("--password", prompt=True, hide_input=True)
@click.option("--database")
def extract(host, user, password, database):
    # Connect to the database
    dbc = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        cursorclass=pymysql.cursors.DictCursor,
    )

    etds = get_etds(dbc)
    print(len(etds))

    with click.progressbar(etds) as bar:
        for etd in bar:
            add_creator(dbc, etd)
            add_identifier(dbc, etd)
            add_subjects(dbc, etd)

    import pprint

    pprint.pprint(etds)


if __name__ == "__main__":
    extract(auto_envvar_prefix="ETD_EXTRACTOR")
