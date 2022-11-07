#! /usr/bin/env python

from bs4 import BeautifulSoup
import click
import csv
import pymysql
import re
import datetime
import subjects as s
from internal_notes import internal_notes
import sys


def get_etds(dbc):
    today = datetime.date.today().year
    rights_notes = (
            f"Copyright © {today} the author(s). Theses may be used for "
            "non-commercial research, educational, or related academic "
            "purposes only. Such uses include personal study, distribution to"
            " students, research and scholarship. Theses may only be shared by"
            " linking to Carleton University Digital Library and no part may "
            "be copied without proper attribution to the author; no part may "
            "be used for commercial purposes directly or indirectly via a "
            "for-profit platform; no adaptation or derivative works are "
            "permitted without consent from the copyright owner."
        )
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`nid` as 'nid', "
            "`node`.`uuid` as 'source_identifier', "
            "`node`.`title` as 'title', "
            "`node`.`status` as 'visibility' "  
            "FROM `node` "
            "WHERE `node`.`type` = 'etd' AND (`node`.`status` = 1 OR `node`.`status` = 0) "
        )
        cursor.execute(sql)
        rows = cursor.fetchall()
    
    for i in range(len(rows)):
        rows[i]['rights_notes'] = rights_notes 
    for s in range(len(rows)):
        if rows[s]["visibility"] == 0:
            rows[s]["visibility"] = 'restricted'
        elif rows[s]["visibility"] == 1:
            rows[s]["visibility"] = 'open'
    return rows

def add_creator(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`dcterms_creator_value` as 'creator' "
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
            "`dcterms_identifier_url` as 'identifier'"
            "FROM `field_data_dcterms_identifier` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if not rows:
        etd["identifier"]= ""
    for row in rows:
        indentifier = row["identifier"]
        if indentifier.startswith("https://doi.org/10.22215"):
            etd["identifier"] = f"DOI: {indentifier}"
            break
    if "identifier" not in etd:
        etd["identifier"] = ""


def add_subjects(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`dcterms_subject_value` as 'subject' "
            "FROM `field_data_dcterms_subject` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    subjects = set()
    for row in rows:
        subject = row["subject"].strip()
        if subject in s.proquest_to_lc:
            subjects.update(s.proquest_to_lc[subject])
        else:
            subject = process_subject(subject)
            if subject in s.lc:
                subjects.add(subject)
            else:
                with open(
                    "curve_subject_not_migrated.txt", "a", encoding="utf-8"
                ) as not_migrated_file:
                    not_migrated_file.write(f"{subject}\n")
    etd["subjects"] = "|".join(sorted(subjects))


def process_subject(subject):
    # Use LC standard, no spaces around double dash.
    subject = subject.replace(" -- ", "--")
    subject = subject.replace("-- ", "--")
    subject = subject.replace(" --", "--")
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
    fix_table = [
        ("  ", " "),
        ("Acculturaton", "Acculturation"),
        ("Achitecture", "Architecture"),
        ("Afghan war", "Afghan War"),
        ("africa", "Africa"),
        ("Aids", "AIDS"),
        ("Air canada", "Air Canada"),
        ("americans", "Americans"),
        ("Analog to digital", "Analog-to-digital"),
        ("Analog-to digital", "Analog-to-digital"),
        ("Armed forces", "Armed Forces"),
        ("armed forces", "Armed Forces"),
        ("Bahai faith", "Bahai Faith"),
        ("Banff national park (Alta.)", "Banff National Park (Alta.)"),
        ("Boyne valley (Ire.)", "Boyne Valley (Ire.)"),
        ("canadian", "Canadian"),
        ("Catholic Church buildings", "Catholic church buildings"),
        ("Catholic church", "Catholic Church"),
        ("Catholic Churches", "Catholic churches"),
        ("charles", "Charles"),
        ("Chemistry, Analytic", "Analytical chemistry"),
        ("Cold war", "Cold War"),
        ("communicaton", "communication"),
        ("Computer aided design", "Computer-aided design"),
        ("Die casting", "Die-casting"),
        ("Candu reactors", "CANDU reactors"),
        ("Dna", "DNA"),
        ("Gas turbines", "Gas-turbines"),
        ("Georgraphy", "Geography"),
        ("indians", "Indians"),
        ("islam", "Islam"),
        ("Latin america", "Latin America"),
        ("Maritime provinces", "Maritime Provinces"),
        ("Middle east", "Middle East"),
        ("Monte carlo method", "Monte Carlo method"),
        ("New brunswick", "New Brunswick"),
        ("Newfoundland and labrador", "Newfoundland and Labrador"),
        ("North america", "North America"),
        ("north america", "North America"),
        ("Palestinian arab", "Palestinian Arab"),
        ("salish", "Salish"),
        ("Soviet union", "Soviet Union"),
        ("Sri lanka", "Sri Lanka"),
        ("Supersymetry", "supersymmetry"),
        ("Unesco", "UNESCO"),
        ("United nations", "United Nations"),
        ("United states", "United States"),
        ("Wireless communication system", "Wireless communication systems"),
        ("Wireless lan's", "Wireless LANs"),
        ("Wireless lans", "Wireless LANs"),
        ("World war", "World War"),
        ("World wide web", "World Wide Web"),
    ]
    for bad, good in fix_table:
        subject = subject.replace(bad, good)
    replace_table = [
        ("Aerial surveillance - canada", "Aerial surveillance--Canada"),
        ("AIDS to air navigation", "Aids to air navigation"),
        ("Airflow--Computersimulation", "Air flow--Computer simulation"),
        ("Apl (Computer program language)", "APL (Computer program language)"),
        (
            "Architecture-- ontario--Toronto (Ont.)",
            "Architecture--Ontario--Toronto",
        ),
        ("Bouddary element methods", "Boundary element methods"),
        ("Headache - treatment", "Headache--Treatment"),
        ("Multinational Armed Forces", "Multinational armed forces"),
        (
            "Safe sex in aids prevention--South Africa",
            "Safe sex in AIDS prevention--South Africa",
        ),
        ("Treads (Computer programs)", "Threads (Computer programs)"),
        ("Uml (Computer science)", "UML (Computer science)"),
        ("World War, 1939-1954--Canada", "World War, 1939-1945--Canada"),
    ]
    for old, new in replace_table:
        if subject == old:
            subject = new
    return subject


def add_abstract(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`dcterms_abstract_value` as 'abstract' "
            "FROM `field_data_dcterms_abstract` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) > 1:
        sys.exit(f"ERROR - {etd} has more than one abstract.")
    elif len(rows) == 1:
        soup = BeautifulSoup(rows[0]["abstract"], "html.parser")
        abstract = soup.get_text(strip=True)
        abstract = abstract.replace("\r", "")
        abstract = abstract.replace("\n", " ")
        abstract = re.sub(r" {2,}", " ", abstract)
        etd["abstract"] = abstract
    else:
        etd["abstract"] = ""


def add_publisher(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`dcterms_publisher_value` as 'publisher' "
            "FROM `field_data_dcterms_publisher` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) != 1:
        sys.exit(f"ERROR - {etd} does not have exactly one publisher.")
    etd["publisher"] = rows[0]["publisher"].strip()


def add_contributors(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`dcterms_contributor_first` as 'contributor_role', "
            "`dcterms_contributor_second` as 'contributor_name' "
            "FROM `field_data_dcterms_contributor` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    contributors = []
    for row in rows:
        name = row["contributor_name"].strip()
        role = row["contributor_role"].strip()
        if role:
            # Uppercase the first character of the role.
            role = role[0].upper() + role[1:]
            contributors.append(f"{name} ({role})")
        else:
            contributors.append(name)
    etd["contributors"] = "|".join(contributors)


def add_date(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`dcterms_date_value` as 'date' "
            "FROM `field_data_dcterms_date` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) != 1:
        sys.exit(f"ERROR - {etd} does not have exactly one date.")
    etd["date"] = rows[0]["date"][:4]


def add_language(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`dcterms_language_first` as 'language' "
            "FROM `field_data_dcterms_language` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) != 1:
        sys.exit(f"ERROR - {etd} does not have exactly one language.")
    language = rows[0]["language"].strip()
    if language == "French":
        etd["language"] = "fra"
    elif language == "Spanish":
        etd["language"] = "spa"
    elif language == "German":
        etd["language"] = "deu"
    elif language == "English":
        etd["language"] = "eng"
    else:
        sys.exit(f"ERROR - {etd} has unexpected language.")


def add_internal_notes(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`dcterms_description_noteinternal_value` as 'note' "
            "FROM `field_data_dcterms_description_noteinternal` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    notes = [row["note"] for row in rows]
    notes.extend(internal_notes.get(etd["nid"], []))
    etd["internal_notes"] = "|".join(notes)


def add_degree(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`thesis_degree_name_first` as 'name', "
            "`thesis_degree_name_second` as 'abbr' "
            "FROM `field_data_thesis_degree_name` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) != 1:
        sys.exit(f"ERROR - {etd} does not have exactly one degree.")
    etd["degree"] = f"{rows[0]['name']} ({rows[0]['abbr']})"


def add_degree_discipline(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`thesis_degree_discipline_value` as 'discipline' "
            "FROM `field_data_thesis_degree_discipline` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) > 1:
        sys.exit(f"ERROR - {etd} has more than one degree discipline.")
    elif len(rows) == 1:
        etd["degree_discipline"] = rows[0]["discipline"].strip()
    else:
        etd["degree_discipline"] = ""


def add_degree_level(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`thesis_degree_level_value` as 'level' "
            "FROM `field_data_thesis_degree_level` "
            "WHERE `entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) != 1:
        sys.exit(f"ERROR - {etd} does not have exactly one degree level.")
    level = rows[0]["level"].strip()
    if level == "Master's":
        etd["degree_level"] = "1"
    elif level == "Doctoral":
        etd["degree_level"] = "2"
    else:
        sys.exit(f"ERROR - {etd} has unexpected degree level.")


def add_pdf_file(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`file_managed`.`uri` as 'uri' "
            "FROM `field_data_etd_pdf` "
            "LEFT JOIN `file_managed` ON "
            "`field_data_etd_pdf`.`etd_pdf_fid` = `file_managed`.`fid` "
            "WHERE `field_data_etd_pdf`.`entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) > 1:
        sys.exit(f"ERROR - {etd} has more than one pdf file.")
    elif len(rows) == 1:
        etd["files"] = rows[0]["uri"]
    else:
        etd["files"] = ""


def add_supplemental_file(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`file_managed`.`uri` as 'uri' "
            "FROM `field_data_etd_supplemental_files` "
            "LEFT JOIN `file_managed` ON "
            "`field_data_etd_supplemental_files`.`etd_supplemental_files_fid`"
            " = "
            "`file_managed`.`fid` "
            "WHERE `field_data_etd_supplemental_files`.`entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) > 1:
        sys.exit(f"ERROR - {etd} has more than one supplemental file.")
    elif len(rows) == 1:
        etd["files"] = etd["files"] + "|" + rows[0]["uri"]

def add_agreement(dbc, etd):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`signature_policy_agreement_target_id` as 'agreement' "
            "FROM `field_data_signature_resource` "
            "LEFT JOIN `field_data_signature_policy_agreement` ON "
            "`field_data_signature_policy_agreement`.`entity_id`"
            " = "
            "`field_data_signature_resource`.`entity_id`"
            "WHERE `field_data_signature_resource`.`signature_resource_target_id` = {}".format((etd["nid"]))
        )
        cursor.execute(sql, etd)
        rows = cursor.fetchall()
    etd["agreement"] = ""
    temp = "" 
    if len(rows) >= 1:

        for i in range(len(rows)):

            if rows[i]['agreement'] == 11:
                temp = 'https://digital.library.carleton.ca/concern/works/pc289j04q|'
            elif rows[i]['agreement'] == 12:
                temp = temp + '://digital.library.carleton.ca/concern/works/j9602065z|'
            elif rows[i]['agreement'] == 13:
                temp = temp + 'https://digital.library.carleton.ca/concern/works/tt44pm84n|'
            elif rows[i]['agreement'] == 14:
                temp = temp + 'https://digital.library.carleton.ca/concern/works/nv9352841|'
            elif rows[i]['agreement'] == 15:
                temp = temp + 'https://digital.library.carleton.ca/concern/works/zc77sq08x|'
            elif rows[i]['agreement'] == 16:
                temp = temp + 'https://digital.library.carleton.ca/concern/works/ng451h485|'
            elif rows[i]['agreement'] == 17:
                temp = temp + 'https://digital.library.carleton.ca/concern/works/4t64gn18r|'
        etd["agreement"] = temp
    
@click.command()
@click.option("--host", default="localhost")
@click.option("--user", default="readonly")
@click.option("--password", prompt=True, hide_input=True)
@click.option("--database")
@click.option(
    "--parent-collection-id",
    required=True,
    help="The source ID for the parent collection in Hyrax.",
)
def extract(host, user, password, database, parent_collection_id):
    # Connect to the database
    dbc = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        cursorclass=pymysql.cursors.DictCursor,
    )

    etds = get_etds(dbc)

    with click.progressbar(etds) as bar:
        for etd in bar:
            add_creator(dbc, etd)
            add_identifier(dbc, etd)
            add_subjects(dbc, etd)
            add_abstract(dbc, etd)
            add_publisher(dbc, etd)
            add_contributors(dbc, etd)
            add_date(dbc, etd)
            add_language(dbc, etd)
            add_internal_notes(dbc, etd)
            add_degree(dbc, etd)
            add_degree_discipline(dbc, etd)
            add_degree_level(dbc, etd)
            add_pdf_file(dbc, etd)
            add_supplemental_file(dbc, etd)
            add_agreement(dbc, etd)

    print("Total: ", len(etds))
    print(
        " Missing subjects: ", sum(1 for etd in etds if etd["subjects"] == "")
    )

    header_columns = [
        "source_identifier",
        "model",
        "title",
        "creator",
        "identifier",
        "subject",
        "abstract",
        "publisher",
        "contributor",
        "date_created",
        "language",
        "internal_note",
        "degree",
        "degree_discipline",
        "degree_level",
        "resource_type",
        "collection",
        "file",
        "rights_notes",
        "visibility",
        "agreement"
    ]

    with open(
        "hyrax_import.csv", "w", newline="", encoding="utf-8"
    ) as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(header_columns)
        for etd in etds:
            csv_writer.writerow(
                [
                    etd["source_identifier"],
                    "Etd",
                    etd["title"],
                    etd["creator"],
                    etd["identifier"],
                    etd["subjects"],
                    etd["abstract"],
                    etd["publisher"],
                    etd["contributors"],
                    etd["date"],
                    etd["language"],
                    etd["internal_notes"],
                    etd["degree"],
                    etd["degree_discipline"],
                    etd["degree_level"],
                    "Thesis",
                    parent_collection_id,
                    etd["files"],
                    etd["rights_notes"],
                    etd["visibility"],
                    etd["agreement"]
                ]
            )


if __name__ == "__main__":
    extract(auto_envvar_prefix="ETD_EXTRACTOR")
