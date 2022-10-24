#! /usr/bin/env python

from bs4 import BeautifulSoup
import click
import csv
import pymysql
import re
import subjects as s
from internal_notes import internal_notes
import sys


def get_etds(dbc):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`nid` as 'nid', "
            "`node`.`uuid` as 'source_identifier', "
            "`node`.`title` as 'title', "
            "`node`.`status` as 'visibility' "  
            "FROM `node` "
            "WHERE `node`.`type` = 'etd' AND `node`.`status` = 1 OR `node`.`status` = 0 "
            "AND `uuid` != '183bce88-4336-4656-b61d-aabaf33b57bd' AND `uuid` != '9e9049e6-9661-4a8a-87c7-7d526344b752' "
            "AND `uuid` != '6388ef9e-4268-42ac-8a47-261f34293b2b' AND `uuid` != 'a9b5b10e-763d-4ec5-95c6-de450752a513' "
            "AND `uuid` != '3a8cd7ae-9f6d-4658-86e8-30b8bb8be409' AND `uuid` != 'e1650247-6ce1-407b-b7fe-95a57d0e2b13' "
            "AND `uuid` != '2ef70b6f-0ff9-4eda-b40e-2994900b636f' AND `uuid` != '6978d884-28f8-4b01-8995-9d45366d4b43' "
            "AND `uuid` != '66e7b8f8-01ec-4c85-bac2-cce9cd9504c6' AND `uuid` != 'b6aab834-6b85-41df-86fe-e049293123a1' "
            "AND `uuid` != 'efb35d41-aa97-4ef1-b435-94da24638381' AND `uuid` != '3d9d3ef4-f407-42a7-b59f-54971b59b03c' "
            "AND `uuid` != 'c0174eee-783e-4fcf-ad57-0fc7cdc59bf3' AND `uuid` != '5aa2d928-2c0d-4255-8e86-b4c76d471dfb' "
            "AND `uuid` != '41967a1c-8dcf-4095-9986-66092defdc7d' AND `uuid` != '9376decd-bd89-4ee5-b527-5de7cbbb1df9' "
            "AND `uuid` != 'e1ba2001-c341-43e9-86ef-e265a2dd9918' AND `uuid` != 'a2784df2-6a4b-47d4-b19c-4e2ae1fb6c1a' "
            "AND `uuid` != '56fd0582-0147-41a2-930e-c03c03cbcaa0' AND `uuid` != '1e092f61-bf6a-4ff9-90c2-5407f8f89800' "
            "AND `uuid` != '50892e3d-aa3e-4722-b2a0-012accb0c52a' AND `uuid` != '2ace4faa-6490-4e4e-921f-8ef91adf3c27' "
            "AND `uuid` != 'bbd85d21-adba-4189-b8bc-0f088dd65336' AND `uuid` != 'a11b7c70-d25f-4aa9-9f6f-8e3a674ef7fa' "
            "AND `uuid` != 'af555e0d-c64a-4317-9ac1-13a5880c6b6e' "
        )
        cursor.execute(sql)
        rows = cursor.fetchall()
 
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
        sys.exit(f"ERROR - {etd} does not have any identifiers.")
    for row in rows:
        indentifier = row["identifier"]
        if indentifier.startswith("https://doi.org/10.22215"):
            etd["identifier"] = f"DOI: {indentifier}"
            break
    if "identifier" not in etd:
        etd["identifier"] = ""
        #sys.exit(f"ERROR - {etd} does not have a DOI.")


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
       # "rights_notes",
        "visibility"
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
                   # etd["rights_notes"],
                    etd["visibility"]
                ]
            )


if __name__ == "__main__":
    extract(auto_envvar_prefix="ETD_EXTRACTOR")
