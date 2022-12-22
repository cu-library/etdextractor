#! /usr/bin/env python

from bs4 import BeautifulSoup
from internal_notes import internal_notes
import click
import csv
import hashlib
import os
import pathlib
import pymysql
import re
import shutil
import subjects as s

SPLIT_PATTERN = "|||"

ACCESS_NOTE = (
    "This work is available on request. "
    "You can request a copy at "
    "https://library.carleton.ca/forms/request-pdf-copy-thesis"
)


class ProcessingException(Exception):
    """Raised when the processor encounters bad ETD data"""


def get_etds(dbc):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`nid` AS 'nid', "
            "`node`.`uuid` AS 'source_identifier', "
            "`node`.`title` AS 'title', "
            "CASE `node`.`status` "
            "  WHEN 0 THEN 'restricted' "
            "  WHEN 1 THEN 'open' "
            "END "
            "AS 'visibility' "
            "FROM `node` "
            "WHERE `node`.`type` = 'etd' "
            "AND `node`.`uuid` NOT IN ( "
            "  '50892e3d-aa3e-4722-b2a0-012accb0c52a' "  # Duplicate of a4c09901-eb02-4746-995d-343fb23111cd # noqa: E501
            ")"
        )
        cursor.execute(sql)
        rows = cursor.fetchall()
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
        raise ProcessingException(
            f"ERROR - {etd} does not have exactly one creator."
        )
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
        etd["identifier"] = ""
    for row in rows:
        indentifier = row["identifier"]
        if indentifier.startswith("https://doi.org/10.22215"):
            etd["identifier"] = f"DOI: {indentifier}"
            break
    if "identifier" not in etd:
        etd["identifier"] = ""


def add_subjects(dbc, etd, subject_processing_log_path):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`dcterms_subject_value` as 'subject' "
            "FROM `field_data_dcterms_subject` "
            "WHERE `entity_id` = %s "
            "ORDER BY `delta`"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()

    subjects = []
    with open(
        subject_processing_log_path, "a", newline="", encoding="utf-8"
    ) as csv_file:
        csv_writer = csv.writer(csv_file)
        if not rows:
            csv_writer.writerow(
                [
                    etd["title"],
                    f"https://curve.carleton.ca/node/{etd['nid']}",
                    etd["identifier"],
                    etd["creator"],
                    "CURVE record has no subjects",
                    "",
                ]
            )
        else:
            for row in rows:
                subject = row["subject"].strip()
                if subject.lower() in s.proquest_to_lc:
                    lcs_from_proquest = s.proquest_to_lc[subject.lower()]
                    subjects.extend(lcs_from_proquest)
                    flat_lcs = "|".join(lcs_from_proquest)
                    csv_writer.writerow(
                        [
                            etd["title"],
                            f"https://curve.carleton.ca/node/{etd['nid']}",
                            etd["identifier"],
                            etd["creator"],
                            f"Mapped from proquest to LC {flat_lcs}",
                            subject,
                        ]
                    )
                elif subject in s.lc:
                    subjects.append(subject)
                    csv_writer.writerow(
                        [
                            etd["title"],
                            f"https://curve.carleton.ca/node/{etd['nid']}",
                            etd["identifier"],
                            etd["creator"],
                            "Exact LC match found",
                            subject,
                        ]
                    )
                else:
                    processed_subject = process_subject(subject)
                    if processed_subject in s.lc:
                        subjects.append(processed_subject)
                        csv_writer.writerow(
                            [
                                etd["title"],
                                f"https://curve.carleton.ca/node/{etd['nid']}",
                                etd["identifier"],
                                etd["creator"],
                                f"LC match '{processed_subject}' found",
                                subject,
                            ]
                        )
                    else:
                        subjects.append(subject)
                        csv_writer.writerow(
                            [
                                etd["title"],
                                f"https://curve.carleton.ca/node/{etd['nid']}",
                                etd["identifier"],
                                etd["creator"],
                                "No LC match",
                                subject,
                            ]
                        )
    etd["subjects"] = SPLIT_PATTERN.join(subjects)


def process_subject(subject):
    # Use LC standard, no spaces around double dash.
    subject = subject.replace(" -- ", "--")
    subject = subject.replace("-- ", "--")
    subject = subject.replace(" --", "--")
    # Drop trailing periods.
    subject = subject.rstrip(".")
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
        raise ProcessingException(f"ERROR - {etd} has more than one abstract.")
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
        raise ProcessingException(
            f"ERROR - {etd} does not have exactly one publisher."
        )
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
    etd["contributors"] = SPLIT_PATTERN.join(contributors)


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
        raise ProcessingException(
            f"ERROR - {etd} does not have exactly one date."
        )
    etd["date"] = rows[0]["date"][:4]


def add_rights_notes(etd):
    etd["rights_notes"] = (
        f"Copyright © {etd['date']} the author(s). Theses may be used for "
        "non-commercial research, educational, or related academic "
        "purposes only. Such uses include personal study, distribution to"
        " students, research and scholarship. Theses may only be shared by"
        " linking to Carleton University Digital Library and no part may "
        "be copied without proper attribution to the author; no part may "
        "be used for commercial purposes directly or indirectly via a "
        "for-profit platform; no adaptation or derivative works are "
        "permitted without consent from the copyright owner."
    )


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
        raise ProcessingException(
            f"ERROR - {etd} does not have exactly one language."
        )
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
        raise ProcessingException(f"ERROR - {etd} has unexpected language.")


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
    etd["internal_notes"] = SPLIT_PATTERN.join(notes)


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
        raise ProcessingException(
            f"ERROR - {etd} does not have exactly one degree."
        )
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
        raise ProcessingException(
            f"ERROR - {etd} has more than one degree discipline."
        )
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
        raise ProcessingException(
            f"ERROR - {etd} does not have exactly one degree level."
        )
    level = rows[0]["level"].strip()
    if level == "Master's":
        etd["degree_level"] = "1"
    elif level == "Doctoral":
        etd["degree_level"] = "2"
    else:
        raise ProcessingException(
            f"ERROR - {etd} has unexpected degree level."
        )


def add_pdf_file_or_access_right(dbc, etd, destination_path):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`file_managed`.`uri` as 'uri', "
            "`filehash`.`md5` as 'md5' "
            "FROM `field_data_etd_pdf` "
            "LEFT JOIN `file_managed` ON "
            "`field_data_etd_pdf`.`etd_pdf_fid` = `file_managed`.`fid` "
            "LEFT JOIN `filehash` ON "
            "`field_data_etd_pdf`.`etd_pdf_fid` = `filehash`.`fid` "
            "WHERE `field_data_etd_pdf`.`entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) > 1:
        raise ProcessingException(f"ERROR - {etd} has more than one pdf file.")
    elif len(rows) == 1:
        etd["files"] = process_file_uri(
            rows[0]["uri"], destination_path, rows[0]["md5"]
        )
        etd["access_right"] = ""
    else:
        etd["files"] = ""
        etd["access_right"] = ACCESS_NOTE


def add_supplemental_file(dbc, etd, destination_path):
    with dbc.cursor() as cursor:
        sql = (
            "SELECT "
            "`file_managed`.`uri` as 'uri', "
            "`filehash`.`md5` as 'md5' "
            "FROM `field_data_etd_supplemental_files` "
            "LEFT JOIN `file_managed` ON "
            "`field_data_etd_supplemental_files`.`etd_supplemental_files_fid`"
            " = "
            "`file_managed`.`fid` "
            "LEFT JOIN `filehash` ON "
            "`field_data_etd_supplemental_files`.`etd_supplemental_files_fid` "
            " = "
            "`filehash`.`fid` "
            "WHERE `field_data_etd_supplemental_files`.`entity_id` = %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    if len(rows) > 1:
        raise ProcessingException(
            f"ERROR - {etd} has more than one supplemental file."
        )
    elif len(rows) == 1:
        etd["files"] = (
            etd["files"]
            + SPLIT_PATTERN
            + process_file_uri(
                rows[0]["uri"], destination_path, rows[0]["md5"]
            )
        )


def process_file_uri(uri, destination_path, md5):
    file_source_path = pathlib.Path(
        "/var/www/drupal/drupal-root/"
        + uri.replace("private://", "sites/default/files/private/").replace(
            "public://", "sites/default/files/"
        )
    )
    if not file_source_path.exists():
        raise ProcessingException(f"ERROR - {uri} doesn't exist.")
    file_destination_path = destination_path / file_source_path.name
    if file_destination_path.exists():
        raise ProcessingException(
            f"ERROR - {file_destination_path} already copied."
        )
    shutil.copy(file_source_path, file_destination_path)
    hash_md5 = hashlib.md5()
    with open(file_destination_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    if hash_md5.hexdigest().lower() != md5.lower():
        raise ProcessingException(
            f"ERROR - {file_destination_path} has the wrong hash."
        )
    return str(file_destination_path.name)


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
            "WHERE "
            "`field_data_signature_resource`.`signature_resource_target_id` "
            "= %s"
        )
        cursor.execute(sql, (etd["nid"],))
        rows = cursor.fetchall()
    agreement_id_to_hyrax_url = {
        11: "https://digital.library.carleton.ca/concern/works/pc289j04q",
        12: "https://digital.library.carleton.ca/concern/works/j9602065z",
        13: "https://digital.library.carleton.ca/concern/works/tt44pm84n",
        14: "https://digital.library.carleton.ca/concern/works/nv9352841",
        15: "https://digital.library.carleton.ca/concern/works/zc77sq08x",
        16: "https://digital.library.carleton.ca/concern/works/ng451h485",
        17: "https://digital.library.carleton.ca/concern/works/4t64gn18r",
    }
    agreement_ids = [row["agreement"] for row in rows]
    agreements = [
        agreement_id_to_hyrax_url[agreement_id]
        for agreement_id in agreement_ids
    ]
    etd["agreement"] = SPLIT_PATTERN.join(agreements)


@click.command()
@click.option("--host", default="localhost")
@click.option("--user", default="readonly")
@click.option("--password", prompt=True, hide_input=True)
@click.option("--database", default="drupal")
@click.option(
    "--parent-collection-id",
    help="The ID for the parent collection in Hyrax, from the URL.",
    default="XXXXXXXX",
)
@click.option(
    "--destination",
    help="The destination for the private files to be sent to",
    default="files",
)
@click.option(
    "--subjects-only",
    help="Process all fields or only process subjects",
    is_flag=True,
)
@click.pass_context
def extract(
    ctx,
    host,
    user,
    password,
    database,
    parent_collection_id,
    destination,
    subjects_only,
):
    # Connect to the database
    dbc = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        cursorclass=pymysql.cursors.DictCursor,
    )

    subject_processing_log_path = pathlib.Path(
        "subject-processing-log.csv"
    ).resolve()
    with open(
        subject_processing_log_path, "w", newline="", encoding="utf-8"
    ) as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(
            ["title", "link", "doi", "creator", "action", "subject"]
        )

    if subjects_only:
        try:
            with dbc:
                etds = get_etds(dbc)
                with click.progressbar(etds) as bar:
                    for etd in bar:
                        # identifier and creator are used in the report
                        add_identifier(dbc, etd)
                        add_creator(dbc, etd)
                        add_subjects(dbc, etd, subject_processing_log_path)
        except Exception as e:
            click.echo(e)
            ctx.exit(1)
        print("Total: ", len(etds))
        print(
            " Missing subjects: ",
            sum(1 for etd in etds if etd["subjects"] == ""),
        )
        ctx.exit(0)

    destination_path = pathlib.Path(destination).resolve()
    shutil.rmtree(destination_path)
    os.mkdir(destination_path)

    try:
        with dbc:
            etds = get_etds(dbc)
            with click.progressbar(etds) as bar:
                for etd in bar:
                    add_creator(dbc, etd)
                    add_identifier(dbc, etd)
                    add_subjects(dbc, etd, subject_processing_log_path)
                    add_abstract(dbc, etd)
                    add_publisher(dbc, etd)
                    add_contributors(dbc, etd)
                    add_date(dbc, etd)
                    add_rights_notes(etd)
                    add_language(dbc, etd)
                    add_internal_notes(dbc, etd)
                    add_degree(dbc, etd)
                    add_degree_discipline(dbc, etd)
                    add_degree_level(dbc, etd)
                    add_pdf_file_or_access_right(dbc, etd, destination_path)
                    add_supplemental_file(dbc, etd, destination_path)
                    add_agreement(dbc, etd),
    except Exception as e:
        click.echo(e)
        ctx.exit(1)

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
        "parents",
        "files",
        "rights_notes",
        "visibility",
        "agreement",
        "access_right",
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
                    etd["agreement"],
                    etd["access_right"],
                ]
            )


if __name__ == "__main__":
    extract(auto_envvar_prefix="ETD_EXTRACTOR")
