from datetime import timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryHook
import requests
import zipfile
from airflow.decorators import dag, task
import os
import xml.etree.ElementTree as ET
from google.cloud import bigquery


CURRENT_DIRECTORY = os.path.dirname(__file__)
ASSETS_DIRECTORY = os.path.join(CURRENT_DIRECTORY, "assets")
DOWNLOADS_DIRECTORY = os.path.join(CURRENT_DIRECTORY, "downloads")
CITIES_XML_DIRECTORY = os.path.join(ASSETS_DIRECTORY, "municipalities.xml")
XML_NAMESPACE = "{http://www.w3.org/2005/Atom}"
URL_TEMPLATE = "https://geoportal.dgu.hr/api/identify/parcel-info?brojCestice={particle_number}&mbrKatastarskeOpcine={city_id}"


XML_PATH_TO_ENTRIES = f"./{XML_NAMESPACE}entry"
XML_PATH_TO_TITLES = f"./{XML_NAMESPACE}title"
XML_PATH_TO_IDS = f"./{XML_NAMESPACE}id"

XML_PATH_TO_PARTICLE_NUMBERS = "./{http://www.opengis.net/gml}featureMember/{http://oss}CESTICE/{http://oss}BROJ_CESTICE"
XML_PATH_TO_CITY_ID = "./{http://www.opengis.net/gml}featureMember/{http://oss}CESTICE/{http://oss}MATICNI_BROJ_KO"


@dag(dag_id="download_cadastre", params={"city": "SPLIT"})
def download_cadastre():
    @task()
    def find_target_city_download_url(**context):
        # Extract the target city name
        target_city_name = context["params"]["city"]
        target_city_name = target_city_name.upper()

        # Parse the cities XML
        with open(CITIES_XML_DIRECTORY) as f:
            tree = ET.parse(f)
            root = tree.getroot()
            entries = root.findall(XML_PATH_TO_ENTRIES)

        # Find the download URL for the target city
        target_city_download_url = None
        for entry in entries:
            title = entry.find(XML_PATH_TO_TITLES).text
            city_name = " ".join(title.split(" ")[2::])

            if city_name == target_city_name:
                target_city_download_url = entry.find(XML_PATH_TO_IDS).text

        # Raise an error if the target city is not found
        if not target_city_download_url:
            raise ValueError(
                f"Could not find download URL for {target_city_name}"
            )

        # Return the download URL
        return target_city_download_url


    @task()
    def download_city_data(download_url, **context):
        # Extract the target city name
        target_city_name = context["params"]["city"]
        target_city_name = target_city_name.lower()

        # Download the archive
        resp = requests.get(download_url)
        resp.raise_for_status()

        # Save the archive to the downloads directory
        filename = f"{target_city_name}.zip"
        download_file_path = os.path.join(DOWNLOADS_DIRECTORY, filename)

        with open(download_file_path, "wb") as f:
            f.write(resp.content)

        return download_file_path


    @task()
    def unzip_city_data(download_file_path, **context):
        target_city_name = context["params"]["city"].lower()

        # Unzip the archive
        unzip_directory = os.path.join(DOWNLOADS_DIRECTORY, target_city_name)
        with zipfile.ZipFile(download_file_path, "r") as zip_ref:
            zip_ref.extractall(unzip_directory)

        return unzip_directory


    @task()
    def extract_particle_ids(unzip_directory, **context):
        # Parse the particle data
        particle_data = os.path.join(unzip_directory, "katastarske_cestice.gml")
        with open(particle_data) as f:
            tree = ET.parse(f)
            root = tree.getroot()
            particle_id_elements = root.findall(XML_PATH_TO_PARTICLE_NUMBERS)
            city_id_element = root.find(XML_PATH_TO_CITY_ID)

        # Extract the particle IDs
        particle_ids = []
        for e in particle_id_elements:
            particle_ids.append(e.text)

        # Extract the municipality ID
        municipality_id = next(city_id_element.iter()).text

        return particle_ids, municipality_id


    @task(execution_timeout=timedelta(hours=2))
    def download_particle_data(particle_and_city_ids, **context):
        # Extract the target city name
        target_city_name = context["params"]["city"]
        target_city_name = target_city_name.lower()

        particle_ids, city_id = particle_and_city_ids

        # Create a storage file for the particle data
        storage_file_path = os.path.join(DOWNLOADS_DIRECTORY, f"{target_city_name}.jsonl")
        storage_file = open(storage_file_path, "w")

        # Download each particle from the Geoportal API
        for particle_id in particle_ids[0:10]:
            particle_url = URL_TEMPLATE.format(
                particle_number=particle_id,
                city_id=city_id,
            )
            resp = requests.get(particle_url)

            # Skip particles that return an error
            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                continue

            # Write the particle data to the storage file
            storage_file.write(resp.text + "\n")
        return storage_file_path


    @task()
    def upload_to_bq(storage_file_path):
        # Get the BigQuery client
        bq_hook = BigQueryHook(gcp_conn_id="devfest2024_conn_id")
        bq_client = bq_hook.get_client()
        table_id = "devfest_2024.cadastre"

        # Create job config
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )

        # Upload the data to BigQuery
        with open(storage_file_path, "rb") as source_file:
            bq_client.load_table_from_file(
                source_file,
                table_id,
                job_config=job_config,
            )

    @task()
    def cleanup(**context):
        # Extract the target city name
        target_city_name = context["params"]["city"]
        target_city_name = target_city_name.lower()

        # Remove the downloads directory
        downloads_directory = os.path.join(DOWNLOADS_DIRECTORY, target_city_name)
        os.system(f"rm -rf {downloads_directory}")

        # Remove the storage file
        storage_file_path = os.path.join(DOWNLOADS_DIRECTORY, f"{target_city_name}.jsonl")
        os.remove(storage_file_path)

        # Remove the archive
        archive_file_path = os.path.join(DOWNLOADS_DIRECTORY, f"{target_city_name}.zip")
        os.remove(archive_file_path)


    
    download_url = find_target_city_download_url()
    download_file_path = download_city_data(download_url)
    unzip_directory = unzip_city_data(download_file_path)
    particle_and_city_ids = extract_particle_ids(unzip_directory) 
    storage_file_path = download_particle_data(particle_and_city_ids)
    upload_to_bq(storage_file_path) >> cleanup()


download_cadastre()
