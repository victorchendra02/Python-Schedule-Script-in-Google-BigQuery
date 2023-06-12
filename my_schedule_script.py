import time
import datetime
import schedule
import random
import string
import json
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound


class bcolors:
    FAIL = "\033[91m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    OKBLUE = "\033[94m"
    HEADER = "\033[95m"
    OKCYAN = "\033[96m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def random_number(n):
    return random.randrange(0, n)


def generate_data():
    def generate_id():
        alphabet = string.ascii_uppercase
        generated_id = (
            f"{alphabet[random_number(len(alphabet))]}"
            f"{alphabet[random_number(len(alphabet))]}"
            f"{alphabet[random_number(len(alphabet))]}"
            f"{random_number(10)}"
            f"{random_number(10)}"
        )

        # print(generated_id, end="-")

        return generated_id

    class_ = ["C", "A", "X", "F", "P", "K", "G", "T"]
    names = ["Jason", "Victor", "Stefannus", "Renata", "Budi", "William", "Tamara", "Bambang"]

    i = random_number(len(class_))
    u = random_number(len(names))
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    data = [
        {
            "id": generate_id(),
            "class": class_[i],
            "name": names[u],
            "timestamp": current_time,
        }
    ]
    return data


def create_dataset_in_bigquery(credentials_path, new_dataset_name):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    client = bigquery.Client(credentials=credentials)

    new_dataset_name = f"{client.project}.{new_dataset_name}"

    dataset = bigquery.Dataset(new_dataset_name)

    dataset.location = "US"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print(
        f"Dataset created {bcolors.OKGREEN}{client.project}.{dataset.dataset_id}{bcolors.ENDC}"
    )


def create_table_in_bigquery(credentials_path, dataset_name, new_table_name, schema):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(credentials=credentials)

    new_table_name = f"{client.project}.{dataset_name}.{new_table_name}"

    table = bigquery.Table(new_table_name, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        f"Table created {bcolors.OKGREEN}{table.project}.{table.dataset_id}.{table.table_id}{bcolors.ENDC}"
    )


def table_insert_rows(credentials_path, dataset_name, table_name):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(credentials=credentials)

    table_name = f"{client.project}.{dataset_name}.{table_name}"

    rows_to_insert = generate_data()
    # print("MASUKK123")
    errors = client.insert_rows_json(table_name, rows_to_insert)  # Make an API request.
    if errors == []:
        print(f"{bcolors.OKGREEN}New rows have been added{bcolors.ENDC}")
    else:
        print(f"Encountered errors while inserting rows: {errors}")


def dataset_exists(credentials_path, dataset_name):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(credentials=credentials)

    dataset_name = f"{client.project}.{dataset_name}"

    try:
        client.get_dataset(dataset_name)  # Make an API request.
        print(f"Dataset {bcolors.FAIL}{dataset_name}{bcolors.ENDC} already exists")
        return "Found"
    except NotFound:
        print(f"Dataset {bcolors.OKBLUE}{dataset_name}{bcolors.ENDC} is not found")
        return "NotFound"


def table_exists(credentials_path, dataset_name, table_name):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(credentials=credentials)

    table_name = f"{client.project}.{dataset_name}.{table_name}"

    try:
        client.get_table(table_name)  # Make an API request.
        print("Table {} already exists.".format(table_name))
    except NotFound:
        print("Table {} is not found.".format(table_name))
    finally:
        return client.get_table(table_name)


def delete_dataset(credentials_path, dataset_name):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(credentials=credentials)

    dataset_name = f"{client.project}.{dataset_name}"

    # Use the delete_contents parameter to delete a dataset and its contents.
    # Use the not_found_ok parameter to not receive an error if the dataset has already been deleted.
    client.delete_dataset(
        dataset_name, delete_contents=True, not_found_ok=True
    )  # Make an API request.

    print(f"Dataset deleted {bcolors.FAIL}{dataset_name}{bcolors.ENDC}")


def run_schedule(credentials_path, dataset_name, table_name):
    schedule.every(6).to(13).seconds.do(
        table_insert_rows, credentials_path, dataset_name, table_name
    )

    # counter = 0
    while True:
        # if counter != 100:
        # counter += 1
        schedule.run_pending()
        time.sleep(1)
        # else:
        #     break


if __name__ == "__main__":
    cred = "C:\\Users\\victo\\[Internship] Data Labs\\dla-internship-program.json"
    dataset_name = "python_schedule_task"
    table_name = "users"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("class", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]

    # 1
    if dataset_exists(cred, dataset_name) == "NotFound":
        print(f"{bcolors.OKBLUE}Creating new dataset...{bcolors.ENDC}")
        create_dataset_in_bigquery(cred, dataset_name)
        create_table_in_bigquery(credentials_path=cred, dataset_name=dataset_name, new_table_name=table_name, schema=schema)

    # else:
    #     print(f"{bcolors.FAIL}Deleting dataset...{bcolors.ENDC}")
    #     delete_dataset(cred, dataset_name)
    #     print(f"{bcolors.OKBLUE}Creating new dataset...{bcolors.ENDC}")
    #     create_dataset_in_bigquery(cred, dataset_name)
    #     create_table_in_bigquery(credentials_path=cred, dataset_name=dataset_name, new_table_name=table_name, schema=schema)

    # 2
    run_schedule(cred, dataset_name, table_name)
