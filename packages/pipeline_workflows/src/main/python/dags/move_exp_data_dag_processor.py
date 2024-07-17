import json
import pandas as pd
from airflow.models import Variable
from azure.storage.blob import BlobServiceClient

class MyDict(dict):
    def __str__(self):
        return json.dumps(self)


# Initialize BlobServiceClient
connection_string = "your_connection_string"  # Replace with your Azure connection string
blob_service_client = BlobServiceClient.from_connection_string(connection_string)


def get_variables():
    global source_chunk_path
    global container_name
    global archive_utterances_path
    global integration_processed_path
    global experiment_output
    source_chunk_path = Variable.get("utteranceschunkpath")
    archive_utterances_path = Variable.get("archiveutterancespath")
    integration_processed_path = Variable.get("integrationprocessedpath")
    experiment_output = Variable.get("experimentoutput")
    container_name = Variable.get("container")  # Updated to use container instead of bucket


def list_blobs_in_a_path(container_name, path):
    """List blobs in a specific path (prefix) in Azure Blob Storage."""
    container_client = blob_service_client.get_container_client(container_name)
    return container_client.list_blobs(name_starts_with=path)


def move_blob(container_name, source_blob_name, destination_blob_name):
    """Move blob from source to destination in Azure Blob Storage."""
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=source_blob_name)
    destination_blob_client = blob_service_client.get_blob_client(container=container_name, blob=destination_blob_name)

    # Start the copy operation
    destination_blob_client.start_copy_from_url(blob_client.url)

    # Delete the source blob after copy
    blob_client.delete_blob()


def download_blob(container_name, blob_name, local_file_name):
    """Download a blob from Azure Blob Storage."""
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    with open(local_file_name, "wb") as file:
        file.write(blob_client.download_blob().readall())


def copy_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
    """Copy a blob within Azure Blob Storage."""
    source_blob_client = blob_service_client.get_blob_client(container=bucket_name, blob=blob_name)
    destination_blob_client = blob_service_client.get_blob_client(container=destination_bucket_name, blob=destination_blob_name)
    
    # Start the copy operation
    destination_blob_client.start_copy_from_url(source_blob_client.url)


def count_utterances_file_chunks(**kwargs):
    get_variables()
    utterances_names = json.loads(Variable.get("utteranceschunkslist"))
    all_blobs = list_blobs_in_a_path(container_name, source_chunk_path)
    list_of_blobs = []
    for blob in all_blobs:
        if blob.name.endswith(".csv"):
            list_of_blobs.append(str(blob.name))
    print("***The utterances file chunks***", list_of_blobs)
    utterances_names["utteranceschunkslist"] = list_of_blobs
    utterances_names = MyDict(utterances_names)
    Variable.set("utteranceschunkslist", utterances_names)


def move_utterance_chunk(source_file_name, experiment_name):
    get_variables()
    archive_utterances_file_name = (
        archive_utterances_path
        + experiment_name
        + "/"
        + source_file_name.split("/")[-1]
    )
    move_blob(container_name, source_file_name, archive_utterances_file_name)


def copy_utterances(src_file_name, **kwargs):
    get_variables()
    extn_list = ["wav", "txt"]
    local_file_name = src_file_name.split("/")[-1]  # Use the file name from the blob
    download_blob(container_name, src_file_name, local_file_name)
    dataframe = pd.read_csv(local_file_name)
    
    for i, row in dataframe.iterrows():
        audio_id = row["audio_id"]
        source = row["source"]
        experiment_name = row["experiment_name"]
        file_name = row["clipped_utterance_file_name"]
        utterance_file_name = str(file_name).split(".")[0]
        print(file_name, utterance_file_name, audio_id)
        
        for extn in extn_list:
            source_blob_name = (
                integration_processed_path
                + source.lower()
                + "/"
                + str(audio_id)
                + "/clean/"
                + utterance_file_name
                + "."
                + extn
            )
            destination_blob_name = (
                experiment_output
                + experiment_name
                + "/"
                + str(audio_id)
                + "/"
                + utterance_file_name
                + "."
                + extn
            )
            copy_blob(
                bucket_name=container_name,
                blob_name=source_blob_name,
                destination_bucket_name=container_name,
                destination_blob_name=destination_blob_name,
            )
    move_utterance_chunk(src_file_name, experiment_name)


if __name__ == "__main__":
    pass
