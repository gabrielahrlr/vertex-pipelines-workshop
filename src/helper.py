def create_bucket(
    bucket_name: str,
    region: str,
    project_id: str,
 #   service_account: str
):
    """
    Create a new bucket in the US region with the STANDARD storage class
    Args:
        bucket_name: name of the bucket
        region: region or zone
        project_id: project id
        service_account: service account
    Output:
        new_bucket: bucket name (without gcs://): string
        new_bucket_gcs: bucket name (with gcs://): string
    """
    from google.cloud import storage
    
    # Initiate client
    storage_client = storage.Client(project=project_id)
    new_bucket = storage_client.bucket(bucket_name)
    new_bucket.storage_class = "STANDARD"
    
    # Create bucket
    try:
        new_bucket = storage_client.create_bucket(new_bucket, location=region)
    except:
        new_bucket.delete(force=True) 
        new_bucket = storage_client.create_bucket(new_bucket, location=region)
    
    print("Created bucket {} in {} with storage class {}".format(
        new_bucket.name,
        new_bucket.location,
        new_bucket.storage_class))
    
    # Add IAM roles
    #policy = new_bucket.get_iam_policy(requested_policy_version=3)
    #policy.bindings.append({"role": "roles/storage.objectViewer", "members": {service_account}})
    #policy.bindings.append({"role": "roles/storage.objectCreator", "members": {service_account}})
    #new_bucket.set_iam_policy(policy)
    
    new_bucket_gcs='gs://'+new_bucket.name
    new_bucket = new_bucket.name
    
    return new_bucket, new_bucket_gcs


def create_dataset(
    project_id: str,
    dataset_name: str,
    delete_existing_dataset=False
):
    """
    Creates a big query dataset
    Args:
        project_id: project id
        dataset_name: name of the dataset
        delete_existing_dataset: delete existing datset if exists
    Output:
        dataset_id: id of the bq-dataset
    """
    from google.cloud import bigquery
    
    # Initiate client
    client = bigquery.Client(project=project_id)
    
    # Create dataset
    dataset_id = "{}.{}".format(client.project, dataset_name)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    try:
        dataset = client.create_dataset(dataset, timeout=30)
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    except:
        if delete_existing_dataset==True:
            client.delete_dataset(
                dataset_id,
                delete_contents=True,
                not_found_ok=True
            )  
            print("Deleted dataset '{}'.".format(dataset_id))
            
            dataset = client.create_dataset(dataset, timeout=30)
            print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
        else:
            print(f"Dataset '{dataset_name}' already exists")
            
    return dataset_id


def get_project_id():
    """
    Returns the project id of the project
    
    Output:
        project_id: string
    """
    import google.auth
    _, PROJECT_ID = google.auth.default()
    
    return PROJECT_ID


def get_timestamp():
    """
    Returns timestamp
    
    Output:
        timestamp: string
    """
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    return timestamp


def delete_temp_buckets(
    project_id: str
):
    """
    Deletes all buckets starting with 'temp' in the GCP project
    Args:
        project_id: str
    """
    from google.cloud import storage

    storage_client = storage.Client(project=project_id)
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        if bucket.name[0:4]=='temp':
            new_bucket = storage_client.bucket(bucket.name)
            new_bucket.delete(force=True)
            print("Bucket '{}' deleted".format(bucket.name))
        

def upload_file_to_gcs(
    project_id: str,
    source: str,
    target: str,
    blob_name: str
):
    """
    Uploads a file as a blob to a GCS bucket
    Args:
        project_id: str
        target: str - gcs bucket name (without gs://)
        source: str - source (local or GCS)
        blob_name: str - name of the blob
    Return:
        path: str - full gs:// path of the uploaded file
    """
    from google.cloud import storage

    storage_client = storage.Client(project=project_id)
    
    bucket = storage_client.bucket(target)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(source)
        
    path = f"gs://{bucket.name}/{blob_name}"
    
    print(f"File saved to: {path}")
    
    return path
        
def upload_pipeline(
    package_path: str,
    bucket_name: str,
    project_id: str
):
    from google.cloud import storage
    
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(package_path)
    blob.upload_from_filename(package_path)
    
    
# def get_service_account():
#     # Todo: use python client instead.
#     service_account = !gcloud iam service-accounts list --filter="Compute Engine default service account"
#     service_account = np.array(service_account[1].split())[5]
#     SERVICE_ACCOUNT = f"{service_account}"
#     return SERVICE_ACCOUNT
