# vertex-pipelines-workshop
Workshop using Vertex Pipelines

In this tutorial, you learn to use `Vertex AI Pipelines`, `KubeFlow Components` and `Google Cloud Pipeline Components` to build a `custom` tabular regression model. `Vertex Pipelines workshop`, is a series of labs on how to build an end-to-end pipeline using Vertex Pipelines and Kubeflow Pipelines (kfp). In the pipeline we orchestrate data creation, data processing, model training and evaluation, and model deployment. We'll also see how to send payloads the endpoint deployed and how to run batch predition jobs. 

In this workshop we'll use the **public datase**t [Auto MPG](https://archive.ics.uci.edu/ml/datasets/auto+mpg) for demonstration purposes. The data concerns city-cycle fuel consumption in miles per gallon, to be predicted in terms of 3 multivalued discrete and 5 continuous attributes. The objective will be to build a model to predict "MPG" (Miles per Gallon).

The Google Cloud Components are [documented here](https://google-cloud-pipeline-components.readthedocs.io/en/latest/google_cloud_pipeline_components.aiplatform.html#module-google_cloud_pipeline_components.aiplatform).

The KubeFlow Compoenents are [documented here](https://www.kubeflow.org/docs/components/pipelines/v1/sdk-v2/python-function-components/)


### Step 1: Initial setup using Cloud Shell

- Activate Cloud Shell in your project by clicking the `Activate Cloud Shell` button as shown in the image below.


- Once the Cloud Shell has activated, copy the following codes and execute them in the Cloud Shell to enable the necessary APIs, and create Pub/Sub subscriptions to read streaming transactions from public Pub/Sub topics.


  ```shell
  gcloud services enable notebooks.googleapis.com
  gcloud services enable cloudresourcemanager.googleapis.com
  gcloud services enable aiplatform.googleapis.com
  gcloud services enable run.googleapis.com
  gcloud services enable cloudbuild.googleapis.com
  gcloud services enable bigquery.googleapis.com
  
  
  # Run the following command to grant the Compute Engine default service account access to read and write pipeline artifacts in Google Cloud Storage.
  PROJECT_ID=$(gcloud config get-value project)
  PROJECT_NUM=$(gcloud projects list --filter="$PROJECT_ID" --format="value(PROJECT_NUMBER)")
  gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:${PROJECT_NUM}-compute@developer.gserviceaccount.com"\
        --role='roles/storage.admin'
  ```
