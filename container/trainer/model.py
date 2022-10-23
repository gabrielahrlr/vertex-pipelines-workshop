
from google.cloud import bigquery
from google.cloud import storage
import numpy as np
import pandas as pd
import os
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

bqclient = bigquery.Client()
storage_client = storage.Client()

def download_table(bq_table_uri: str):
    prefix = "bq://"
    if bq_table_uri.startswith(prefix):
        bq_table_uri = bq_table_uri[len(prefix):]

    table = bigquery.TableReference.from_string(bq_table_uri)
    rows = bqclient.list_rows(
        table,
    )
    
    return rows.to_dataframe(create_bqstorage_client=False)


def build_and_compile_model(norm):
        model = keras.Sequential([
            norm,
            layers.Dense(64, activation='relu'),
            layers.Dense(64, activation='relu'),
            layers.Dense(1)
        ])
        model.compile(loss='mean_absolute_error', optimizer=tf.keras.optimizers.Adam(0.001))
        return model
    

def train_model(params):
    raw_dataset = download_table(params['data-dir'])
    raw_dataset.rename(columns = {
        'mpg':'MPG',
        'cyl':'Cylinders',
        'dis':'Displacement',
        'hp': 'Horsepower',
        'weight': 'Weight',
        'accel': 'Acceleration',
        'year': 'Model Year',
        'origin': 'Origin'}, inplace = True)

    # Get data in shape
    dataset = raw_dataset.copy()
    dataset.tail()
    dataset = dataset.dropna()
    dataset['Origin'] = dataset['Origin'].map({1: 'USA', 2: 'Europe', 3: 'Japan'})
    dataset = pd.get_dummies(dataset, columns=['Origin'], prefix='', prefix_sep='')
    train_dataset = dataset.sample(frac=0.8, random_state=0)
    test_dataset = dataset.drop(train_dataset.index)
    train_features = train_dataset.copy()
    test_features = test_dataset.copy()
    train_labels = train_features.pop('MPG')
    test_labels = test_features.pop('MPG')

    # Create model
    normalizer = tf.keras.layers.Normalization(axis=-1)
    normalizer = tf.keras.layers.Normalization(axis=-1)
    normalizer.adapt(np.array(train_features))
    normalizer = tf.keras.layers.Normalization(axis=-1)
    normalizer.adapt(np.array(train_features))
    first = np.array(train_features[:1])
    horsepower = np.array(train_features['Horsepower'])
    horsepower_normalizer = layers.Normalization(input_shape=[1,], axis=None)
    horsepower_normalizer.adapt(horsepower)


    dnn_model = build_and_compile_model(normalizer)
    dnn_model.summary()

    history = dnn_model.fit(
        train_features,
        train_labels,
        validation_split=0.2,
        verbose=0, epochs=100
    )

    test_results = {}

    test_results['dnn_model'] = dnn_model.evaluate(
        test_features,
        test_labels,
        verbose=0
    )

    # Log metrics
    metrics_training = {metric: values[-1] for metric, values in history.history.items()}
    metrics.log_metric('loss', metrics_training['loss'])
    metrics.log_metric('val_loss', metrics_training['val_loss'])
    model.uri = bucket
    model.metadata['loss'] = metrics_training['loss']
    model.metadata['val_loss'] = metrics_training['val_loss']
    model.metadata['pipeline'] = pipeline_name

    # Save the model to GCS
    dnn_model.save(params['model-dir'])


