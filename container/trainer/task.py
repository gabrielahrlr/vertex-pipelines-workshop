
import os
import argparse
import logging
    
    

from trainer import model

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    logging.info(f"Training bq path {os.environ['AIP_TRAINING_DATA_URI']}")
    logging.info(f"Validation bq path {os.environ['AIP_VALIDATION_DATA_URI']}")
    # Vertex custom container training args. These are set by Vertex AI during training but can also be overwritten.
    parser.add_argument('--model-dir', dest='model-dir',
                        default=os.environ['AIP_MODEL_DIR'], type=str, help='GCS URI for saving model artifacts.')
    
    parser.add_argument('--train-data-dir', dest='train-data-dir',
                        default=os.environ['AIP_TRAINING_DATA_URI'], type=str, help='BQ URI where the data is')    
    
    parser.add_argument('--val-data-dir', dest='val-data-dir',
                        default=os.environ['AIP_VALIDATION_DATA_URI'], type=str, help='BQ URI where the data is')    
    
    args = parser.parse_args()
    params = args.__dict__
    
    
    
    model.train_model(params)
