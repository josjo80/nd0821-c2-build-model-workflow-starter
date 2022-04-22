#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb

import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()
    logger.info("Downloading artifact")
    artifact = run.use_artifact(args.input_artifact)
    artifact_path = artifact.file()

    df = pd.read_csv(artifact_path)

    #Drop outliers
    logger.info("Dropping outliers")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    #Convert last_review to datetime
    logger.info("Fixing data type")
    df['last_review'] = pd.to_datetime(df['last_review'])

    #Boundary check
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    #Saving cleaned data to csv
    logger.info("Saving cleaned data to csv")
    df.to_csv("clean_sample.csv", index=False)

    #Upload file as artifact to W&B
    logger.info("Uploading cleaned data as artifact to W&B")
    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully qualified name for input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the W&B artifact that will be created",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the artifact to be created",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price of real estate to include in data",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price of real estate to include in data",
        required=True
    )


    args = parser.parse_args()

    go(args)
