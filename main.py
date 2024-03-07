import json
import os
import requests
from dotenv import load_dotenv
import random
import string

load_dotenv()

CKAN_URL = os.getenv("CKAN_URL")
CKAN_API_KEY = os.getenv("CKAN_API_KEY")
CKAN_ORG_NAME = os.getenv("CKAN_ORG_NAME")

# generate random string using only letters
def random_string(length=15):
    return ''.join(random.choices(string.digits, k=length))

# pretty print dict
def pp(d):
    print(json.dumps(d, indent=2))

def get_dataset_from_rw(dataset_id: str):
    dataset = requests.get(
        f"https://api.resourcewatch.org/v1/dataset/{dataset_id}?includes=layer,metadata"
    ).json()
    return dataset["data"]["attributes"]

def map_layer_to_resource(layer: dict, dataset_rw_id: str, layer_rw_id: str):
    pp(layer)
    resource = {
        "title": layer.get("name", "No title"),
        "description": layer.get("description", "No title"),
        "name": slugify(layer.get("slug", random_string(15))),
        "format": "Layer",
        "url_type": "layer-raw",
        "state": "active",
        "url": f"https://api.resourcewatch.org/v1/dataset/{dataset_rw_id}/layer/{layer_rw_id}",
        "rw_id": layer_rw_id,
        "resource_type": "layer-raw",
    }
    return resource

def slugify(text: str):
    return text.lower().replace(" ", "-")

def map_dataset_rw_to_ckan(dataset: dict, rw_id: str):
    metadata = dataset.get("metadata", [{}])[0].get("attributes", {})
    layers = dataset.get("layer", [])
    ckan_dataset = {
        "name": slugify(dataset["slug"]),
        "title": dataset["name"],
        "notes": metadata.get("description", ''),
        "owner_org": CKAN_ORG_NAME,
        "visibility_type": "public",
        "is_approved": True,
        "technical_notes": "http://google.com",
        "private": False,
        "license_id": "not-specified",
        "author": "Data Lab",
        "author_email": "datalab@rw.com",
        "maintainer": "Data Lab",
        "maintainer_email": "datalab@rw.com",
        "state": "active",
        "language": metadata.get("language", "en"),
        "citation": metadata.get("citation", ""),
        "cautions": metadata.get("cautions", ""),
        "resources": [map_layer_to_resource(layer["attributes"], rw_id, layer["id"]) for layer in layers]
    }
    return ckan_dataset

def create_dataset_in_ckan(dataset_id: str):
    dataset = get_dataset_from_rw(dataset_id)
    ckan_dataset = map_dataset_rw_to_ckan(dataset, dataset_id)
    response = requests.post(
        f"{CKAN_URL}/api/3/action/package_create",
        json=ckan_dataset,
        headers={"Authorization": CKAN_API_KEY},
    )
    print(response.json())

datasets = [
    "065ffeb1-c0b8-49c4-80b7-bd168e17568c"
]

for dataset_id in datasets:
    create_dataset_in_ckan(dataset_id)
