from __future__ import annotations

import json
from pathlib import Path

from datalad.distribution.dataset import Dataset
from roadmap_datamanager.helpers import ensure_paths, get_dataset_version

from datetime import datetime, timezone
from typing import Dict, Any


class Metadata:
    def __init__(self, ds_root: str | Path, path: str | Path = None, ):
        """
        Metadata class initializer.
        :param path: Relative or absolute path to file for which we consider the metadata.
        :param ds_root: Root path of the Datamanager tree.
        """
        self.ds_root, self.path, self.absolute_path, self.relposix = ensure_paths(ds_root, path)
        self.metapath = self.ds_root / 'metadata.json'
        if self.metapath.is_file():
            self.meta = json.loads(self.metapath.read_text())
        else:
            self.meta = {}

        self.path_key = self.relposix
        self.ds = Dataset(self.ds_root)

    def save(self):
        self.metapath.write_text(json.dumps(self.meta, indent=4))

    def add(self, payload: dict, mode: str = 'overwrite', user_name: str | None = None, user_email: str | None = None,
            extractor_name: str | None = None, extractor_version: str | None = None, node_type: str | None = None,
            name: str | None = None):
        """
        Add a metadata dictionaray to the class
        :param payload: (dict) The metadata dictionary
        :param mode: (str) 'merge' or 'overwrite'
        :param user_name: (str | None) The username associated with the metadata dictionary.
        :param user_email: (str | None) The email associated with the metadata dictionary.
        :param extractor_name: (str | None) The name of the metadata extractor.
        :param extractor_version: (str | None) The version of the metadata extractor.
        :param node_type: (str | None) The node type of the dataset.
        :param name: (str | None) Human-readable name for the file or folder whose meta-data will be saved.
        :return: no return value
        """
        dataset_id = self.ds.id
        dataset_version = get_dataset_version(self.ds)
        extraction_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        # Choose a Schema.org type
        if self.relposix == '.':
            type_str = "Dataset"
        elif self.absolute_path.exists() and self.absolute_path.is_dir():
            type_str = "Collection"
        else:
            type_str = "CreativeWork"

        # Empty relpath identifies the dataset itself
        if self.relposix != '.':
            node_id = f"datalad:{node_type}{dataset_id}:{self.relposix}"
            toplevel_type = 'file'
        else:
            node_id = f"datalad:{node_type}{dataset_id}"
            toplevel_type = 'dataset'

        # Interpret this JSON object as a Schema.org entity, so that name, description, etc., have their
        # standardized meanings.
        extracted: Dict[str, Any] = {
            "@context": {
                "@vocab": "https://schema.org/",
                "dm": "https://your-vocab.example/terms/"
            },
            "@type": type_str,
            "@id": node_id,
            "identifier": self.relposix,  # machine ID (relative path)
        }
        # Only include a human-facing name if you have one
        if name:
            extracted["name"] = name

        if payload:
            extracted.update(payload)

        # The extractor is the current script, as metadata is manually provided when installing a file or folder
        # This is the toplevel metadata record envelope, which contains the extracted metadata as a nested subfield
        # All according to the JSON-LD schema
        record = {
            "type": toplevel_type,
            "extractor_name": extractor_name,
            "extractor_version": extractor_version,
            "extraction_parameter": {
                "path": self.relposix,
                "node_type": node_type
            },
            "extraction_time": extraction_time,
            "agent_name": user_name,
            "agent_email": user_email,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "path": self.relposix,
            "extracted_metadata": extracted
        }

        if mode == 'overwrite' or self.path_key not in self.meta.keys():
            self.meta[self.path_key] = record
        elif mode == 'merge':
            # custom merge, start with 'extracted_metadata' field, which is a default field assumed to be present
            new_em = self.meta[self.path_key]['extracted_metadata'].update(record['extracted_metadata'])
            # now the top level merge
            self.meta[self.path_key] = self.meta[self.path_key].update(record)
            self.meta[self.path_key]['extracted_metadata'] = new_em

    def get(self, mode='envelope'):
        """
        Get a metadata dictionary for the file references in self.path/self.relposix/self.path_key.
        :param mode: (str) 'envelope' for entire metadata dictionary or 'meta' for metadata subdict only.
        :return: (dict) The metadata dictionary.
        """
        meta = self.meta.get(self.path_key, {})
        if mode == 'envelope':
            return meta
        elif mode == 'meta':
            return meta.get('extracted_metadata', {})
        return {}
