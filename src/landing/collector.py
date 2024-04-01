import requests
import json
import os
import re

from hdfs import Client


class DataCollector:
    def __init__(self, config: dict | str | None = None):
        super().__init__()
        self.config = None
        if config is None:
            self.config = {}
        elif isinstance(config, dict):
            self.config = config
        else:
            self.config = DataCollector._load_config_file(config)
        self._validate_config()

    @staticmethod
    def instance(config: dict | str) -> "DataCollector":
        if isinstance(config, dict):
            pass
        else:
            config = DataCollector._load_config_file(config)
        assert "type" in config.keys(), "Instancing configuration must specify 'type'"
        if isinstance(config["type"], type):
            collector = config["type"]
        elif isinstance(config["type"], str):
            collector = globals()[config["type"]]
        else:
            raise ValueError(
                f"Configuration 'type' must be a type or name, not {type(config).__name__}"
            )
        return collector(config)

    @staticmethod
    def _load_config_file(config_file: str) -> dict:
        with open(config_file, mode="r") as handler:
            config = json.load(handler)
            assert isinstance(
                config, dict
            ), "Configuration JSON file must be a dictionary"
            return config

    def retrive(self, version: str, client: Client) -> str:
        raise NotImplementedError()

    def versions(self) -> list[str]:
        raise NotImplementedError()

    def _validate_config(self):
        raise NotImplementedError()


class FileCollector(DataCollector):
    VERSION_REGEX = r"^.*?(\d(?:.*\d)?|\D*)\D*\.[^\.]+$"
    # Idealista:    ^(\d{4}_\d{2}_\d{2})_idealista\.(?:json|JSON)$
    # Wikidata:     ^(.*)_extended\.(?:csv|CSV)$
    # Barcelona:    ^(\d{4})_Distribucio_territorial_renda_familiar\.(?:csv|CSV)$
    FILE_REGEX = r"^(.+)\.[^\.]+$"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrive(self, version: str, client: Client) -> str:
        folders = self.config["folders"]
        folders = folders if isinstance(folders, list) else [folders]
        matches = [
            (folder, file)
            for folder in folders
            if os.path.exists(folder)
            for file in os.listdir(folder)
            if re.match(self.config["file"], file) is not None
            and re.match(self.config["version"], file).group(1) == version
        ]
        assert (
            len(matches) > 0
        ), f"Expected exactly one match for version '{version}' but no match was found"
        assert (
            len(matches) <= 1
        ), f"Expected exactly one match for version '{version}' but matched with {', '.join(matches)}"
        (folder, match) = matches[0]
        source = os.path.join(folder, match)
        dest = os.path.join("", match)
        client.upload(dest, source, overwrite=True)
        return dest

    def versions(self) -> list[str]:
        folders = self.config["folders"]
        folders = folders if isinstance(folders, list) else [folders]
        versions = [
            re.match(self.config["version"], file).group(1)
            for folder in folders
            if os.path.exists(folder)
            for file in os.listdir(folder)
            if re.match(self.config["file"], file) is not None
        ]
        return versions

    def _validate_config(self):
        # Correct or error on missing attributes
        cls = FileCollector
        assert (
            "folders" in self.config.keys()
        ), "FileCollector configuration must specify 'folders'"
        if "file" not in self.config.keys():
            self.config["file"] = self.config.get("version", cls.FILE_REGEX)
        if "version" not in self.config.keys():
            self.config["version"] = cls.VERSION_REGEX


class URLCollector(DataCollector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrive(self, version: str, client: Client) -> str:
        r = requests.get(
            self.config["URL"].format(version=version),
            stream=True,
            **self.config["request"],
        )
        dest = "file"
        chunk_size = 65536
        with client.write(dest, chunk_size=chunk_size) as writer:
            for chunk in r.iter_content(chunk_size=chunk_size):
                writer.write(chunk)
        return dest

    def versions(self) -> list[str]:
        raise NotImplementedError()

    def _validate_config(self):
        # Correct or error on missing attributes
        # cls = URLCollector
        assert (
            "URL" in self.config.keys()
        ), "URLCollector configuration must specify 'URL'"
        if "request" not in self.config.keys():
            self.config["request"] = {}
        assert isinstance(
            self.config["request"], dict
        ), "Request information ('request') must be a dictionary"
