import requests
import urllib
import json
import os
import re

from datetime import datetime
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
    # Income:   (?i)(?P<link>https://opendata-ajuntament\.barcelona\.cat/data/[-A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=]+/download/(?P<version>\d+)_distribucio_territorial_renda_familiar\.csv)
    # Padró:    (?si)title="(?P<version>\d+)_pad_cdo_b_barri-des\.csv".+?(?P<link>https://opendata-ajuntament\.barcelona\.cat/data/[-A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=]+/download)
    INFO_REGEX = r"(?P<link>https?:\/\/(?:www\.)?(?:[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b)*(?:\/(?P<version>[\d\w\.-]*))+(?:[\?])*(?:[-A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=]+)*)"

    def __init__(self, *args, **kwargs):
        self.links: dict[str, str] = {}
        self.get_URL = None
        super().__init__(*args, **kwargs)

    def retrive(self, version: str, client: Client) -> str:
        link = self.links.get(version)
        assert link is not None
        r = requests.get(
            link,
            stream=True,
            **self.config["request"],
        )
        extension = [
            elem.split("/", maxsplit=1)[1]
            for elem in r.headers["Content-Type"].split(";")
            if "/" in elem
        ][0]
        dest = f"file.{extension}"
        chunk_size = 65536
        with client.write(dest, buffersize=chunk_size, overwrite=True) as writer:
            for chunk in r.iter_content(chunk_size=chunk_size):
                writer.write(chunk)
        return dest

    def versions(self) -> list[str]:
        self.get_URL()
        return list(self.links.keys())

    def get_now_URL(self):
        now = self.config["now"]
        version = datetime.today().strftime(now["date"])
        self.links[version] = now["URL"]

    def get_scraping_URL(self):
        scraping = self.config["scraping"]
        r = requests.get(scraping["web"])
        for matchobj in re.finditer(scraping["info"], r.text):
            link = matchobj.group("link")
            version = matchobj.group("version")
            self.links[version] = link

    def _validate_config(self):
        # Correct or error on missing attributes
        cls = URLCollector
        if "request" not in self.config.keys():
            self.config["request"] = {}
        assert isinstance(
            self.config["request"], dict
        ), "Request information ('request') must be a dictionary"

        # Now: Get from the same URL and label it with the current time up to the desired precision
        if "now" in self.config.keys():
            now = self.config["now"]
            self.get_URL = self.get_now_URL
            assert (
                "URL" in now.keys()
            ), "URLCollector with now option must specify 'URL' in its subdictionary"
            try:
                urllib.parse.urlparse(now["URL"])
            except AttributeError as err:
                raise "Invalid URL in 'URL' parameter for 'now' option" from err
            assert (
                "date" in now.keys()
            ), "URLCollector with now option must specify 'date' in its subdictionary"
            _ = datetime.today().strftime(now["date"])

        # scraping: Get from links scraped from a web page
        elif "scraping" in self.config.keys():
            scraping = self.config["scraping"]
            self.get_URL = self.get_scraping_URL
            assert (
                "web" in scraping.keys()
            ), "URLCollector with now option must specify 'web' in its subdictionary"
            try:
                urllib.parse.urlparse(scraping["web"])
            except AttributeError as err:
                raise "Invalid URL in 'web' parameter for 'scraping' option" from err
            if "info" not in scraping.keys():
                scraping["info"] = scraping.get("info", cls.INFO_REGEX)
            assert "(?P<link>" in scraping["info"]
            assert "(?P<version>" in scraping["info"]
        else:
            assert False, "No method specified to obtain the datasets URL"
