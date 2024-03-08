import shutil
import json
import re
import os


class DataCollector:
    def __init__(self, config: dict | str | None = None):
        super().__init__()
        self.config = None
        if config is None:
            self.config = {}
        elif isinstance(config, dict):
            self.config = config
        elif isinstance(config, str):
            self.load_config_file(config)
        else:
            raise ValueError(
                f"Configuration must be a dictionary, file path or empty not {type(config).__name__}"
            )
        self.validate_config()

    def load_config_file(self, config_file: str):
        with open(config_file, mode="r") as handler:
            self.config = json.load(handler)

    def retrive(self, version: str, dest: str):
        raise NotImplementedError()

    def versions(self) -> list[str]:
        raise NotImplementedError()

    def validate_config(self):
        raise NotImplementedError()


class FileCollector(DataCollector):
    VERSION_REGEX = r"^.*?(\d(?:.*\d)?|\D*)\D*\.[^\.]+$"
    # ^(\d{4}_\d{2}_\d{2})_idealista\.(?:json|JSON)$
    # ^(.*)_extended\.(?:csv|CSV)$
    # ^(\d{4})_Distribucio_territorial_renda_familiar\.(?:csv|CSV)$
    FILE_REGEX = r"^(.+)\.[^\.]+$"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrive(self, version: str, dest: str):
        files = os.listdir(self.config["folder"])
        matches = [
            file
            for file in files
            if re.match(self.config["file"], file) is not None
            and re.match(self.config["version"], file).group(1) == version
        ]
        assert (
            len(matches) > 0
        ), f"Expected exactly one match for version '{version}' but no match was found"
        assert (
            len(matches) <= 1
        ), f"Expected exactly one match for version '{version}' but matched with {', '.join(matches)}"
        match = matches[0]
        shutil.copyfile(
            os.path.join(self.config["folder"], match),
            os.path.join(dest, match),
        )

    def versions(self) -> list[str]:
        files = os.listdir(self.config["folder"])
        versions = [
            re.match(self.config["version"], file).group(1)
            for file in files
            if re.match(self.config["file"], file) is not None
        ]
        return versions

    def validate_config(self):
        assert isinstance(
            self.config, dict
        ), f"Configuration must be a dictionary, not a {type(self.config).__name__}"

        # Correct or error on missing attributes
        cls = FileCollector
        keys = [repr(key) for key in self.config.keys()]
        assert (
            "folder" in self.config.keys()
        ), f"Configuration must specify 'folder' but found this: {', '.join(keys)}"
        if "file" not in self.config.keys():
            self.config["file"] = self.config.get("version", cls.FILE_REGEX)
        if "version" not in self.config.keys():
            self.config["version"] = cls.VERSION_REGEX
