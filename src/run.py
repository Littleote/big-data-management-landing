import argparse
import os
import sys
from pathlib import Path
from typing import Callable

from hdfs import Client, InsecureClient
from landing.collector import DataCollector as Collector
from landing.loader import mongoimport

from service import Service


def landing(
    collector: Collector, client: Client, host: str, source: Path, version: str
):
    file: str | None = None
    try:
        file = collector.retrive(version, client)
        records = mongoimport(
            client,
            file,
            db_name="bdm",
            coll_name=f"{source.stem}/{version}",
            db_url=host,
        )
        print(
            f"Loaded {records} records from source/version: '{source.stem}/{version}'"
        )
    except Exception as err:
        print(f"Failed to load from source/version: '{source.stem}/{version}'")
        print("Failed due to the following error:")
        print(err)
    if file is not None:
        client.delete(file)


def retrive(args: argparse.Namespace):
    metadata = Path("landing/metadata")
    sources = metadata.glob("**/*.json")
    if args.source is None:
        # Ask user for the metadata source
        print("Select a source:")
        sources = [source.relative_to(metadata) for source in sources]
        source = sources[select_from(sources)]
        source = Path(metadata, source)
    else:
        source = args.source
        if source == "*":
            # Run for all source files
            for source in sources:
                args.source = source.relative_to(metadata)
                retrive(args)
            return
        else:
            # Validate the source specified in the arguments
            if Path(source).suffix.lower() != ".json":
                source += ".json"
            source = Path(metadata, source)
            if not source.is_file():
                valid = [str(source.relative_to(metadata)) for source in sources]
                raise ValueError(
                    f"Invalid source file ({source.relative_to(metadata)}). Files are: {', '.join(valid)}"
                )

    # Instantiate landing elements
    collector = Collector.instance(source)
    client = InsecureClient(f"http://{args.host}:9870", user="bdm")
    versions = collector.versions()

    if args.all:
        # Retrive all available versions
        for version in versions:
            landing(collector, client, args.host, source, version)
    else:
        # Retrive user selected version
        latest = max(versions)
        if args.latest:
            use_latest = True
        else:
            user = input(f"Use latest version? ({latest}) [Y]/N ")
            use_latest = user[:1].lower() != "n"
        if use_latest:
            version = latest
        else:
            print("Select an available version of the source:")
            version = versions[select_from(versions)]
        landing(collector, client, args.host, source, version)


def select_from(
    options: list,
    /,
    *,
    on_bad_input: Callable | str | None = "raise",
) -> int | None:
    """
    Enumerate all options and return the index specified by the user

    If it failes to parse the user input, use `on_bad_input` to determine what to do
    """
    for i, option in enumerate(options):
        print(f"{i + 1}.- {str(option)}")
    try:
        index = int(input())
        assert 0 < index <= len(options)
        return index - 1
    except (ValueError, AssertionError) as err:
        if callable(on_bad_input):
            return on_bad_input()
        elif isinstance(on_bad_input, str):
            if on_bad_input.lower() == "raise":
                raise err
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        type=str,
        required=True,
        help="The IP of the server to connect to",
    )
    parser.add_argument(
        "--start_service",
        type=str,
        metavar=["USERNAME", "PASSWORD"],
        nargs=2,
        help="Credentials to start up the system",
    )

    subparsers = parser.add_subparsers(dest="cmd")
    retrive_cmd = subparsers.add_parser("retrive")
    retrive_cmd.add_argument(
        "--source",
        help="Specify the dataset source to retrive from the ones present in the metadata. \
            Use '*' to retrive from all sources",
    )
    which_version = retrive_cmd.add_mutually_exclusive_group()
    which_version.add_argument(
        "--all",
        action="store_true",
        help="Load all available versions of the source",
    )
    which_version.add_argument(
        "--latest",
        action="store_true",
        help="Load latest versions of the source",
    )
    retrive_cmd.set_defaults(func=retrive)

    args = parser.parse_args(sys.argv[1:])
    if args.cmd is None:
        parser.print_help()
    else:
        os.chdir(Path(__file__).absolute().parent)
        with Service(args.host, *args.start_service):
            args.func(args)


if __name__ == "__main__":
    main()
