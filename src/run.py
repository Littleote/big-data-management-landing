import argparse
import os
import sys
import tempfile
from pathlib import Path
from typing import Callable

from landing.collector import DataCollector as Collector
from landing.loader_test import load


def retrive(args: argparse.Namespace):
    metadata = Path("landing/metadata")
    sources = metadata.glob("**/*.json")
    if args.source is None:
        print("Select a source:")
        sources = [source.relative_to(metadata) for source in sources]
        source = sources[select_from(sources)]
        source = Path(metadata, source)
    else:
        source = args.source
        if Path(source).suffix.lower() != ".json":
            source += ".json"
        source = Path(metadata, source)
        if not source.is_file():
            valid = [str(source.relative_to(metadata)) for source in sources]
            raise ValueError(
                f"Invalid source file ({source.relative_to(metadata)}). Files are: {', '.join(valid)}"
            )
    collector = Collector.instance(source)
    versions = collector.versions()
    latest = max(versions)
    use_latest = input(f"Use latest version? ({latest}) [Y]/N ")
    if use_latest[:1].lower() == "n":
        print("Select an available version of the source:")
        version = versions[select_from(versions)]
    else:
        version = latest

    with tempfile.TemporaryDirectory() as directory:
        file = collector.retrive(version, directory)

        load(source.stem, version, file)
        # TODO: ADD Data loader step


def select_from(
    options: list,
    /,
    *,
    on_bad_input: Callable | str | None = "raise",
) -> int | None:
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
    subparsers = parser.add_subparsers(dest="cmd")
    retrive_cmd = subparsers.add_parser("retrive")
    retrive_cmd.add_argument(
        "--source",
        help="Specify the dataset source to retrive from the ones present in the metadata",
    )
    retrive_cmd.set_defaults(func=retrive)

    args = parser.parse_args(sys.argv[1:])
    if args.cmd is None:
        parser.print_help()
    else:
        os.chdir(Path(__file__).absolute().parent)
        args.func(args)


if __name__ == "__main__":
    main()
