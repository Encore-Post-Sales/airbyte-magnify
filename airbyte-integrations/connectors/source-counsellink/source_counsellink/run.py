import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceCounsellink

def run():
    source = SourceCounsellink()
    launch(source, sys.argv[1:])
