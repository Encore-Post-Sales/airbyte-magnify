#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_my_hours import SourceMyHours


def run():
    source = SourceMyHours()
    launch(source, sys.argv[1:])
