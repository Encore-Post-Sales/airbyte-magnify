#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_jira_data_center import SourceJiraDataCenter


def run():
    source = SourceJiraDataCenter()
    launch(source, sys.argv[1:])
