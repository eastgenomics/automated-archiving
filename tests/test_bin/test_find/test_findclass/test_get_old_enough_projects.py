from bin.find import FindClass
from bin.environment import EnvironmentVariableClass
from bin.util import get_members

import pytest
from unittest import mock


def test_get_old_enough_projects():
    """
    Tests different cases with FindClass._get_old_enough_projects
    Cases represented by mock dataset:
    - 
    """
    # set up the basic pre-reqs for the FindClass
    env = EnvironmentVariableClass()
    find = FindClass(env, get_members("members.ini"))
    # TODO: continue

