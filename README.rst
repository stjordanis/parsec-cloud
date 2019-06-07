Showcase Pytest-trio issue #75

see
- https://github.com/python-trio/pytest-trio/pull/75
- https://github.com/python-trio/pytest-trio/pull/77

.. code-block:: shell

    $ pip install -e .[all]
    [...]
    $ py.test tests/test_showcase_pytest_trio_issue_75.py -s
    [...]
    tests/test_showcase_pytest_trio_issue_75.py Test started...
    Crash in messages_monitor :(
    .
    [...]
    ======= 1 passed in 0.25 seconds =======
    $ py.test tests/test_showcase_pytest_trio_issue_75.py -s --detect-pytest-trio-issue-75
    tests/test_showcase_pytest_trio_issue_75.py Test started...
    Crash in messages_monitor :(
    F
    [...]
    ======= 1 failed in 0.33 seconds =======
