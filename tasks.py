# Third Party
from invoke import task
from invoke_common_tasks import *  # noqa


@task
def test_spark(c):
    """Test on tests marked as spark."""
    c.run("python3 -m pytest -m 'spark'")
