# Third Party
from invoke_common_tasks import *  # noqa
from invoke import task


@task
def test_spark(c):
    """Test on tests marked as spark."""
    c.run("python3 -m pytest -m 'spark'")
