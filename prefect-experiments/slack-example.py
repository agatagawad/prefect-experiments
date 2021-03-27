# From Slack
# https://prefect-community.slack.com/archives/CL09KU1K7/p1607733221071000?thread_ts=1607727047.066800&cid=CL09KU1K7

from prefect import Flow, task, Parameter
from prefect.tasks.prefect import StartFlowRun
from prefect.utilities import tasks


@task
def task_a():
    return "hello"


@task
def task_b(val):
    return val


with Flow("flow_b") as flow_b:
    input = Parameter("result_a")
    result_b = task_b(input)
flow_b.register(project_name="examples")

# flow_b.set_reference_tasks([result_b])

# tmp_result_b = flow_b.run(parameters={'result_a': 200})
pass


@task
def task_c(x, y):
    return x * y


with Flow("flow_c") as flow_c:
    input = Parameter("result_b")
    other_param = Parameter("other_param", default=2)
    result_c = task_c(input, other_param)
flow_c.register(project_name="examples")

# tmp_result_c = flow_c.run(parameters={'result_b': 5})
pass


task_b = StartFlowRun(flow_name="flow_b", project_name="examples", wait=True)
task_c = StartFlowRun(flow_name="flow_c", project_name="examples", wait=True)


# The following two call rise exception Success:
# prefect.engine.signals.SUCCESS: c3bd9327-ab86-4209-b7c6-831d218ce47c finished in state <Success: "All reference tasks succeeded.">
#
# task_b.run(parameters={'result_a': 100})
# task_c.run(parameters={'result_b': 2})
pass

with Flow("flow_a") as flow_a:
    result_a = task_a()
    result_b = task_b(upstream_tasks=[result_a], parameters={"result_a": result_a})
    result_c = task_c(
        upstream_tasks=[result_b], parameters={"result_b": result_b.result}
    )
    # We access the .result of result_b because StartFlowRun returns a State object,
    # which includes the state message, result, context, and any cached inputs

flow_a.register("examples")
