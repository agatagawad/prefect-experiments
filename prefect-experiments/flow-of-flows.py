
# Example from https://docs.prefect.io/core/idioms/flow-to-flow.html

from prefect import Flow, task
# from prefect.core import task
from prefect.core.parameter import Parameter
from prefect.tasks.prefect import StartFlowRun


@task
def A_task1(val):
    return 10*val

@task
def A_task2(val):
    return val + 5

with Flow(name='A') as flow_A:
    A_param = Parameter('A_param', 2)
    x = A_task1(A_param)
    y = A_task2(x)
flow_A.register(project_name='examples')


@task
def B_task1(val):
    return 20*val

@task
def B_task2(val):
    return val + 15

with Flow(name='B') as flow_B:
    B_param = Parameter('B_param', 1)
    x = B_task1(B_param)
    y = B_task2(x)
flow_B.register(project_name='examples')



@task
def C_task1(val):
    return 20*val

@task
def C_task2(val):
    return val + 15

with Flow(name='C') as flow_C:
    C_param = Parameter('C_param', 1)
    x = C_task1(C_param)
    y = C_task2(x)
flow_C.register(project_name='examples')

@task
def D_task1(val):
    return 20*val

@task
def D_task2(val):
    return val + 15

@task
def D_task3(x, y, val):
    return x + y + val

with Flow(name='D') as flow_D:
    C_param = Parameter('D_param', 1)
    x = D_task1(D_param)
    y = D_task2(x)
    z = D_task3(x, y, C_param)
flow_D.register(project_name='examples')



# assumes you have registered the following flows in a project named "examples"
flow_a = StartFlowRun(flow_name="A", project_name="examples", wait=True)
flow_b = StartFlowRun(flow_name="B", project_name="examples", wait=True)
flow_c = StartFlowRun(flow_name="C", project_name="examples", wait=True)
flow_d = StartFlowRun(flow_name="D", project_name="examples", wait=True)

with Flow("parent-flow") as flow:
    b = flow_b(upstream_tasks=[flow_a])
    c = flow_c(upstream_tasks=[flow_a])
    d = flow_d(upstream_tasks=[b, c])

flow.register(project_name='examples')