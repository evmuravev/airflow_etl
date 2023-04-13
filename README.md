The case is as follows:
1. Each dag is a separate data flow for processing and loading data
2. Each flow consists of a set of jobs
3. Job is a set of atomic tasks presented as a TaskGroup in terms of Airflow
4. Each flow consists of two parts: a program part (the code of the dag itself) and a descriptive part (config.yml), which contains the parameters of the dag and registers its jobs
4. Each job also consists of two parts: a program part (a set of tasks) and a descriptive part (config.yml), which contains job parameters and specifies dependencies with other jobs.

The advantages of this approach are the ease of creating jobs as an elementary unit of dag from atomic tasks, registering new jobs in the dag configuration file without having to change the code. A potential improvement of the approach is the generation of tasks within the jobs themselves according to the descriptive part (config.yml) (i.e. moving towards self-service)

##### Example of the dag:
![](./dag.png)

1. Example of the dag's descriptive part:
```yml
dag_id: flow_2
###other_params
### ...
jobs:
  - job_1
  - job_2
  - job_2_1
  - job_3
  - job_4
```
2. Example of the job's descriptive part:
```yml
job_id: job_4
###other_params
### ...
depends_on:
  - job_2_1
  - job_3
```