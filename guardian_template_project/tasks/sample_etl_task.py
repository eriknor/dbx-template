from guardian_template_project.common import Task
import pkg_resources


class SQLTask(Task):
    def _execute_sql(self):
        queries = self.conf["queries"]
        for query in queries:
            if "filename" in query:
                query_path = pkg_resources.resource_filename("guardian_template_project", f"resources/sql/{query.get('filename')}")
                with open(query_path) as file:
                    for line in file:
                        self.spark.sql(line.rstrip().format_map(query))
            elif "query" in query:
               self.spark.sql(query.get("query").format_map(query)) 
               
    def launch(self):
        self.logger.info("Launching SQL task")
        self._execute_sql()
        self.logger.info("SQL task finished!")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = SQLTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
