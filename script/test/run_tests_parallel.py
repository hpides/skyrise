import os
import sys
import time
import os.path
import argparse
import threading
import subprocess
import multiprocessing


class Task:

    def __init__(self, test_binary: str, test_name: str, test_command: str, output_dir: str):
        self.binary = test_binary
        self.name = test_name.replace("/", "-")
        self.command = test_command

        self.profile_file = Task._profile_path(output_dir, self.name)
        self.log_file = Task._log_path(output_dir, self.name)

        self.exit_code = None
        self.execution = None
        self.duration = 0

        self.execution_count = 0

    @staticmethod
    def _profile_path(output_dir, test_name):
        return os.path.join(output_dir, f"skyriseTest_{test_name}.profraw")

    @staticmethod
    def _log_path(output_dir, test_name):
        return os.path.join(output_dir, f"skyriseTest_{test_name}.log")

    def kill_execution(self):
        if self.execution is not None:
            self.execution.terminate()

    def run(self) -> bool:
        self.execution_count += 1

        with open(self.profile_file, "w+") as prof:
            with open(self.log_file, "w+") as log:
                environment = {**os.environ.copy(), **{'LLVM_PROFILE_FILE': prof.name}}
                self.execution = subprocess.Popen(self.command, stdout=log, stderr=log, env=environment)
                self.execution.wait()
                self.exit_code = self.execution.returncode

        return self.exit_code == 0


class TaskManager:

    def __init__(self, tasks, retry_count):
        self.open = tasks
        self.running, self.passed = set(), []

        self.retry_count = retry_count
        self.failed_finally = False

        self.lock = threading.Lock()

    def next_task(self):
        with self.lock:
            if self.__done() or self.failed_finally:
                return None

            task = self.open.pop()
            self.running.add(task)

        return task

    def register_passed_task(self, task, duration_ms):
        with self.lock:
            self.running.remove(task)
            self.passed.append(task)

            sys.stdout.write(f"[{len(self.passed)}/{self.__total_number_of_tests()}] "
                             f"Finished execution of {task.name} within {duration_ms}ms.\n")
            sys.stdout.flush()

    def register_failed_task(self, task):
        with self.lock:
            if task.execution_count < self.retry_count:
                self.open.append(task)
            else:
                self.failed_finally = True

                for running_task in self.running:
                    running_task.kill_execution()

                sys.stderr.write(f"Failure during execution of test {task.name}:\n")
                with open(task.log_file, "r") as log:
                    sys.stderr.writelines(log.readlines())

                sys.stdout.flush()

    def __done(self) -> bool:
        return len(self.open) == 0

    def __total_number_of_tests(self):
        return len(self.passed) + len(self.open) + len(self.running)


class Worker(threading.Thread):

    def __init__(self, task_manager, worker_id):
        threading.Thread.__init__(self)
        self.task_manager = task_manager
        self.worker_id = worker_id
        self.running = True

    def run(self) -> None:
        while True:
            task = self.task_manager.next_task()
            if task is None:
                break
            begin = time.time()
            test_result = task.run()
            duration_ms = int(1000 * (time.time() - begin))

            if test_result:
                self.task_manager.register_passed_task(task, duration_ms)
            else:
                self.task_manager.register_failed_task(task)

        self.running = False


def execute_tests(tasks, worker_count, retry_count) -> (str, str):
    taskmanager = TaskManager(tasks, retry_count)

    pool = [Worker(taskmanager, i) for i in range(worker_count)]

    test_count = len(tasks)
    test_suite_count = len(set(map(lambda test: test.name.split(".")[0], tasks)))

    sys.stdout.write(f"Running {test_count} tests from {test_suite_count} test suites with {len(pool)} workers.\n")
    sys.stdout.flush()

    for worker in pool:
        worker.start()

    for worker in pool:
        worker.join()

    if taskmanager.failed_finally:
        sys.exit("Test execution failed.")

    sys.stdout.write(f"Finished Test-Execution and cleared Worker-Pool.\n")
    sys.stdout.flush()

    completed_tests = taskmanager.passed

    return list(map(lambda t: t.log_file, completed_tests)), list(map(lambda t: t.profile_file, completed_tests))


def merge_logs(log_files, global_log):
    sys.stdout.write(f"Merging Log-Files…\n")
    sys.stdout.flush()

    with open(global_log, "w+") as log_acc:
        for log_file in log_files:
            with open(log_file, "r") as log:
                log_acc.writelines(log.readlines())
                log_acc.write("\n")


def merge_profs(prof_files, global_profile):
    sys.stdout.write(f"Merging Profile-Files…\n")
    sys.stdout.flush()

    command = ["llvm-profdata", "merge", "-sparse"]

    for prof_file in prof_files:
        command.append(prof_file)

    command.append("-o")
    command.append(global_profile)

    subprocess.check_output(command)


def find_tests(test_binary, gtest_filter, output_dir):
    tasks = []

    command = [test_binary]
    list_command = command + ['--gtest_list_tests']
    if gtest_filter != '':
        list_command += ['--gtest_filter=' + gtest_filter]

    try:
        test_list = subprocess.check_output(list_command, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f"{test_binary}: {str(e)}\n{e.output}")

    try:
        test_list = test_list.split('\n')
    except TypeError:
        # subprocess.check_output() returns bytes in python3
        test_list = test_list.decode(sys.stdout.encoding).split('\n')

    test_group = ''
    for line in test_list:
        if not line.strip():
            continue
        if line[0] != " ":
            # Remove comments for typed tests and strip whitespace.
            test_group = line.split('#')[0].strip()
            continue
        # Remove comments for parameterized tests and strip whitespace.
        line = line.split('#')[0].strip()
        if not line:
            continue

        test_name = test_group + line

        # Skip disabled tests
        if '.DISABLED_' in test_name:
            continue

        # Skip PRE_ tests which are used by Chromium.
        if '.PRE_' in test_name:
            continue

        test_command = command + [f'--gtest_filter={test_name}']
        tasks.append(Task(test_binary, test_name, test_command, output_dir))

    return tasks


def main(binary, output_directory, gtest_filter_mask, worker_count, retry_count):
    tests = find_tests(test_binary=binary, output_dir=output_directory, gtest_filter=gtest_filter_mask)

    logs, profs = execute_tests(tasks=tests, worker_count=worker_count, retry_count=retry_count)

    global_log = os.path.join(output_directory, "skyriseTest.log")
    global_profile = os.path.join(output_directory, "skyriseTest.profdata")

    merge_logs(logs, global_log)
    merge_profs(profs, global_profile)


if __name__ == "__main__":
    default_worker_count = multiprocessing.cpu_count() * 3

    parser = argparse.ArgumentParser()

    parser.add_argument('--binary', dest='binary', type=str, required=True)
    parser.add_argument('--output_dir', dest='output_directory', type=str, required=True)
    parser.add_argument('--retries', dest='retry_count', type=int, default=1, required=False)
    parser.add_argument('--gtest_filter', dest='gtest_filter_mask', type=str, default="*", required=False)
    parser.add_argument('--workers', dest='worker_count', type=int, default=default_worker_count, required=False)

    args = parser.parse_args()

    main(args.binary, args.output_directory, args.gtest_filter_mask, args.worker_count, args.retry_count)

    sys.exit(0)
